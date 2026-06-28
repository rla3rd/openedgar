import os
import sys
import django
import pandas as pd
import pyarrow as pa
import numpy as np
import zstandard as zstd
import re
from tqdm import tqdm
from sentence_transformers import SentenceTransformer

# Setup Django
sys.path.append('/home/ralbright/projects/openedgar/sec_openedgar')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.local')
django.setup()

from openedgar.models import Filing
from django.conf import settings
import hyperstreamdb as hdb

# Constants
HDB_PATH = "/home/ralbright/projects/openedgar/hdb_data"
EMBED_MODEL = "BAAI/bge-large-en-v1.5"
BATCH_SIZE = 50
MAX_EMBED_TOKENS = 512  # Approximation

def split_markdown_sections(text):
    """
    Splits markdown by headers and returns a list of (section_name, content).
    Tracks hierarchy for breadcrumb section names.
    """
    lines = text.split('\n')
    sections = []
    current_path = []
    current_content = []
    
    header_re = re.compile(r'^(#+) (.*)$')
    
    for line in lines:
        match = header_re.match(line)
        if match:
            # Save previous section if it had content
            if current_content:
                section_name = " > ".join(current_path) if current_path else "prologue"
                sections.append((section_name, "\n".join(current_content).strip()))
                current_content = []
            
            level = len(match.group(1))
            title = match.group(2).strip()
            
            # Update current_path hierarchy
            if level <= len(current_path):
                current_path = current_path[:level-1]
            while len(current_path) < level - 1:
                current_path.append("unknown")
            
            current_path.append(title)
        else:
            current_content.append(line)
            
    # Final section
    if current_content:
        section_name = " > ".join(current_path) if current_path else "prologue"
        sections.append((section_name, "\n".join(current_content).strip()))
        
    return sections

def get_zst_content(path):
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        try:
            return dctx.decompress(f.read()).decode('utf-8')
        except Exception:
            return None

def main():
    import argparse
    import gc
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, help='Path to file with accession numbers')
    parser.add_argument('--limit', type=int, help='Limit number of filings')
    parser.add_argument('--batch-size', type=int, default=128, help='Size of embedding batches')
    parser.add_argument('--clear', action='store_true', help='Clear existing HDB directory before starting')
    parser.add_argument('--skip-sgml', action='store_true', help='Skip ingesting SGML headers')
    args = parser.parse_args()

    # Initialize HDB
    if args.clear and os.path.exists(HDB_PATH):
        print(f"Clearing existing HDB directory at {HDB_PATH}...")
        import shutil
        shutil.rmtree(HDB_PATH)
    
    os.makedirs(HDB_PATH, exist_ok=True)
    print(f"Opening HDB table at {HDB_PATH}...")
    table = hdb.Table(HDB_PATH)
    
    # Initialize Embedding Model
    print(f"Loading embedding model {EMBED_MODEL}...")
    model = SentenceTransformer(EMBED_MODEL)
    ctx = hdb.GPUContext.auto_detect()
    print(f"Using GPU Context: {ctx.backend}")

    # Query filings that have sidecars
    filings = Filing.objects.filter(markdown_path__isnull=False)
    
    if args.file:
        with open(args.file, 'r') as f:
            accs = [line.strip() for line in f if line.strip()]
        filings = filings.filter(accession_number__in=accs)
    
    filings = filings.order_by('-date_filed')
    
    if args.limit:
        filings = filings[:args.limit]
    count = filings.count()
    print(f"Found {count} filings to ingest.")

    # Idempotency check: Skip filings already in HDB
    existing_accs = set()
    try:
        print("Checking for existing accessions in HDB (streaming)...")
        # Use streaming API to avoid OOM for large tables
        reader = table.to_arrow_stream(columns=["accession_number"])
        for batch in reader:
            # Efficiently collect unique accessions from each batch
            acc_col = batch.column("accession_number")
            existing_accs.update(acc_col.to_pylist())
        
        if existing_accs:
            print(f"Found {len(existing_accs)} filings already in HDB. They will be skipped.")
        else:
            print("HDB table is empty.")
    except Exception as e:
        print(f"Could not retrieve existing accessions: {e}")

    pending_rows = []

    def flush_pending():
        nonlocal pending_rows
        if not pending_rows:
            return
        
        # Batch encode
        texts_to_embed = [r.pop('_text_for_embed') for r in pending_rows]
        
        try:
            embeddings = model.encode(texts_to_embed, convert_to_numpy=True, batch_size=32, show_progress_bar=False).tolist()
            
            for i, row in enumerate(pending_rows):
                row['embedding'] = embeddings[i]
            
            df = pd.DataFrame(pending_rows)
            table.write_pandas(df)
        except Exception as e:
            print(f"\n[ERROR] Failed to process batch: {e}")
        finally:
            pending_rows = []
            gc.collect()

    for filing in tqdm(filings, desc="Ingesting filings"):
        if filing.accession_number in existing_accs:
            continue

        global_chunk_idx = 0
        if not args.skip_sgml:
            sgml_path = filing.resolved_sgml_path
            sgml_text = get_zst_content(sgml_path)
            if sgml_text:
                pending_rows.append({
                    'accession_number': filing.accession_number,
                    'form_type': filing.form_type.form if filing.form_type else "unknown",
                    'source_file': 'sgml',
                    'section': 'sgml_header',
                    'chunk_index': global_chunk_idx,
                    'cik': filing.cik.cik,
                    'date_filed': filing.date_filed,
                    'text': sgml_text,
                    '_text_for_embed': sgml_text[:2000],
                    'embedding': None
                })
                global_chunk_idx += 1

        # 2. Process Form Content
        form_path = filing.resolved_markdown_path
        form_text = get_zst_content(form_path)
        if form_text:
            try:
                sections = split_markdown_sections(form_text)
                for section_name, content in sections:
                    if not content: continue
                    # Basic chunking if too long
                    chunks = [content[i:i+2000] for i in range(0, len(content), 2000)]
                    for idx, chunk in enumerate(chunks):
                        pending_rows.append({
                            'accession_number': filing.accession_number,
                            'form_type': filing.form_type.form if filing.form_type else "unknown",
                            'source_file': 'form',
                            'section': section_name,
                            'chunk_index': global_chunk_idx,
                            'cik': filing.cik.cik,
                            'date_filed': filing.date_filed,
                            'text': chunk,
                            '_text_for_embed': f"{section_name}: {chunk[:2000]}",
                            'embedding': None
                        })
                        global_chunk_idx += 1
                        
                        if len(pending_rows) >= args.batch_size:
                            flush_pending()
            except Exception as e:
                print(f"\n[SKIP] Error parsing sections for {filing.accession_number}: {e}")

        if len(pending_rows) >= args.batch_size:
            flush_pending()

    # Final Write
    flush_pending()

    print("Ingestion complete. Optimizing index...")
    try:
        table.add_index("embedding", "hnsw")
        table.commit()
    except Exception as e:
        print(f"Optimization failed: {e}")
    print("Done.")


if __name__ == "__main__":
    main()

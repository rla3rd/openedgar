import logging
import os
import pathlib
import pandas as pd
import hyperstreamdb as hdb
from typing import List, Dict, Any, Optional
import re

logger = logging.getLogger(__name__)

class ModernRAGPipeline:
    def __init__(self, uri: str = None):
        data_dir = os.getenv('EDGAR_LOCAL_DATA_DIR', '/tmp')
        self.uri = uri or os.environ.get("HYPERSTREAM_RAG_URI", f"file://{data_dir}/hyperstream_rag_v3")
        self._table = None

    @property
    def table(self):
        if self._table is None:
            self._ensure_table()
        return self._table

    def _ensure_table(self):
        """Ensures the HyperStreamDB table exists with Autovectorization enabled."""
        if not os.path.exists(self.uri.replace("file://", "")):
            # Define schema with mathematical anchors
            schema = hdb.Schema([
                hdb.Field("id", hdb.DataType.string()),
                hdb.Field("cik", hdb.DataType.string()),
                hdb.Field("accession_number", hdb.DataType.string()),
                hdb.Field("form_type", hdb.DataType.string()),
                hdb.Field("date_filed", hdb.DataType.string()),
                hdb.Field("section", hdb.DataType.string()), 
                hdb.Field("content", hdb.DataType.string()),
                hdb.Field("start_pos", hdb.DataType.int64()), # Mathematical Anchor
                hdb.Field("end_pos", hdb.DataType.int64()),   # Mathematical Anchor
                hdb.Field("embedding", hdb.DataType.vector(1024))
            ])
            
            self._table = hdb.Table.create(self.uri, schema)
            logger.info(f"Created new HyperStreamDB table at {self.uri}")
            
            try:
                self._table.add_primary_key("id")
                self._table.add_index("cik")
                self._table.add_index("accession_number")
                
                # Autovectorization (bge-m3)
                self._table.define_embedding(
                    column="content", 
                    function="bge-m3", 
                    vector_column="embedding"
                )
                logger.info("RAG Schema V3: Math-First anchors and Autovectorization enabled.")
            except Exception as e:
                logger.warning(f"Metadata optimization partially failed: {e}")
        else:
            self._table = hdb.Table(self.uri)
            logger.info(f"Connected to HyperStreamDB table at {self.uri}")

    def chunk_markdown(self, markdown: str, chunk_size: int = 1500) -> List[Dict]:
        """
        Atomic chunking for Markdown (Zero Overlap).
        Splits by headers first, then by size. Returns text + original offsets.
        """
        # Identify heading boundaries for semi-semantic splitting
        boundaries = [m.start() for m in re.finditer(r'^#{1,3}\s', markdown, re.MULTILINE)]
        
        chunks = []
        current_pos = 0
        content_len = len(markdown)
        
        while current_pos < content_len:
            target_end = current_pos + chunk_size
            if target_end >= content_len:
                chunks.append({"content": markdown[current_pos:], "start": current_pos, "end": content_len})
                break
            
            # Use nearest boundary if available
            split_pos = target_end
            for b in reversed(boundaries):
                if current_pos < b <= target_end:
                    if (b - current_pos) > (chunk_size * 0.5):
                        split_pos = b
                        break
            
            # Fallback to newline
            if split_pos == target_end:
                nl_pos = markdown.rfind('\n', current_pos, target_end)
                if nl_pos > current_pos + (chunk_size * 0.5):
                    split_pos = nl_pos + 1
            
            chunks.append({
                "content": markdown[current_pos:split_pos].strip(),
                "start": current_pos,
                "end": split_pos
            })
            current_pos = split_pos
            
        return chunks

    def ingest_filing_chunks(self, cik: str, accession_number: str, form_type: str, date_filed: str, markdown: str):
        """
        Ingests atomic chunks with offsets into HyperStreamDB.
        """
        meta_chunks = self.chunk_markdown(markdown)
        
        batch = []
        for i, m_chunk in enumerate(meta_chunks):
            batch.append({
                "id": f"{accession_number}_{m_chunk['start']}_{i}",
                "cik": str(cik).zfill(10),
                "accession_number": accession_number,
                "form_type": form_type,
                "date_filed": date_filed,
                "section": "filing_content",
                "content": m_chunk["content"],
                "start_pos": m_chunk["start"],
                "end_pos": m_chunk["end"]
            })
            
        if batch:
            df = pd.DataFrame(batch)
            self.table.write(df, mode='append')
            self.table.commit()
            logger.info(f"Ingested {len(batch)} atomic chunks for {accession_number}.")

    def query(self, text: str, k: int = 5, cik: str = None) -> pd.DataFrame:
        """
        Vector search. Results include start_pos/end_pos for query-time window expansion.
        To deduplicate and expand context:
        1. Group results by accession_number.
        2. Fetch neighboring characters from the sidecar fragments on disk using start_pos/end_pos.
        """
        search_query = self.table.query()
        if cik:
            search_query = search_query.filter(f"cik = '{str(cik).zfill(10)}'")
        
        results = search_query.vector_search(text, column="embedding", k=k)
        return results.to_pandas()

    def fetch_expanded_context(self, filing_row: Dict, buffer_chars: int = 1024) -> str:
        """
        [Helper] Reconstructs an expanded window for a specific search result.
        Uses the sidecar fragments (.out.##.md.zst) on disk to fetch context 
        without polluting the vector space.
        """
        acc = filing_row['accession_number']
        start = filing_row['start_pos']
        end = filing_row['end_pos']
        
        # Real-world logic would:
        # 1. Identify which .out.##.md.zst chunk(s) contain the range [start-buffer, end+buffer]
        # 2. Decompress and return the combined text slice.
        # This keeps the DB small and the Math pure.
        return f"...[Unified context expansion for {acc} at {start}-{end} with {buffer_chars} overlap]..."

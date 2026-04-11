import os
import sys
import pathlib
import hashlib
import logging
import django
from datetime import datetime

# 1. Environment Setup (Standalone Boilerplate)
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "sec_openedgar"))

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.local')
django.setup()

import zstandard as zstd
from openedgar.models import Filing, FilingDocument, Company, CompanyInfo
from openedgar.clients.local import LocalClient
from openedgar.parsers.openedgar import extract_text

# Setup specialized logger for migration
logger = logging.getLogger("migration")
logger.setLevel(logging.INFO)
h = logging.StreamHandler()
h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(h)

def compute_sha1(buffer):
    sha1 = hashlib.sha1()
    sha1.update(buffer)
    return sha1.hexdigest()

def ingest_file(file_path: pathlib.Path, client: LocalClient):
    """
    Core logic to ingest a single .zstd filing into the OpenEDGAR lake.
    """
    try:
        # 1. Parse metadata from path
        # Expected: data/{Year}/{QTR}/{Accession}.{Ext}.zstd
        parts = file_path.parts
        year = int(parts[-3])
        quarter = parts[-2]
        filename = file_path.name
        file_meta = filename.split('.')
        accession = file_meta[0]
        ext = file_meta[1] if len(file_meta) > 1 else "nc"

        # 2. Decompress
        dctx = zstd.ZstdDecompressor()
        with open(file_path, 'rb') as f:
            raw_content = dctx.decompress(f.read())
        
        # 3. Deduplicate via SHA1
        sha1 = compute_sha1(raw_content)
        
        # 4. Check/Create Company (Minimal Record)
        # Extract CIK from accession if possible (CIC is often first 10 digits)
        cik_val = int(accession.split('-')[0])
        company, _ = Company.objects.get_or_create(cik=cik_val, defaults={'cik_name': f"Migrated Co {cik_val}"})

        # 5. Check/Create Filing Record
        filing, created = Filing.objects.get_or_create(
            accession_number=accession,
            defaults={
                'cik': company,
                'date_filed': datetime(year, 1, 1).date(), # Placeholder if unknown, updated during parse
                'is_processed': False,
                'sha1': sha1
            }
        )

        # 6. Save to Data Lake (Accession-Indexed)
        # Format: documents/raw/{accession}.{ext}.zst
        raw_cas_path = f"documents/raw/{accession}.{ext}.zst"
        if not client.path_exists(raw_cas_path):
            client.put_buffer(raw_cas_path, raw_content)

        # 7. Fast Parsing (selectolax)
        text_content = extract_text(raw_content, content_type="text/html")
        
        text_cas_path = f"documents/text/{accession}.{ext}.zst"
        if not client.path_exists(text_cas_path):
            client.put_buffer(text_cas_path, text_content)

        # 8. Sync FilingDocument (The entity standard)
        FilingDocument.objects.get_or_create(
            filing=filing,
            type=ext.upper(),
            defaults={
                'file_name': f"{accession}.{ext}",
                'sha1': sha1,
                'content_type': 'text/html' if 'htm' in ext or 'nc' in ext else 'text/plain',
                'is_processed': True,
                'sequence': 1
            }
        )
        
        # Mark as processed
        filing.is_processed = True
        filing.save()
        return True

    except Exception as e:
        logger.error(f"Failed to ingest {file_path}: {e}")
        return False

def main(data_dir=None):
    if not data_dir:
        data_dir = os.getenv("EDGAR_LOCAL_DATA_DIR", "data")
    
    data_path = pathlib.Path(data_dir)
    if not data_path.exists():
        logger.error(f"Data directory not found: {data_path}")
        return

    client = LocalClient()
    logger.info(f"Starting mass migration from {data_path}...")

    total = 0
    success = 0
    
    # Recurse through Year/QTR
    for zstd_file in data_path.rglob("*.zstd"):
        total += 1
        if ingest_file(zstd_file, client):
            success += 1
        
        if total % 100 == 0:
            logger.info(f"Progress: {success}/{total} processed...")

    logger.info(f"Migration Complete! Successfully ingested {success} of {total} filings.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()

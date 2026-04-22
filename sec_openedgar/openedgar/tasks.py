"""
MIT License
Copyright (c) 2024 Richard Albright
Copyright (c) 2018 ContraxSuite, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Libraries
import sys
import traceback
import datetime
import hashlib
import logging
import os
import pathlib
from openedgar.parsers.ownership_parser import OwnershipParser
import requests
import sec2md
import spacy
import asyncio
import aiohttp
import zstandard as zstd
from tarfile import ReadError
import io
import orjson as json
import pandas as pd
from bs4 import BeautifulSoup
from typing import Iterable, Union, Optional, Dict, List

# Packages
import dateutil.parser
import django.db.utils
from celery import shared_task

# Project
from config.settings.base import S3_DOCUMENT_PATH
from openedgar.clients.s3 import S3Client
from openedgar.clients.local import LocalClient
import openedgar.clients.openedgar
import openedgar.parsers.openedgar
try:
    from openedgar.models import Company, CompanyFact, CompanyInfo, FactIndex, FilingIndex, Filing, FilingDocument, FormIndex, SearchQuery, SearchQueryTerm, SearchQueryResult
except Exception:
    # This may fail in spawned worker processes before django.setup() is called.
    # We handle this by importing models locally within the worker functions.
    pass
from openedgar.sec_api import sec_api
import hyperstreamdb as hs
from openedgar.processes.rag_pipeline import ModernRAGPipeline

# Initialize RAG Pipeline lazily
rag_pipeline = None

def get_rag_pipeline():
    global rag_pipeline
    if rag_pipeline is None:
        rag_pipeline = ModernRAGPipeline()
    return rag_pipeline

# import tabula for formtypes
import tabula

class AsyncDownloader:
    def __init__(self, rate_limit=5):
        self.semaphore = asyncio.Semaphore(rate_limit)
        self.extractions = []
        self._headers = None

    @property
    def headers(self):
        if self._headers is None:
            from django.conf import settings
            identity = getattr(settings, "EDGAR_IDENTITY", os.getenv("EDGAR_IDENTITY", "User user@example.com"))
            self._headers = {"User-Agent": identity}
        return self._headers

    async def stream_to_disk(self, session, url, output_path, max_retries=5, verbose=False):
        async with self.semaphore:
            for attempt in range(max_retries):
                try:
                    import aiofiles
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    async with session.get(url, headers=self.headers, timeout=600) as response:
                        if response.status == 200:
                            total_size = int(response.headers.get('content-length', 0))
                            from tqdm import tqdm
                            file_pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f"  Downloading {output_path.name}", leave=False, disable=not verbose)
                            
                            import zlib
                            import pyzstd
                            decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16) # Handle Gzip header
                            compressor = pyzstd.ZstdCompressor(3)
                            
                            async with aiofiles.open(output_path, 'wb') as f:
                                async for chunk in response.content.iter_chunked(1024 * 1024):
                                    # Decompress incoming Gzip
                                    raw_data = decompressor.decompress(chunk)
                                    if raw_data:
                                        # Compress to Zstd
                                        zstd_data = compressor.compress(raw_data)
                                        await f.write(zstd_data)
                                    file_pbar.update(len(chunk))
                                
                                # Flush remainders
                                last_raw = decompressor.flush()
                                if last_raw:
                                    await f.write(compressor.compress(last_raw))
                                await f.write(compressor.flush())
                                
                            file_pbar.close()
                            return True
                        elif response.status == 429:
                            retry_after = int(response.headers.get("Retry-After", 60))
                            logger.warning(f"Rate limited on {url}. Waiting {retry_after}s (Attempt {attempt+1}/{max_retries})")
                            await asyncio.sleep(retry_after)
                        else:
                            logger.error(f"Failed to fetch {url}: Status {response.status}")
                            if response.status >= 500: # Retry on server errors
                                await asyncio.sleep(2 ** attempt)
                                continue
                            return False
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"Network error fetching {url} (Attempt {attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Unexpected error fetching {url} (Attempt {attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(2 ** attempt)
            
            logger.error(f"Exhausted retries for {url}")
            return False

    async def fetch_text(self, session, url, max_retries=5):
        """Used for small HTML/Index pages, returns text with automatic 429 handling."""
        for attempt in range(max_retries):
            async with self.semaphore:
                try:
                    async with session.get(url, headers=self.headers) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 429:
                            retry_after = int(response.headers.get("Retry-After", 10))
                            print(f"Rate limited on {url}. Waiting {retry_after}s... (Attempt {attempt+1}/{max_retries})")
                            await asyncio.sleep(retry_after)
                            continue
                        else:
                            print(f"Error fetching {url}: HTTP {response.status}")
                            if response.status >= 500:
                                await asyncio.sleep(2 ** attempt)
                                continue
                            return None
                except Exception as e:
                    print(f"Network error on {url} (Attempt {attempt+1}): {str(e)}")
                    await asyncio.sleep(2 ** attempt)
        return None

    async def download_and_compress(self, session, url, output_path, verbose=False, replace=False, download_only=False, forms=None, pbar=None):
        """Streams the tar.gz exactly as it is to disk and queues background extraction."""
        import tempfile
        import shutil
        # 1. Stream from SEC (gzip) and convert to .tar.zst on-the-fly
        with tempfile.NamedTemporaryFile(delete=False, suffix=".tar.zst") as tmp:
            tmp_path = pathlib.Path(tmp.name)
            
        success = await self.stream_to_disk(session, url, tmp_path, verbose=verbose)
        if not success:
            if tmp_path.exists(): tmp_path.unlink()
            return False
            
        # 2. Extract into final zstd chunks in the background
        try:
            if tmp_path.exists():
                final_dest = output_path
                if download_only:
                    # Move directly to RAW as a cache for later processing
                    raw_dir = output_path.parent / "RAW"
                    raw_dir.mkdir(parents=True, exist_ok=True)
                    final_dest = raw_dir / output_path.name
                    
                final_dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(tmp_path), str(final_dest))
                
            if download_only:
                if verbose: print(f"  Download complete for {output_path.name} (Staged in RAW mode).")
                if pbar: pbar.update(1)
                return True
                
            # Trigger background extraction and track the future
            if verbose: print(f"  Download complete. Queuing extraction for {output_path.name}...")
            loop = asyncio.get_event_loop()
            task = loop.run_in_executor(None, extract_and_compress_tar_feed, str(output_path), True, verbose, replace, forms)
            
            # Store metadata for potential retries
            self.extractions.append({
                "url": url,
                "path": output_path,
                "future": task
            })
            return True
        except Exception as e:
            logger.error(f"Error handling SEC feed {url}: {e}")
            if tmp_path.exists(): tmp_path.unlink()
            return False

def compress_content_zstd(content: bytes) -> bytes:
    """Helper to compress content using zstandard."""
    cctx = zstd.ZstdCompressor(level=3)
    return cctx.compress(content)

# spaCy for modern NLP
nlp = None
try:
    nlp = spacy.load("en_core_web_sm")
except (IOError, ImportError):
    # Model will be loaded on demand or user will be prompted
    pass

def get_spacy_nlp():
    global nlp
    if nlp is None:
        try:
            nlp = spacy.load("en_core_web_sm")
        except:
            # Fallback for CI or minimal environments
            import spacy
            from spacy.lang.en import English
            nlp = English()
            nlp.add_pipe('sentencizer')
    return nlp

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

def format_metadata_to_markdown(d, depth=0):
    """
    Recursive formatter to transform nested metadata into a structured ALL-CAPS Markdown report.
    """
    md = ""
    indent = "  " * depth
    prefix = "### " if depth == 0 else "#### " if depth == 1 else ""
    for k, v in d.items():
        title = k.replace("-", " ").replace("_", " ").upper()
        if isinstance(v, dict):
            md += f"\n{prefix}{title}\n"
            md += format_metadata_to_markdown(v, depth + 1)
        elif isinstance(v, list):
            md += f"\n{prefix}{title}\n"
            for item in v:
                if isinstance(item, dict):
                    md += format_metadata_to_markdown(item, depth + 1)
                else:
                    md += f"{indent}{item}\n"
        elif v:
            md += f"{indent}**{title}**: {v}\n"
    return md

def _process_and_archive_filing(content_bytes: bytes, output_dir: pathlib.Path, fallback_acc: str, verbose: bool = False, replace: bool = True, forms: List[str] = None, extract_only: bool = False) -> bool:
    """
    Processes a single raw filing string (typically SGML).
    If extract_only=True, it simply shatters the SGML into its component documents and sidecars
    without performing high-fidelity synthesis or database updates.
    """

    """
    Unified engine to parse a filing, generate dual sidecars, 
    calculate random-access offsets, and sync the JSONB index to PostgreSQL.
    """
    try:
        from openedgar.parsers.openedgar import parse_filing
        # Quick existence check if replace is False
        # We use the main sidecar as the 'canary' for completion
        acc_num_canary = fallback_acc # Default until we parse
        parsed = parse_filing(content_bytes, extract=False)
        
        if not (parsed and "documents" in parsed):
            return False
            
        # Sanitize Accession Number
        acc_num = (parsed.get("accession_number") or fallback_acc).strip()
        acc_num = acc_num.split('\r')[0].split('\n')[0].replace(" ", "").replace(">", "")
        form_type = str(parsed.get("form_type", "DOC")).strip().upper()
        
        # --- Form Filtering ---
        if forms:
            # Normalize forms to uppercase for matching
            target_forms = [f.upper().strip() for f in forms]
            if form_type not in target_forms:
                if verbose: print(f"  Skipping non-matching form type: {form_type} for {acc_num}")
                return True # Return true because we "successfully" skipped it
        
        # --- Skip Logic ---
        md_path = output_dir / f"{acc_num}.sgml.md.zst"
        if not replace and md_path.exists():
            if verbose: print(f"  Skipping existing {acc_num}...")
            return True

        if verbose: print(f"  Archiving {acc_num}...")
        
        # Calculate Byte-Offsets for Random Access
        full_meta = parsed.get('full_metadata', {})
        try:
            from secsgml2.utils import calculate_documents_locations_in_tar
            calculate_documents_locations_in_tar(full_meta)
        except Exception as e:
            logger.warning(f"Failed to calculate offsets for {acc_num}: {e}")

        # Use the shared formatter
        metadata_md = f"# SEC Filing Metadata (Structured)\n"
        metadata_md += format_metadata_to_markdown(full_meta)
        metadata_md += f"\n## Documents\n"
        
        raw_sgml_header = parsed.get("raw_header", b"")
        processed_count = 0
        documents_index_data = {
            "header": full_meta,
            "documents": []
        }

        # Use pyzstd for all compression
        import pyzstd

        # Handle each document
        for doc in parsed["documents"]:
            seq = str(doc.get("sequence", "1")).strip()
            seq = seq.split('\r')[0].split('\n')[0].zfill(2)
            raw_type = str(doc.get("type", "doc") or "doc").lower()
            clean_type = raw_type.split('\r')[0].split('\n')[0].replace("/", "")
            doc_type_prefix = clean_type[:4].replace(" ", "").strip()
            
            chunk_name = f"{acc_num}.{doc_type_prefix}{seq}.zst"
            chunk_path = output_dir / chunk_name
            
            doc_file_name = doc.get('file_name', 'unknown')
            doc_sha1 = doc.get('sha1', 'unknown')
            
            # Semantic Header for Sidecar
            metadata_md += f"\n#### DOCUMENT {seq}: {raw_type.upper()}\n"
            metadata_md += f"**FILENAME**: `{doc_file_name}`\n"
            metadata_md += f"**RESOURCE**: `{chunk_name}`\n"
            metadata_md += f"**SHA1**: `{doc_sha1}`\n"
            
            # Payload compression
            payload = doc.get("content_text") or doc.get("content", b"")
            if isinstance(payload, str):
                payload = payload.encode('utf-8')
            
            with open(chunk_path, "wb") as f_out:
                f_out.write(pyzstd.compress(payload))
                
            documents_index_data["documents"].append({
                "type": raw_type,
                "sequence": seq,
                "filename": doc_file_name,
                "sha1": doc_sha1,
                "path": chunk_name
            })
            processed_count += 1

        # --- Stage 2: Synthesis & DB Integration ---
        if extract_only:
            return True

        # High-fidelity synthesis for Ownership Forms (Form 3, 4, 5)
        if form_type in ('3', '4', '5', '3/A', '4/A', '5/A'):
            if verbose: print(f"  Synthesizing high-fidelity Markdown for {acc_num}...")
            try:
                # Use the full raw content for synthesis to ensure sibling chunks/header discovery is self-contained if needed
                # (Though here we have the full string content from secsgml2)
                synthesizer = OwnershipParser()
                md_content = synthesizer.synthesize(content_bytes.decode('utf-8', errors='ignore'), accession=acc_num)
                
                if not md_content.startswith("Error"):
                    synth_path = output_dir / f"{acc_num}.out.md.zst"
                    
                    if synth_path.exists() and not replace:
                        if verbose: print(f"    Skipping existing synthesis for {acc_num}...")
                    else:
                        with open(synth_path, "wb") as f_out:
                            f_out.write(pyzstd.compress(md_content.encode('utf-8')))
            except Exception as e:
                logger.error(f"Failed high-fidelity synthesis for {acc_num}: {e}")

        # Technical manifest sidecar (Basic metadata)
        master_markdown = ""

            
        # Sidecar 2: Technical Manifest / Filing Header
        header_md_path = output_dir / f"{acc_num}.sgml.md.zst"
        with open(header_md_path, "wb") as f_out:
            f_out.write(pyzstd.compress(metadata_md.encode('utf-8')))
            
        # Sidecar 3: Raw SGML Header
        if raw_sgml_header:
            header_path = output_dir / f"{acc_num}.sgml.zst"
            raw_bytes = raw_sgml_header if isinstance(raw_sgml_header, bytes) else raw_sgml_header.encode('utf-8', errors='ignore')
            with open(header_path, "wb") as f_out:
                f_out.write(pyzstd.compress(raw_bytes))

        # Sync to PostgreSQL
        try:
            from openedgar.models import Filing
            Filing.objects.filter(accession_number=acc_num).update(
                documents_index=documents_index_data,
                is_processed=True,
                processed_document_count=processed_count
            )
        except Exception as e:
            logger.error(f"Failed to sync Golden Source index for {acc_num}: {e}")

        return True
    except Exception as e:
        logger.error(f"Error processing filing {fallback_acc}: {e}")
        return False

def extract_and_compress_tar_feed(tar_file_path: str, remove_after: bool = False, verbose: bool = False, replace: bool = True, forms: List[str] = None, destination_dir: pathlib.Path = None, extract_only: bool = False):
    """
    Extracts SEC Bulk Feed archives (.tar.gz or .tar.zst) and streams each record into Zstandard chunks.
    """
    import tarfile
    import os
    import gzip
    try:
        import pyzstd
    except ImportError:
        pyzstd = None

    tar_path = pathlib.Path(tar_file_path)
    if not tar_path.exists():
        return False
        
    output_dir = destination_dir or tar_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    if verbose: print(f"Extracting {tar_path.name} to {output_dir}...")
    
    try:
        processed_tally = 0
        
        # Determine the appropriate decompressor based on extension
        # We check both suffix and name for robustness
        if tar_path.suffix == ".zst" or ".tar.zst" in tar_path.name:
            if pyzstd is None:
                raise ImportError("pyzstd is required for .zst files but not installed.")
            zf = pyzstd.open(tar_path, "rb")
        elif tar_path.suffix == ".gz" or ".tar.gz" in tar_path.name:
            zf = gzip.open(tar_path, "rb")
        else:
            # Fallback to direct opening (e.g. for uncompressed .tar)
            zf = open(tar_path, "rb")

        from concurrent.futures import ThreadPoolExecutor
        # Use a reasonable number of threads to overlap I/O and CPU (pyzstd/gzip release GIL)
        # We cap it at 10 to avoid overwhelming the DB connection pool in a single process
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            with zf:
                with tarfile.open(fileobj=zf) as tar:
                    for member in tar.getmembers():
                        if member.isfile() and member.name.endswith(".nc"):
                            member_stream = tar.extractfile(member)
                            if member_stream is not None:
                                fallback_acc = os.path.basename(member.name).replace(".nc", "")
                                
                                # Optimization: Early skip if file exists and we are not replacing
                                if not replace:
                                    md_path = output_dir / f"{fallback_acc}.sgml.md.zst"
                                    if md_path.exists():
                                        processed_tally += 1
                                        continue

                                content = member_stream.read()
                                # Dispatch to thread pool
                                futures.append(executor.submit(
                                    _process_and_archive_filing, 
                                    content, output_dir, fallback_acc, 
                                    verbose=False, replace=replace, forms=forms, 
                                    extract_only=extract_only
                                ))
                                processed_tally += 1
                                if verbose and processed_tally % 100 == 0:
                                    print(f"  Progress: {processed_tally} filings queued from {tar_path.name}")
            
            # Wait for completion and check results
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in filing worker thread: {e}")
        
        if remove_after:
            # Move to RAW folder relative to the original location or as specified
            final_raw = tar_path.parent / "RAW"
            final_raw.mkdir(parents=True, exist_ok=True)
            if str(tar_path.parent) != str(final_raw):
                import shutil
                shutil.move(str(tar_path), str(final_raw / tar_path.name))
        return True
    except Exception as e:
        logger.error(f"Failed processing tarball {tar_file_path}: {e}")
        # If the file is corrupted or failed to extract, delete it so it can be re-downloaded
        if tar_path.exists():
            logger.warning(f"Corrupted archive detected; deleting {tar_path.name} for re-download retry.")
            tar_path.unlink()
        return False

def convert_legacy_zstd_worker(zstd_file_path: str, forms: List[str] = None) -> bool:
    """
    Modernizes a legacy .zstd filing by converting it to the dual-sidecar Format.
    Deletes the original .zstd once archived.
    """
    zstd_path = pathlib.Path(zstd_file_path)
    if not zstd_path.exists(): return False
        
    try:
        import pyzstd
        with open(zstd_path, "rb") as f_in:
            content_bytes = pyzstd.decompress(f_in.read())
            
        fallback_acc = zstd_path.name.split('.')[0]
        success = _process_and_archive_filing(content_bytes, zstd_path.parent, fallback_acc, forms=forms)
        
        if success:
            import shutil
            raw_dir = zstd_path.parent / "RAW"
            raw_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(zstd_path), str(raw_dir / zstd_path.name))
        return success
    except Exception as e:
        logger.error(f"Failed converting legacy file {zstd_file_path}: {e}")
        return False

def migrate_archives_to_zstd_worker(file_path: Union[str, pathlib.Path], verbose=False, replace_original=False, threads=0) -> bool:
    """
    Converts a .tar.gz archive to .tar.zst with integrity verification.
    Reused from migrate_archives_to_zstd.py script.
    Optimized for HDD by using multi-threaded zstd for single-file sequential I/O.
    """
    import zlib
    import pyzstd
    import tarfile
    import shutil
    import pathlib
    import gzip
    import os
    
    if isinstance(file_path, str):
        file_path = pathlib.Path(file_path)
        
    zst_path = file_path.parent / (file_path.name.replace('.tar.gz', '.tar.zst'))
    
    if zst_path.exists():
        if verbose: print(f"  Skipping {file_path.name} (Zstd version exists)")
        return True

    # Use a temp file in the same directory to ensure atomic move on same filesystem
    temp_zst = file_path.with_suffix('.zst.tmp')
    
    try:
        # 1. Pipeline: Decompress Gzip -> Compress Zstd
        # We use multi-threaded zstd to speed up the compression part of the pipeline
        # while keeping the I/O sequential.
        zstd_option = {pyzstd.CParameter.nbWorkers: threads}
        compressor = pyzstd.ZstdCompressor(zstd_option)
        
        with gzip.open(file_path, 'rb') as f_in, open(temp_zst, 'wb') as f_out:
            while True:
                # 16MB buffer to stay sequential and avoid disk thrashing on HDDs
                chunk = f_in.read(16 * 1024 * 1024)
                if not chunk: break
                f_out.write(compressor.compress(chunk))
            
            f_out.write(compressor.flush())

        # 2. Integrity Verification
        with pyzstd.open(temp_zst, "rb") as zf:
            with tarfile.open(fileobj=zf) as tar:
                # Checking the TOC catches most issues
                _ = tar.getmembers()

        # 3. Finalize
        shutil.move(str(temp_zst), str(zst_path))
        if verbose: print(f"  Successfully converted to {zst_path.name}")
        
        if replace_original:
            file_path.unlink()
            if verbose: print(f"  Deleted original {file_path.name}")
            
        return True

    except Exception as e:
        logger.error(f"ERROR converting {file_path.name}: {e}")
        if temp_zst.exists(): temp_zst.unlink()
        
        # If the file is corrupted (unexpected end of data, etc), delete it 
        # so it can be re-downloaded by the regular ingestion task.
        err_str = str(e).lower()
        if "unexpected end of data" in err_str or "not a gzipped file" in err_str:
            logger.warning(f"Corrupted archive detected; deleting {file_path.name} to allow for re-download.")
            if file_path.exists():
                file_path.unlink()
                
        return False

def get_text_quarter(month):
    if month in ("01", "02", "03"):
        return "QTR1"
    elif month in ("04", "05", "06"):
        return "QTR2"
    elif month in ("07", "08", "09"):
        return "QTR3"
    elif month in ("10", "11", "12"):
        return "QTR4"

async def download_bulk_filings_async(year=None, qtr=None, backfill=True, verbose=False, days=None, replace=False, forms: List[str] = None, download_only=False):
    base_url = 'https://www.sec.gov/Archives/edgar/Feed/'
    downloader = AsyncDownloader()
    
    from openedgar.sec_api import sec_api
    
    # --- Targeted Mode (Central Index) ---
    if forms:
        if verbose: print(f"Targeted Mode: Ingesting forms {forms} via Central Index...")
        
        # 1. Determine Years/Quarters
        if year:
             target_years = [year]
        elif days:
             target_years = sorted(list(set(int(str(d)[:4]) for d in days)))
        else:
             import datetime
             target_years = list(range(1997, datetime.datetime.now().year + 1))
             
        for yr in target_years:
            qtrs = [qtr] if qtr else [1, 2, 3, 4]
            for qt in qtrs:
                if verbose: print(f"  Fetching Index for {yr} QTR{qt}...")
                df = sec_api.get_filings_for_quarter(yr, qt)
                if df.empty: continue
                
                # Filter by form type (case-insensitive and normalized)
                target_forms = [f.upper().strip() for f in forms]
                df_filtered = df[df['form'].str.upper().str.strip().isin(target_forms)]
                
                if df_filtered.empty:
                    if verbose: print(f"    No matching forms found in {yr} QTR{qt}.")
                    continue
                    
                if verbose: print(f"    Found {len(df_filtered)} filings to ingest.")
                
                # 2. Concurrent Throttled Download & Process
                # We use a Semaphore to allow multiple pending requests while the RateLimiter sequences them
                sem = asyncio.Semaphore(20) # Allow 20 concurrent requests to hide latency
                
                async def _throttled_ingest(row, pbar):
                    async with sem:
                        url = row['text_url']
                        acc_num = row['accession_number']
                        filing_date = row['date_filed'] # This is a date object or string 'YYYY-MM-DD'
                        
                        # Derive granular MM/DD paths
                        date_parts = str(filing_date).split('-')
                        if len(date_parts) >= 3:
                            month, day = date_parts[1], date_parts[2]
                            row_output_dir = output_dir / month / day
                        else:
                            row_output_dir = output_dir
                            
                        row_output_dir.mkdir(parents=True, exist_ok=True)

                        try:
                            # Check if skipping
                            md_path = row_output_dir / f"{acc_num}.sgml.md.zst"
                            if not replace and md_path.exists():
                                return True
                                
                            # Download using the async rate-limited method
                            resp = await sec_api._get_async(url)
                            content = resp.content
                            
                            # Skip processing if download_only
                            if download_only:
                                output_path = row_output_dir / f"{acc_num}.sgml"
                                with open(output_path, "wb") as f_out:
                                    f_out.write(content)
                                return True
                            
                            # Process into the granular directory
                            return _process_and_archive_filing(content, row_output_dir, acc_num, verbose=False, replace=replace, forms=forms, extract_only=True)
                        except Exception as e:
                            logger.error(f"Failed to ingest filing {acc_num}: {e}")
                            return False
                        finally:
                            pbar.update(1)

                success_count = 0
                from tqdm import tqdm
                pbar = tqdm(total=len(df_filtered), desc=f"Ingesting {yr} QTR{qt}", disable=not verbose)
                
                data_dir = pathlib.Path(os.getenv("EDGAR_LOCAL_DATA_DIR", "/media/data"))
                output_dir = data_dir / "data" / str(yr) / f"QTR{qt}"
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # Use asyncio.gather to run tasks concurrently
                tasks = [_throttled_ingest(row, pbar) for _, row in df_filtered.iterrows()]
                results = await asyncio.gather(*tasks)
                success_count = sum(1 for r in results if r)
                
                pbar.close()
                if verbose: print(f"  Finished {yr} QTR{qt}: Ingested {success_count} filings.")
        return # Exit Targeted Mode
        
    # --- Bulk Mode (Daily Feeds) ---
    async with aiohttp.ClientSession() as session:
        if not year or backfill:
            years = []
            discovery_success = False
            
            # --- Try Network Discovery First ---
            try:
                if verbose: print(f"Connecting to SEC Root Feed at {base_url}...")
                page_content = await downloader.fetch_text(session, base_url)
                if page_content:
                    soup = BeautifulSoup(page_content, features="html.parser")
                    table = soup.find("table")
                    
                    # Use provided year as start_year, default to 1997
                    start_year = year or begin_year
                    
                    if days:
                        years = sorted(list(set(int(str(d)[:4]) for d in days)))
                    elif backfill:
                        for tr in table.find_all('tr'):
                            row = tr.find_all('td')
                            if len(row) > 0:
                                yr_text = row[0].find('a').attrs['href'].replace("/", "")
                                if yr_text != "" and yr_text.isdigit():
                                    if int(yr_text) >= start_year:
                                        years.append(int(yr_text))
                    else:
                        years = [datetime.datetime.now().year]
                    discovery_success = True
            except Exception as e:
                if verbose: print(f"  Network discovery failed: {e}. Falling back to local filesystem...")
            
            # --- Local Filesystem Fallback ---
            if not discovery_success or os.getenv("EDGAR_USE_LOCAL_DATA") == "1":
                data_dir = pathlib.Path(os.getenv("EDGAR_LOCAL_DATA_DIR", "/media/data"))
                local_years = []
                if data_dir.exists():
                    for p in data_dir.iterdir():
                        if p.is_dir() and p.name.isdigit():
                            yr = int(p.name)
                            if yr >= (year or begin_year):
                                local_years.append(yr)
                
                # Merge or replace based on state
                if not years:
                    years = sorted(local_years)
                else:
                    years = sorted(list(set(years) | set(local_years)))
                    
            if not years:
                if verbose: print("CRITICAL: No years found for discovery (Network or Local). Check your connection and 'EDGAR_LOCAL_DATA_DIR'.")
                return
        else:
            # Targeted Mode: Single Year (No Backfill)
            years = [year]
            
        years.sort()
        
        data_dir = pathlib.Path(os.getenv("EDGAR_LOCAL_DATA_DIR"))
        
        for year in years:
            data_dir = pathlib.Path(os.getenv("EDGAR_LOCAL_DATA_DIR"))
            year_dir = data_dir / str(year)
            
            if not qtr:
                quarters = []
                discovery_success = False
                year_url = f"{base_url}{year}/"
                try:
                    if verbose: print(f"Checking year {year} index at {year_url}...")
                    year_page = await downloader.fetch_text(session, year_url)
                    if year_page:
                        year_soup = BeautifulSoup(year_page, features="html.parser")
                        year_table = year_soup.find("table")
                        for tr in year_table.find_all('tr'):
                            row = tr.find_all('td')
                            if len(row) > 0:
                                qtr_text = row[0].find('a').attrs['href'].replace("/", "")
                                if qtr_text.startswith("QTR"):
                                    quarters.append(qtr_text)
                        discovery_success = True
                except Exception as e:
                    if verbose: print(f"  Network QTR discovery failed for {year}: {e}. Checking local...")
                
                if not discovery_success or os.getenv("EDGAR_USE_LOCAL_DATA") == "1":
                    if year_dir.exists():
                        local_qtrs = [p.name for p in year_dir.iterdir() if p.is_dir() and p.name.startswith("QTR")]
                        quarters = sorted(list(set(quarters) | set(local_qtrs)))
            else:
                quarters = [f"QTR{qtr}"]
                            
            for quarter in quarters:
                qtr_url = f"{base_url}{year}/{quarter}/"
                discovery_success = False
                feeds = []
                
                try:
                    if verbose: print(f"Checking quarter {quarter} index at {qtr_url}...")
                    qtr_page = await downloader.fetch_text(session, qtr_url)
                    if qtr_page:
                        qtr_soup = BeautifulSoup(qtr_page, features="html.parser")
                        qtr_table = qtr_soup.find("table")
                        for tr in qtr_table.find_all('tr'):
                            row = tr.find_all('td')
                            if len(row) > 0:
                                filename = row[0].find('a').attrs['href'].replace("/", "")
                                if filename.endswith('.gz') or filename.endswith('.zst'):
                                    feeds.append(filename)
                        discovery_success = True
                except Exception as e:
                    if verbose: print(f"  Network Feed discovery failed for {year}/{quarter}: {e}. Checking local...")
                
                if not discovery_success or os.getenv("EDGAR_USE_LOCAL_DATA") == "1":
                    raw_dir = year_dir / quarter / "RAW"
                    if raw_dir.exists():
                        local_feeds = [p.name for p in raw_dir.iterdir() if p.is_file() and (p.name.endswith('.gz') or p.name.endswith('.zst'))]
                        feeds = sorted(list(set(feeds) | set(local_feeds)))
                
                download_tasks = []
                for filename in feeds:
                    # Filter days if needed
                    day_prefix = filename.split(".")[0]
                    if days and day_prefix not in [str(d) for d in days]:
                        continue
                        
                    # On disk we store as .zst
                    zst_filename = filename.replace('.gz', '.zst')
                    file_url = f"{base_url}{year}/{quarter}/{filename}"
                    
                    # Extract MM/DD for granular extraction
                    month, day = day_prefix[4:6], day_prefix[6:8]
                    
                    # ARCHIVE PATH: Consistent with legacy spec (YYYY/QTRX/RAW)
                    archive_path = data_dir / "data" / str(year) / quarter / "RAW" / zst_filename
                    # EXTRACTION PATH: Granular to prevent filesystem saturation
                    extraction_dir = data_dir / "data" / str(year) / quarter / month / day
                    
                    # --- Skip Logic (Download) ---
                    legacy_gz = archive_path.with_suffix('.gz')
                    # Check archive location first
                    if not replace and archive_path.exists():
                        if verbose: print(f"  Skipping existing feed: {zst_filename}")
                        continue
                    elif not replace and legacy_gz.exists():
                        if verbose: print(f"  Found legacy archive {legacy_gz.name}, queueing migration to Zstd...")
                        # We'll handle this as a special task in the parallel pool
                        download_tasks.append(("MIGRATE", legacy_gz, extraction_dir))
                        continue
                    
                    download_tasks.append((file_url, archive_path, extraction_dir))
                
                if download_tasks:
                    from tqdm import tqdm
                    desc = f"Ingesting {year} {quarter}"
                    pbar = tqdm(total=len(download_tasks), desc=desc, disable=not verbose)
                    
                    sem = asyncio.Semaphore(2) # Reduced to 2 concurrent large downloads to avoid 429s
                    
                    async def _throttled_download(file_url, archive_path, extraction_dir):
                        async with sem:
                            if file_url == "MIGRATE":
                                # archive_path here is actually the legacy_gz path
                                legacy_gz = archive_path
                                zst_path = legacy_gz.with_suffix('.zst')
                                loop = asyncio.get_event_loop()
                                await loop.run_in_executor(None, migrate_archives_to_zstd_worker, legacy_gz, verbose, True)
                                success = True
                                archive_path = zst_path
                            else:
                                success = await downloader.download_and_compress(session, file_url, archive_path, verbose=verbose, replace=replace, download_only=download_only, forms=forms, pbar=pbar)
                            
                            if success and not download_only:
                                # Trigger extraction to the granular directory
                                loop = asyncio.get_event_loop()
                                task = loop.run_in_executor(None, extract_and_compress_tar_feed, str(archive_path), False, verbose, replace, forms, extraction_dir)
                                downloader.extractions.append({
                                    "url": file_url,
                                    "path": archive_path,
                                    "future": task
                                })
                            return success

                    download_results = await asyncio.gather(*[_throttled_download(url, path, ext_dir) for url, path, ext_dir in download_tasks])
                    results_count = sum(1 for r in download_results if r)
                    failure_count = len(download_results) - results_count
                    pbar.close()
                    
                    # --- Finalization & Self-Healing Retry ---
                    if downloader.extractions:
                        if verbose: print(f"  Awaiting background worker synchronization for {quarter}...")
                        extraction_results = await asyncio.gather(*[e["future"] for e in downloader.extractions])
                        
                        # Identify failures for immediate retry
                        failed_items = []
                        for i, success in enumerate(extraction_results):
                            if not success:
                                failed_items.append(downloader.extractions[i])
                        
                        if failed_items:
                            if verbose: print(f"  Detected {len(failed_items)} failed extractions. Triggering forced re-download...")
                            # Clear old tasks before re-queueing
                            processed_before_retry = [e for e in downloader.extractions if e not in failed_items]
                            downloader.extractions = [] 
                            
                            retry_tasks = []
                            for item in failed_items:
                                # Force replace=True for the retry attempt
                                retry_tasks.append(downloader.download_and_compress(session, item["url"], item["path"], verbose=verbose, replace=True, download_only=download_only, forms=forms))
                            
                            if retry_tasks:
                                await asyncio.gather(*retry_tasks)
                                # Final wait for the NEW retry extractions
                                if verbose: print(f"  Awaiting rescue extraction results...")
                                await asyncio.gather(*[e["future"] for e in downloader.extractions])
                        
                        downloader.extractions = [] # Reset for next quarter
                    
                    if verbose:
                        print(f"  Finished {year} {quarter}: Processed {results_count} daily feeds.")


async def process_bulk_filings_async(year=None, qtr=None, days=None, forms: List[str] = None, replace: bool = False, verbose: bool = False):
    """
    Purely local processing: scans the filesystem for daily archives and processes them.
    Does not attempt any network connections.
    """
    root_dir = pathlib.Path(os.getenv("EDGAR_LOCAL_DATA_DIR", "/media/data"))
    data_dir = root_dir / "data"
    if not data_dir.exists():
        data_dir = root_dir # Fallback to root if 'data' subfolder isn't used
        
    if not data_dir.exists():
        if verbose: print(f"CRITICAL: Data directory {data_dir} does not exist.")
        return

    # --- Year Discovery ---
    years = []
    found_years = [int(p.name) for p in data_dir.iterdir() if p.is_dir() and p.name.isdigit()]
    
    if not year:
        years = found_years
    else:
        # Treat provided year as the starting floor
        years = [yr for yr in found_years if yr >= year]
    
    years.sort()

    for yr in years:
        yr_dir = data_dir / str(yr)
        if not yr_dir.exists():
            continue

        # --- Quarter Discovery ---
        quarters = []
        if not qtr:
            for p in yr_dir.iterdir():
                if p.is_dir() and p.name.startswith("QTR") and p.name[3:].isdigit():
                    quarters.append(p.name)
        else:
            quarters = [f"QTR{qtr}"]
        quarters.sort()

        for qt in quarters:
            raw_dir = yr_dir / qt / "RAW"
            if not raw_dir.exists():
                continue

            # --- Feed Discovery ---
            feeds_dict = {} # prefix -> filename
            if not days:
                for p in raw_dir.iterdir():
                    if p.is_file() and (p.name.endswith(".nc.tar.gz") or p.name.endswith(".nc.tar.zst")):
                        prefix = p.name.split(".")[0]
                        # Prioritize .zst if both exist
                        if prefix not in feeds_dict or p.name.endswith(".zst"):
                            feeds_dict[prefix] = p.name
            else:
                for d in days:
                    if str(d).startswith(str(yr)):
                        # Look for existing file in RAW
                        for ext in ('.nc.tar.zst', '.nc.tar.gz'):
                            if (raw_dir / f"{d}{ext}").exists():
                                feeds_dict[str(d)] = f"{d}{ext}"
                                break
            feeds = sorted(feeds_dict.values())

            if not feeds:
                continue

            if verbose: print(f"Processing {yr} {qt}: Found {len(feeds)} archives.")
            
            from tqdm import tqdm
            pbar = tqdm(total=len(feeds), desc=f"Processing {yr} {qt}", disable=not verbose)
            
            # Use ProcessPoolExecutor for true multi-core CPU-bound tasks
            from concurrent.futures import ProcessPoolExecutor
            cpu_count = os.cpu_count() or 4
            # We use a slightly higher worker count than the semaphore to keep the pool warm
            # but the semaphore controls the actual parallel I/O load.
            executor = ProcessPoolExecutor(max_workers=cpu_count)
            # Cap parallel feeds to avoid DB connection exhaustion (10 threads per feed * 8 feeds = 80 connections)
            sem_limit = min(cpu_count, 8)
            sem = asyncio.Semaphore(sem_limit) 
            
            async def _process_feed(filename):
                async with sem:
                    archive_path = raw_dir / filename
                    loop = asyncio.get_event_loop()
                    
                    # Check if it needs conversion (.gz -> .zst)
                    if archive_path.suffix == ".gz":
                        if verbose: print(f"  Migrating legacy archive {filename} to Zstandard for performance...")
                        # migrate_archives_to_zstd_worker is synchronous, run in executor
                        await loop.run_in_executor(executor, migrate_archives_to_zstd_worker, archive_path, verbose, True)
                        # Update archive_path to the new .zst location
                        archive_path = archive_path.with_suffix('.zst')
                    
                    day_prefix = filename.split(".")[0]
                    month, day = day_prefix[4:6], day_prefix[6:8]
                    extraction_dir = yr_dir / qt / month / day
                    
                    # Use executor for CPU-bound extraction
                    # extract_and_compress_tar_feed is where the bulk of the work happens
                    await loop.run_in_executor(executor, extract_and_compress_tar_feed, str(archive_path), False, verbose, replace, forms, extraction_dir, True)
                    pbar.update(1)

            # Parallel execution of all feeds in the quarter
            try:
                await asyncio.gather(*[_process_feed(f) for f in feeds])
            finally:
                executor.shutdown(wait=True)
            pbar.close()

def process_bulk_filings(year=None, qtr=None, days=None, forms=None, replace=False, verbose=False):
    import asyncio
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    loop.run_until_complete(process_bulk_filings_async(
        year=year,
        qtr=qtr,
        days=days,
        forms=forms,
        replace=replace,
        verbose=verbose
    ))

def download_bulk_filings(year=None, qtr=None, backfill=True, verbose=False, replace=False, days=None, forms=None, download_only=False):

    import asyncio
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    loop.run_until_complete(download_bulk_filings_async(
        year=year, 
        qtr=qtr, 
        backfill=backfill, 
        verbose=verbose, 
        days=days, 
        replace=replace,
        forms=forms,
        download_only=download_only
    ))
def process_formtypes():
    try:
        pdf_path = "https://www.sec.gov/info/edgar/forms/edgform.pdf"
        dfs = tabula.read_pdf(pdf_path, user_agent=os.getenv('EDGAR_IDENTITY'), pages='2-31', lattice=True)
        forms = pd.concat(dfs)
        forms['Submission Type'] = forms['Submission Type'].str.replace('\r', ' ')
        forms.reset_index(inplace=True, drop=True)
        forms = forms[['Submission Type', 'Description']][~pd.isnull(forms['Submission Type'])].copy()
        forms['Submission Type'] = forms['Submission Type'].str.split(', ')
        forms = forms.explode('Submission Type')
        forms.rename(columns={'Submission Type': 'Form'}, inplace=True)
        forms.reset_index(inplace=True, drop=True)
        for form in forms.itertuples():
            try:
                f = FormIndex.objects.get(form=form.Form)
            except:
                f = FormIndex()

            f.form = form.Form
            f.description = form.Description
            f.save()
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')

def process_company():
    """
    populate company table
    """
    try:
        df = sec_api.get_cik_lookup_data()
        ciknames = {}
        company_objects = []
        for row in df.itertuples():
            ciknames[row.cik] = row.name
        for cik in ciknames.keys():
            company_objects.append(
                Company(cik=cik, cik_name=ciknames[cik]))
        Company.objects.bulk_create(
            company_objects,
            update_conflicts=True, 
            update_fields=['cik_name'],
            unique_fields=['cik'])
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')
        
def process_companyinfo_cik(cik:int):
    try:
        processed = False
        company = Company.objects.get(cik=cik)
        c = edgar.Company(company.cik)
        cik = company.cik
        ci = CompanyInfo()
        ci.cik = company
        ci.name = c.name
        ci.is_company = c.is_company
        ci.category = c.category
        ci.description = c.description
        ci.entity_type = c.entity_type
        ci.ein = c.ein
        ci.industry = c.industry
        ci.sic = c.sic
        ci.sic_description = c.sic_description
        ci.state_of_incorporation = c.state_of_incorporation
        ci.state_of_incorporation_description = c.state_of_incorporation_description
        ci.fiscal_year_end = c.fiscal_year_end
        ci.mailing_address = c.mailing_address.__dict__
        ci.business_addres = c.business_address.__dict__
        ci.phone = c.phone
        ci.tickers = c.tickers
        ci.exchanges = c.exchanges
        ci.former_names = c.former_names
        ci.flags = c.flags
        ci.insider_transaction_for_owner_exists = c.insider_transaction_for_owner_exists
        ci.insider_transaction_for_issuer_exists = c.insider_transaction_for_issuer_exists
        ci.website = c.website
        ci.investor_website = c.investor_website
        try:
            oci = CompanyInfo.objects.get(cik=c.cik)
        except CompanyInfo.DoesNotExist:
            oci = None
        if oci != ci:
            ci.processed = True
            ci.save()
        return processed
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{cik}: {error} - {details}')

@shared_task
def process_companyinfo(cik:int=0, multiple:bool=False, upsert:bool=False):
    try:
        print('Getting Company Objects')
        company = None
        if cik == 0 or multiple:
            if not upsert:
                # ciks in company not in companyinfo
                company_ciks = set(
                    Company.objects \
                        .all() \
                        .filter(cik__gte=cik) \
                        .order_by('cik') \
                        .values_list('cik', flat=True))
                companyinfo_ciks = set(
                    CompanyInfo.objects \
                        .all() \
                        .filter(cik__gte=cik) \
                        .order_by('cik') \
                        .values_list('cik', flat=True))
                get_ciks = company_ciks.union(companyinfo_ciks) \
                    - company_ciks.intersection(companyinfo_ciks)
                companies = Company.objects.all().filter(cik__in=get_ciks).order_by('cik')
            else:
                # ciks >= passed in cik
                companies = Company.objects.all().filter(cik__gte=cik).order_by('cik')
        else:
            # single cik
            companies = [Company.objects.get(cik=cik)]
        print(f'Got {len(companies)} Company Objects')
        i = 0
        total = len(companies)
        for company in companies:
            i += 1
            processed = process_companyinfo_cik(company.cik)
            if processed:
                action = 'processed'
            else:
                action = 'skipped'
            print(f"{action} {company.cik}: {i} of {total}")
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        if company is None:
            err_cik = cik
        else:
            err_cik = company.cik
        sys.stderr.write(f'{err_cik}: {error} - {details}')
        
def process_companyfacts_bulk():
    data_path = pathlib.Path(os.getenv('EDGAR_LOCAL_DATA_DIR'))
    facts_path = data_path / 'companyfacts'
    facts_files = facts_path.glob('*.json')
    
    results = []
    for filenm in facts_files:
        cik = int(filenm.stem[3:])
        res = process_companyfacts_cik.s(cik).apply_async(serializer='json')
        results.append(res)
    for res in results:
        res.get()
    
@shared_task
def process_companyfacts_cik(cik:int):
    processed = False
    try:
        from openedgar.sec_api import sec_api
        import hyperstreamdb as hs
        
        # Pull from SEC securely with fast parsed DataFrames
        facts_df, meta_df = sec_api.get_company_facts_pandas(cik)
        
        if facts_df is not None and not facts_df.empty:
            # Postgres Model: Save structural FactIndex metadata for search
            for row in meta_df.itertuples():
                try:
                    fi = FactIndex.objects.get(fact=row.fact)
                except FactIndex.DoesNotExist:
                    fi = FactIndex()
                fi.fact = row.fact
                fi.label = row.label
                fi.description = row.description
                fi.save()
                
            # Formatting the Facts dataframe payload
            facts_df['cik'] = str(cik).zfill(10)
            facts_df['id'] = facts_df['fact'].astype(str) + '_' + facts_df['accn'].astype(str)
            facts_df['val'] = facts_df['val'].fillna(0.0)
            facts_df['fy'] = facts_df['fy'].fillna(0)
            facts_df['fp'] = facts_df['fp'].fillna('')
            facts_df['frame'] = facts_df['frame'].fillna('')

            # HyperStreamDB Persistance Bypassing Django CompanyFact
            data_dir = os.getenv('EDGAR_LOCAL_DATA_DIR', '/tmp')
            hs_uri = os.environ.get("HYPERSTREAM_FACTS_URI", f"file://{data_dir}/hyperstream_facts")
            table = hs.Table(uri=hs_uri)
            
            # Extreme high throughput Vector/Facts engine ingestion
            table.upsert(facts_df, key_column='id')
            processed = True
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')
        processed = False
    finally:
        print(cik, processed)
    return processed
        
@shared_task
def process_companyfacts(cik:int=0, multiple:bool=False, upsert:bool=False):
    try:
        print('Getting CompanyInfo Objects')
        company = None
        if cik == 0 or multiple:
            if not upsert:
                # ciks in company not in companyinfo
                company_ciks = set(
                    CompanyInfo.objects \
                        .all() \
                        .filter(cik__gte=cik) \
                        .filter(is_company=True) \
                        .order_by('cik') \
                        .values_list('cik', flat=True))
                companyfact_ciks = set(
                    CompanyFact.objects \
                        .all() \
                        .filter(cik__gte=cik) \
                        .order_by('cik') \
                        .values_list('cik', flat=True))
                ciks = company_ciks.union(companyfact_ciks) \
                    - company_ciks.intersection(companyfact_ciks)
              
            else:
                # ciks >= passed in cik 
                ciks = CompanyInfo.objects \
                    .all() \
                    .filter(cik__gte=cik) \
                    .filter(is_company=True) \
                    .order_by('cik') \
                    .values_list('cik', flat=True)
        else:
            # single cik
            ciks = CompanyInfo.objects \
                .get(cik=cik) \
                .filter(is_company=True) \
                .values_list('cik', flat=True)
        print(f'Got {len(ciks)} CIKs')
        i = 0
        total = len(ciks)
        for cik in ciks:
            i += 1
            processed = process_companyfacts_cik(cik)
            if processed:
                action = 'processed'
            else:
                action = 'skipped'
            print(f"{action} {cik}: {i} of {total}")
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        if company is None:
            err_cik = cik
        else:
            err_cik = company.cik
        sys.stderr.write(f'{err_cik}: {error} - {details}')

def process_filingindex_year(year:int, batch_size:int=1000, upsert:bool=False, formtypes:Iterable[str]=None):
    filing_index = sec_api.get_filings([year])
    if not filing_index.empty:
        if formtypes is not None:
            filing_index = filing_index[filing_index['form'].isin(formtypes)]
        if not upsert:
            accnos = FilingIndex.objects.all() \
                .filter(date_filed__gte=datetime.date(year, 1, 1)) \
                .filter(date_filed__lte=datetime.date(year, 12, 31)) \
                .order_by('accession_number') \
                .values_list('accession_number', flat=True)
            filing_index = filing_index[~(filing_index['accession_number'].isin(accnos))]
        filing_ct: int = filing_index.shape[0]
        for start in range(0, filing_ct, batch_size):
            end = min(start + batch_size, filing_ct)
            filing_objects = []
            filing_index.drop_duplicates(subset=['accession_number'], keep='last', inplace=True)
            for filing in filing_index.iloc[start:end].itertuples():
                f = FilingIndex()
                try:
                    company = Company.objects.get(cik=filing.cik)
                except Company.DoesNotExist:
                    company = Company.objects.create(cik=filing.cik, cik_name=filing.company)
                f.accession_number = filing.accession_number
                try:
                    frm = FormIndex.objects.get(form=filing.form)
                except FormIndex.DoesNotExist:
                    frm = FormIndex.objects.create(form=filing.form)
                f.form_type = frm
                f.date_filed = filing.filing_date
                f.cik = company
                f.company = filing.company
                try:
                    of = FilingIndex.objects.get(accession_number=f.accession_number)
                except FilingIndex.DoesNotExist:
                    of = None
                if f != of:
                    filing_objects.append(f)
            FilingIndex.objects.bulk_create(
                filing_objects,
                update_conflicts=True, 
                unique_fields=['accession_number'],
                update_fields=['cik', 'company', 'form_type', 'date_filed']
                )
            print(f"FilingIndex Batch {year}: {start} - {end}")
            
@shared_task  
def process_filingindex(backfill:bool=False, upsert:bool=False, formtypes:Iterable[str]=None):
    try:
        if not backfill:
            """
            Get list of index files for a given year.
            :param year: filing year to retrieve
            :return:
            """
            year = datetime.date.today().year
            
            # Log entrance
            logger.info("Locating form index list for {0}".format(year))

            # Form index dataframe
            process_filingindex_year(year, upsert=upsert)
          
        else:
            min_year: int = 1950
            max_year: int = 2050
            # Log entrance
            logger.info("Retrieving form index list")

            # Retrieve dataframe
            for year in range(min_year, max_year + 1):
                process_filingindex_year(year, upsert=upsert)
        
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')
        
def process_filings(year:int=None, upsert=False, backfill:bool=False):
    try:
        if not backfill:
            """
            Get list of index files for a given year.
            :param year: filing year to retrieve
            :return:
            """
            if year is None:
                year = datetime.date.today().year
            
            # Log entrance
            logger.info("Locating form index list for {0}".format(year))

            # Form index dataframe
            filings_df = sec_api.get_filings([year])
            filings = [row for row in filings_df.itertuples()]
            filing_ct: int = len(filings)
            
            # Log exit
            logger.info("Successfully located {0} form index files for {1}".format(filing_ct, year))
        else:
            min_year: int = 1950
            max_year: int = 2050
            # Log entrance
            logger.info("Retrieving form index list")

            # Retrieve dataframe
            filings_df = sec_api.get_filings(list(range(min_year, max_year + 1)))
            filings = [row for row in filings_df.itertuples()]
            filing_ct: int = len(filings)
            
            # Log exit
            logger.info("Successfully located {0} form index files from {1} to {2}".format(filing_ct, min_year, max_year))
        
        filing_objects = []  
        for filing in filings:
            f = Filing()
            try:
                company = Company.objects.get(cik=filing.cik)
            except Company.DoesNotExist:
                company = Company.objects.create(cik=filing.cik, cik_name=filing.company)
            
            # Use secsgml via sec_api to fetch robust metadata lazily
            doc_count, accept_dt, _ = sec_api.get_filing_sgml_header(filing.text_url)
            
            f.document_count = doc_count
            f.acceptance_datetime = accept_dt
            f.accession_number = filing.accession_number
            f.form_type = filing.form
            f.date_filed = filing.filing_date
            f.cik = company
            f.company = filing.company
            f.document_url = filing.document_url
            f.homepage_url = filing.homepage_url
            f.text_url = filing.text_url
            try:
                of = Filing.objects.get(accession_number=f.accession_number)
            except Filing.DoesNotExist:
                of = None
            if f != of:
                filing_objects.append(f)
        Filing.objects.bulk_create(
            filing_objects,
            update_conflicts=True, 
            unique_fields=['accession_number'])
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')

# this really really looks like its the full text document being used, due to start_pos and end_pos                
def create_filing_documents(client, documents, filing, store_raw: bool = True, store_text: bool = True):
    """
    Create filing document records given a list of documents
    and a filing record.
    :param documents: list of documents from parse_filing
    :param filing: Filing record
    :param store_raw: whether to store raw contents
    :param store_text: whether to store text contents
    :return:
    """
    # Get client if we're using S3


    # Iterate through documents
    document_records = []
    
    # JSONB Array tracker for master Filing RAG performance
    documents_index_data = []
    
    for document in documents:
        # Create DB object
        filing_doc = FilingDocument()
        filing_doc.filing = filing
        filing_doc.type = document["type"]
        filing_doc.sequence = document["sequence"]
        filing_doc.file_name = document["file_name"]
        filing_doc.content_type = document["content_type"]
        filing_doc.description = document["description"]
        filing_doc.sha1 = document["sha1"]
        filing_doc.start_pos = document["start_pos"]
        filing_doc.end_pos = document["end_pos"]
        filing_doc.is_processed = True
        filing_doc.is_error = len(document["content"]) > 0
        document_records.append(filing_doc)

        # Use CAS for storage (Deduplicated Content-Addressable Storage)
        # We store the cleaned text only, or both if needed, but always via hash.
        if store_text and document["content_text"] is not None:
            sha1, cas_path = client.put_cas_buffer(document["content_text"])
            
            # Update the record with the actual content hash
            filing_doc.sha1 = sha1
            logger.info("Stored document in CAS: filing={0}, doc_type={1}, sha1={2}"
                        .format(filing, document["type"], sha1))
            
            # RAG Ingestion: Chunk and ingest into HyperStreamDB with autovectorization
            try:
                pipeline = get_rag_pipeline()
                pipeline.ingest_filing_chunks(
                    cik=filing.cik.cik,
                    accession_number=filing.accession_number,
                    form_type=filing.form_type,
                    date_filed=str(filing.date_filed),
                    markdown=document["content_text"]
                )
            except Exception as e:
                logger.error(f"RAG Ingestion failed for {filing.accession_number}: {e}")
        elif store_raw and len(document["content"]) > 0:
            sha1, cas_path = client.put_cas_buffer(document["content"])
            filing_doc.sha1 = sha1
            
        # Append structurally mapped chunk signature to the fast JSON pointer
        documents_index_data.append({
            "sequence": document.get("sequence", 1),
            "type": document.get("type", "doc"),
            "file_name": document.get("file_name", "unknown"),
            "sha1": filing_doc.sha1
        })

    # Create in bulk for older non-indexed relational systems
    FilingDocument.objects.bulk_create(document_records)
    
    # Securely commit the newly mapped Array Index globally backwards to the overarching Filing
    if hasattr(filing, 'documents_index'):
        filing.documents_index = documents_index_data
        filing.save(update_fields=['documents_index', 'is_processed'])
    return len(document_records)

@shared_task
def sync_security_master():
    """
    Sync the Security Master (Company table) with the official SEC ticker mapping.
    Ensures that all CIKs are mapped to their current trading symbols.
    """
    SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
    headers = {"User-Agent": os.getenv("EDGAR_IDENTITY", "Researcher/1.0")}
    
    logger.info("Starting Security Master Sync from SEC...")
    try:
        response = requests.get(SEC_TICKER_URL, headers=headers)
        response.raise_for_status()
        ticker_data = response.json()
        
        from openedgar.models import Company
        from openedgar.processes.symbology import OpenFIGIClient
        
        updated_count = 0
        new_count = 0
        
        # SEC JSON is formatted as {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
        for entry in ticker_data.values():
            cik = entry.get('cik_str')
            ticker = entry.get('ticker')
            name = entry.get('title')
            
            if cik and ticker:
                company, created = Company.objects.update_or_create(
                    cik=cik,
                    defaults={
                        'cik_name': name,
                        'ticker': ticker,
                        'is_active': True
                    }
                )
                if created:
                    new_count += 1
                else:
                    updated_count += 1
                
                # Enrich with FIGI if not present
                if not company.figi:
                    OpenFIGIClient.enrich_company_model(company)
                    
        logger.info(f"Security Master Sync Complete: {new_count} new, {updated_count} updated/enriched.")
        return {"new": new_count, "updated": updated_count}
        
    except Exception as e:
        logger.error(f"Security Master Sync failed: {e}")
        return {"error": str(e)}


def create_filing_error(row, filing_path: str):
    """
    Create a Filing error record from an index row.
    :param row:
    :param filing_path:
    :return:
    """
    # Get vars
    cik = row["CIK"]
    company_name = row["Company Name"]
    form_type = row["Form Type"]

    try:
        date_filed = dateutil.parser.parse(str(row["Date Filed"])).date()
    except ValueError:
        date_filed = None
    except IndexError:
        date_filed = None

    # Create empty error filing record
    filing = CompanyFiling()
    filing.form_type = form_type
    filing.date_filed = date_filed
    filing.path = filing_path
    filing.is_error = True
    filing.is_processed = False

    # Get company info
    try:
        company = Company.objects.get(cik=cik)

        try:
            _ = CompanyInfo.objects.get(company=company, date=date_filed)
        except CompanyInfo.DoesNotExist:
            # Create company info record
            company_info = CompanyInfo()
            company_info.company = company
            company_info.name = company_name
            company_info.sic = None
            company_info.state_incorporation = None
            company_info.state_location = None
            company_info.date = date_filed
            company_info.save()
    except Company.DoesNotExist:
        # Create company
        company = Company()
        company.cik = cik
        company.cik_name = company_name

        try:
            company.save()
        except django.db.utils.IntegrityError:
            return create_filing_error(row, filing_path)

        # Create company info record
        company_info = CompanyInfo()
        company_info.company = company
        company_info.name = company_name
        company_info.sic = None
        company_info.state_incorporation = None
        company_info.state_location = None
        company_info.date = date_filed
        company_info.save()

    # Finally update company and save
    filing.company = company
    filing.save()
    return True


@shared_task
def process_filing_index(client_type: str, file_path: str, filing_index_buffer: Union[str, bytes] = None,
                         form_type_list: Iterable[str] = None, store_raw: bool = False, store_text: bool = False):
    """
    Process a filing index from an S3 path or buffer.
    :param file_path: S3 or local path to process; if filing_index_buffer is none, retrieved from here
    :param filing_index_buffer: buffer; if not present, s3_path must be set
    :param form_type_list: optional list of form type to process
    :param store_raw:
    :param store_text:
    :return:
    """
    # Log entry
    logger.info("Processing filing index {0}...".format(file_path))

    if client_type == "S3":
        client = S3Client()
    else:
        client = LocalClient()

    # Retrieve buffer if not passed
    if filing_index_buffer is None:
        logger.info("Retrieving filing index buffer for: {}...".format(file_path))
        filing_index_buffer = client.get_buffer(file_path)

    # Write to disk to handle headaches
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(filing_index_buffer)
    temp_file.close()

    # Get main filing data structure
    filing_index_data = openedgar.clients.openedgar.list_index()
    logger.info("Parsed {0} records from index".format(filing_index_data.shape[0]))

    # Iterate through rows
    bad_record_count = 0
    for _, row in filing_index_data.iterrows():
        # Check for form type whitelist
        if form_type_list is not None:
            if row["Form Type"] not in form_type_list:
                logger.info("Skipping filing {0} with form type {1}...".format(row["File Name"], row["Form Type"]))
                continue

        # Cleanup path
        if row["File Name"].lower().startswith("data/"):
            filing_path = "edgar/{0}".format(row["File Name"])
        elif row["File Name"].lower().startswith("edgar/"):
            filing_path = row["File Name"]

        # Check if filing record exists
        try:
            filing = Filing.objects.get(path=filing_path)
            logger.info("Filing record already exists: {0}".format(filing))
        except Filing.MultipleObjectsReturned as e:
            # Create new filing record
            logger.error("Multiple Filing records found for s3_path={0}, skipping...".format(filing_path))
            logger.info("Raw exception: {0}".format(e))
            continue
        except Filing.DoesNotExist as f:
            # Create new filing record
            logger.info("No Filing record found for {0}, creating...".format(filing_path))
            logger.info("Raw exception: {0}".format(f))

            # Check if exists; download and upload to S3 if missing
            if not client.path_exists(filing_path):
                # Download
                try:
                    filing_buffer, _ = openedgar.clients.openedgar.get_buffer("/Archives/{0}".format(filing_path))
                except RuntimeError as g:
                    logger.error("Unable to access resource {0} from EDGAR: {1}".format(filing_path, g))
                    bad_record_count += 1
                    create_filing_error(row, filing_path)
                    continue

                # Upload
                client.put_buffer(filing_path, filing_buffer)

                logger.info("Downloaded from EDGAR and uploaded to {}...".format(client_type))
            else:
                # Download
                logger.info("File already stored on {}, retrieving and processing...".format(client_type))
                filing_buffer = client.get_buffer(filing_path)

            # Parse
            filing_result = process_filing(client, filing_path, filing_buffer, store_raw=store_raw, store_text=store_text)
            if filing_result is None:
                logger.error("Unable to process filing.")
                bad_record_count += 1
                create_filing_error(row, filing_path)

    # Create a filing index record
    # this bit of code below is changed from FilingIndex
    # to CompanyFiling, the columns have not been updated yet
    edgar_url = "/Archives/{0}".format(file_path).replace("//", "/")
    try:
        filing_index = Filing.objects.get(edgar_url=edgar_url)
        filing_index.total_record_count = filing_index_data.shape[0]
        filing_index.bad_record_count = bad_record_count
        filing_index.is_processed = True
        filing_index.is_error = False
        filing_index.save()
        logger.info("Updated existing filing index record.")
    except Filing.DoesNotExist:
        filing_index = Filing()
        filing_index.edgar_url = edgar_url
        filing_index.date_published = None
        filing_index.date_downloaded = datetime.date.today()
        filing_index.total_record_count = filing_index_data.shape[0]
        filing_index.bad_record_count = bad_record_count
        filing_index.is_processed = True
        filing_index.is_error = False
        filing_index.save()
        logger.info("Created new filing index record.")

    # Delete file if we make it this far
    os.remove(temp_file.name)


@shared_task
def process_filing(client, file_path: str, filing_buffer: Union[str, bytes] = None, store_raw: bool = False,
                   store_text: bool = False):
    """
    Process a filing from a path or filing buffer.
    :param file_path: path to process; if filing_buffer is none, retrieved from here
    :param filing_buffer: buffer; if not present, s3_path must be set
    :param store_raw:
    :param store_text:
    :return:
    """
    # Log entry
    logger.info("Processing filing {0}...".format(file_path))


    # Check for existing record first
    try:
        filing = Filing.objects.get(s3_path=file_path)
        if filing is not None:
            logger.error("Filing {0} has already been created in record {1}".format(file_path, filing))
            return None
    except CompanyFiling.DoesNotExist:
        logger.info("No existing record found.")
    except CompanyFiling.MultipleObjectsReturned:
        logger.error("Multiple existing record found.")
        return None

    # Get buffer
    if filing_buffer is None:
        logger.info("Retrieving filing buffer from S3...")
        filing_buffer = client.get_buffer(file_path)

    # Get main filing data structure
    filing_data = openedgar.parsers.openedgar.parse_filing(filing_buffer, extract=store_text)
    if filing_data["cik"] is None:
        logger.error("Unable to parse CIK from filing {0}; assuming broken and halting...".format(file_path))
        return None

    try:
        # Get company
        company = Company.objects.get(cik=filing_data["cik"])
        logger.info("Found existing company record.")

        # Check if record exists for date
        try:
            _ = CompanyInfo.objects.get(company=company, date=filing_data["date_filed"])

            logger.info("Found existing company info record.")
        except CompanyInfo.DoesNotExist:
            # Create company info record
            company_info = CompanyInfo()
            company_info.company = company
            company_info.name = filing_data["company_name"]
            company_info.sic = filing_data["sic"]
            company_info.state_incorporation = filing_data["state_incorporation"]
            company_info.state_location = filing_data["state_location"]
            company_info.date = filing_data["date_filed"].date() if isinstance(filing_data["date_filed"],
                                                                               datetime.datetime) else \
                filing_data["date_filed"]
            company_info.save()

            logger.info("Created new company info record.")

    except Company.DoesNotExist:
        # Create company
        company = Company()
        company.cik = filing_data["cik"]

        try:
            # Catch race with another task/thread
            company.save()

            try:
                _ = CompanyInfo.objects.get(company=company, date=filing_data["date_filed"])
            except CompanyInfo.DoesNotExist:
                # Create company info record
                company_info = CompanyInfo()
                company_info.company = company
                company_info.name = filing_data["company_name"]
                company_info.sic = filing_data["sic"]
                company_info.state_incorporation = filing_data["state_incorporation"]
                company_info.state_location = filing_data["state_location"]
                company_info.date = filing_data["date_filed"]
                company_info.save()
        except django.db.utils.IntegrityError:
            company = Company.objects.get(cik=filing_data["cik"])

        logger.info("Created company and company info records.")

    # Now create the filing record
    try:
        filing = Filing()
        filing.form_type = filing_data["form_type"]
        filing.accession_number = filing_data["accession_number"]
        filing.date_filed = filing_data["date_filed"]
        filing.document_count = filing_data["document_count"]
        filing.company = company
        filing.sha1 = hashlib.sha1(filing_buffer).hexdigest()
        filing.s3_path = file_path
        filing.is_processed = False
        filing.is_error = True
        filing.save()
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unable to create filing record: {0}".format(e))
        return None

    # Create filing document records
    try:
        create_filing_documents(client, filing_data["documents"], filing, store_raw=store_raw, store_text=store_text)
        filing.is_processed = True
        filing.is_error = False
        filing.save()
        return filing
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unable to create filing documents for {0}: {1}".format(filing, e))
        return None


@shared_task
def extract_filing(client, file_path: str, filing_buffer: Union[str, bytes] = None):
    """
    Extract the contents of a filing from an S3 path or filing buffer.
    :param file_path: S3 path to process; if filing_buffer is none, retrieved from here
    :param filing_buffer: buffer; if not present, s3_path must be set
    :return:
    """
    # Get buffer



    if filing_buffer is None:
        logger.info("Retrieving filing buffer from S3...")
        filing_buffer = client.get_buffer(file_path)

    # Get main filing data structure
    _ = openedgar.parsers.openedgar.parse_filing(filing_buffer)


@shared_task
def search_filing_document_sha1(client, sha1: str, term_list: Iterable[str], search_query_id: int, document_id: int,
                                case_sensitive: bool = False,
                                token_search: bool = False, stem_search: bool = False):
    """
    Search a filing document by sha1 hash.
    :param stem_search:
    :param token_search:
    :param sha1: sha1 hash of document to search
    :param term_list: list of terms
    :param search_query_id:
    :param document_id:
    :param case_sensitive:
    :return:
    """
    # Get buffer
    logger.info("Retrieving buffer from S3...")
    text_s3_path = pathlib.Path(S3_DOCUMENT_PATH, "text", sha1).as_posix()
    document_buffer = client.get_buffer(text_s3_path).decode("utf-8")

    # Check if case
    if not case_sensitive:
        document_buffer = document_buffer.lower()

    # TODO: Refactor search types
    # TODO: Cleanup flow for reduced recalc
    # TODO: Don't search same SHA1 repeatedly, but need to coordinate with calling process

    # Get contents
    nlp_engine = get_spacy_nlp()
    if not token_search and not stem_search:
        document_contents = document_buffer
    elif token_search:
        doc = nlp_engine(document_buffer)
        document_contents = [token.text for token in doc]
    elif stem_search:
        doc = nlp_engine(document_buffer)
        document_contents = [token.lemma_ for token in doc]

    # For term in term list
    counts = {}
    for term in term_list:
        if stem_search:
            term_doc = nlp_engine(term)
            term = term_doc[0].lemma_ if len(term_doc) > 0 else term

        if case_sensitive:
            counts[term] = document_contents.count(term)
        else:
            counts[term] = document_contents.count(term.lower())

    search_query = None
    results = []
    for term in counts:
        if counts[term] > 0:
            # Get search query if empty
            if search_query is None:
                search_query = SearchQuery.objects.get(id=search_query_id)

            # Get term
            search_term = SearchQueryTerm.objects.get(search_query_id=search_query_id, term=term)

            # Create result
            result = SearchQueryResult()
            result.search_query = search_query
            result.filing_document_id = document_id
            result.term = search_term
            result.count = counts[term]
            results.append(result)

    # Create if any
    if len(results) > 0:
        SearchQueryResult.objects.bulk_create(results)
    logger.info("Found {0} search terms in document sha1={1}".format(len(results), sha1))
    return True


@shared_task
def extract_filing_document_data_sha1(client, sha1: str):
    """
    Extract structured data from a filing document by sha1 hash, e.g.,
    dates, money, noun phrases.
    :param sha1:
    :param document_id:
    :return:
    """
    # Get buffer
    logger.info("Retrieving buffer from S3...")
    text_s3_path = pathlib.Path(S3_DOCUMENT_PATH, "text", sha1).as_posix()
    document_buffer = client.get_buffer(text_s3_path).decode("utf-8")

    # TODO: Build your own database here.
    _ = len(document_buffer)

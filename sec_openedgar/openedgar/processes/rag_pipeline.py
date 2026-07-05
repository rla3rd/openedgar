import logging
import os
import pathlib
import pandas as pd
import hyperstreamdb as hdb
from typing import List, Dict, Any, Optional
import re
import multiprocessing

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
        Process-isolated wrapper to ingest atomic chunks.
        """
        ctx = multiprocessing.get_context('spawn')
        p = ctx.Process(
            target=_run_ingest_process,
            args=(self.uri, cik, accession_number, form_type, date_filed, markdown)
        )
        p.start()
        p.join()
        if p.exitcode != 0:
            raise RuntimeError(f"Ingestion process exited with non-zero code: {p.exitcode}")

    def _raw_ingest_filing_chunks(self, cik: str, accession_number: str, form_type: str, date_filed: str, markdown: str):
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
                "end_pos": m_chunk["end"],
                "embedding": None
            })
            
        if batch:
            df = pd.DataFrame(batch)
            self.table.write(df, mode='append')
            self.table.commit()
            logger.info(f"Ingested {len(batch)} atomic chunks for {accession_number}.")

    def query(self, text: str, k: int = 5, cik: str = None, accession_number: str = None) -> pd.DataFrame:
        """
        Process-isolated wrapper for vector search.
        """
        ctx = multiprocessing.get_context('spawn')
        queue = ctx.Queue()
        p = ctx.Process(
            target=_run_query_process,
            args=(self.uri, text, k, cik, accession_number, queue)
        )
        p.start()
        p.join()
        if p.exitcode != 0:
            raise RuntimeError(f"Query process exited with non-zero code: {p.exitcode}")
        records = queue.get()
        return pd.DataFrame(records)

    def _raw_query(self, text: str, k: int = 5, cik: str = None, accession_number: str = None) -> pd.DataFrame:
        search_query = self.table.query()
        if cik:
            search_query = search_query.filter(f"cik = '{str(cik).zfill(10)}'")
        if accession_number:
            search_query = search_query.filter(f"accession_number = '{accession_number}'")
        
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

    async def query_local_llm(self, prompt: str) -> str:
        """
        Queries the local LLM running in LM Studio.
        """
        import asyncio
        from sec_research.utils.inference import InferenceProvider
        
        provider = InferenceProvider(
            provider_type="openai",
            model=os.getenv("LOCAL_LLM_MODEL", "qwen/qwe3.6-27b"),
            api_url=os.getenv("LOCAL_LLM_URL", "http://localhost:1234/v1/chat/completions"),
            api_key="no-key"
        )
        
        res = await asyncio.to_thread(provider.call, prompt, "You are a professional financial research assistant.")
        return res


def _run_ingest_process(uri, cik, accession_number, form_type, date_filed, markdown):
    from openedgar.processes.rag_pipeline import ModernRAGPipeline
    pipeline = ModernRAGPipeline(uri=uri)
    pipeline._raw_ingest_filing_chunks(cik, accession_number, form_type, date_filed, markdown)


def _run_query_process(uri, text, k, cik, accession_number, queue):
    from openedgar.processes.rag_pipeline import ModernRAGPipeline
    pipeline = ModernRAGPipeline(uri=uri)
    results_df = pipeline._raw_query(text, k, cik, accession_number)
    records = results_df.to_dict(orient='records')
    queue.put(records)



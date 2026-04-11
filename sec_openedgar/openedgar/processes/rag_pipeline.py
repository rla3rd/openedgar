import logging
import os
import pathlib
import pandas as pd
import hyperstreamdb as hdb
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class ModernRAGPipeline:
    def __init__(self, uri: str = None):
        data_dir = os.getenv('EDGAR_LOCAL_DATA_DIR', '/tmp')
        self.uri = uri or os.environ.get("HYPERSTREAM_RAG_URI", f"file://{data_dir}/hyperstream_rag_v2")
        self._table = None

    @property
    def table(self):
        if self._table is None:
            self._ensure_table()
        return self._table

    def _ensure_table(self):
        """Ensures the HyperStreamDB table exists with Autovectorization enabled."""
        if not os.path.exists(self.uri.replace("file://", "")):
            # Define schema with a content field and a vector field
            schema = hdb.Schema([
                hdb.Field("id", hdb.DataType.string()),
                hdb.Field("cik", hdb.DataType.string()),
                hdb.Field("accession_number", hdb.DataType.string()),
                hdb.Field("form_type", hdb.DataType.string()),
                hdb.Field("date_filed", hdb.DataType.string()),
                hdb.Field("section", hdb.DataType.string()), # e.g. "Item 1A"
                hdb.Field("content", hdb.DataType.string()),
                hdb.Field("embedding", hdb.DataType.vector(1024)) # Assuming BGE-M3 or similar
            ])
            
            self._table = hdb.Table.create(self.uri, schema)
            logger.info(f"Created new HyperStreamDB table at {self.uri}")
            
            # Metadata Indexing & Optimization
            try:
                # Primary Key for unique chunk identification
                self._table.add_primary_key("id")
                # Secondary Indexes for fast CIK/Type filtering
                self._table.add_index("cik")
                self._table.add_index("accession_number")
                self._table.add_index("form_type")
                
                # Autovectorization (Background)
                # This ensures any text written to 'content' is automatically vectorized by the DB
                self._table.define_embedding(
                    column="content", 
                    function="bge-m3", 
                    vector_column="embedding"
                )
                logger.info("Metadata indexes and Autovectorization (bge-m3) enabled.")
            except Exception as e:
                # We log but don't crash, as fallback to manual search is possible
                logger.warning(f"Metadata optimization partially failed: {e}")
        else:
            self._table = hdb.Table(self.uri)
            logger.info(f"Connected to HyperStreamDB table at {self.uri}")

    def chunk_markdown(self, markdown: str, chunk_size: int = 1500, overlap: int = 200) -> List[str]:
        """
        Simple structural chunking for Markdown. 
        Splits by headers first, then by size if needed.
        """
        # Split by headers (## or ###)
        sections = re.split(r'\n(#{1,3}\s+.*)\n', markdown)
        
        chunks = []
        current_chunk = ""
        
        for section in sections:
            if len(current_chunk) + len(section) < chunk_size:
                current_chunk += section + "\n"
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                # If a single section is too big, sub-split it
                if len(section) > chunk_size:
                    for i in range(0, len(section), chunk_size - overlap):
                        chunks.append(section[i:i + chunk_size])
                    current_chunk = ""
                else:
                    current_chunk = section + "\n"
                    
        if current_chunk:
            chunks.append(current_chunk.strip())
            
        return chunks

    def ingest_filing_chunks(self, cik: str, accession_number: str, form_type: str, date_filed: str, markdown: str):
        """
        Chunks and ingests structured markdown directly into HyperStreamDB.
        Leverages DB-side autovectorization for speed.
        """
        chunks = self.chunk_markdown(markdown)
        
        batch = []
        for i, chunk_text in enumerate(chunks):
            # We don't need to generate embeddings here! The DB does it on .write()
            batch.append({
                "id": f"{accession_number}_{i}",
                "cik": str(cik).zfill(10),
                "accession_number": accession_number,
                "form_type": form_type,
                "date_filed": date_filed,
                "section": "filing_content",
                "content": chunk_text
            })
            
        if batch:
            df = pd.DataFrame(batch)
            # HyperStreamDB v0.2.2 .write handles the conversion and autovectorization
            self.table.write(df, mode='append')
            self.table.commit()
            logger.info(f"Ingested {len(batch)} chunks for {accession_number} into HyperStreamDB RAG table.")

    def query(self, text: str, k: int = 5, cik: str = None) -> pd.DataFrame:
        """
        Performs a vector search against the autovectorized table.
        """
        search_query = self.table.query()
        if cik:
            search_query = search_query.filter(f"cik = '{str(cik).zfill(10)}'")
        
        # HyperStreamDB v0.2.2 supports passing text query directly 
        # to the vector_search if define_embedding was used.
        results = search_query.vector_search(text, column="embedding", k=k)
        return results.to_pandas()

import re

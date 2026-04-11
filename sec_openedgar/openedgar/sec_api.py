import os
import time
import httpx
import zipfile
import io
import re
import threading
import orjson
import pandas as pd
import secsgml2
from typing import List, Dict, Any, Tuple, Optional

class AdvancedSECRatelimiter:
    """
    Optimized Token Bucket Ratelimiter for SEC endpoints
    Allows bursts up to capacity and replenishes at fill_rate/sec.
    """
    def __init__(self, capacity: int = 10, fill_rate: float = 9.5):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.fill_rate = fill_rate
        self.last_update = time.monotonic()
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            while True:
                now = time.monotonic()
                delta = now - self.last_update
                self.tokens += delta * self.fill_rate
                if self.tokens > self.capacity:
                    self.tokens = self.capacity
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                # Wait for just enough time to get 1 token
                sleep_time = (1.0 - self.tokens) / self.fill_rate
                time.sleep(sleep_time)


class SecAPI:
    def __init__(self, identity: str = None):
        self.identity = identity or os.getenv("EDGAR_IDENTITY", "User user@example.com")
        self.client = httpx.Client(
            headers={
                "User-Agent": self.identity,
                "Accept-Encoding": "gzip, deflate"
            },
            timeout=30.0,
            http2=True  # SEC supports HTTP/2, helps with connection multiplexing
        )
        self.limiter = AdvancedSECRatelimiter(capacity=10, fill_rate=9.5)
        
        # Pre-compile SGML extraction regexes for high performance header parsing
        self._doc_count_rx = re.compile(rb'PUBLIC-DOCUMENT-COUNT:\s+(\d+)')
        self._accept_date_rx = re.compile(rb'ACCEPTANCE-DATETIME:\s+(\d{14})')

    def _get(self, url: str) -> httpx.Response:
        self.limiter.wait()
        retries = 3
        for i in range(retries):
            resp = self.client.get(url)
            if resp.status_code == 429:
                time.sleep(10 * (i + 1))
                continue
            resp.raise_for_status()
            return resp
        resp.raise_for_status()

    def get_cik_lookup_data(self) -> pd.DataFrame:
        url = "https://www.sec.gov/files/company_tickers.json"
        content = self._get(url).content
        data = orjson.loads(content)  # C-optimized JSON parsing
        
        records = []
        for v in data.values():
            records.append({
                "cik": str(v["cik_str"]).zfill(10),
                "ticker": v["ticker"],
                "name": v["title"]
            })
        return pd.DataFrame(records)

    def get_company_submissions(self, cik: int) -> Dict[str, Any]:
        cik_str = str(cik).zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{cik_str}.json"
        try:
            content = self._get(url).content
            return orjson.loads(content)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {}
            raise

    def get_company_facts_pandas(self, cik: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        cik_str = str(cik).zfill(10)
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json"
        try:
            content = self._get(url).content
            data = orjson.loads(content)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None, None
            raise

        facts_data = data.get('facts', {})
        if not facts_data:
            return None, None

        all_facts = []
        fact_meta = []
        seen_concepts = set()

        for namespace, tax_facts in facts_data.items():
            for concept, concept_data in tax_facts.items():
                label = concept_data.get('label', '')
                description = concept_data.get('description', '')
                
                if concept not in seen_concepts:
                    fact_meta.append({
                        'fact': concept,
                        'label': label,
                        'description': description
                    })
                    seen_concepts.add(concept)

                for unit, un_facts in concept_data.get('units', {}).items():
                    for f in un_facts:
                        all_facts.append({
                            'fact': concept,
                            'namespace': namespace,
                            'val': f.get('val'),
                            'accn': f.get('accn'),
                            'end': f.get('end'),
                            'filed': f.get('filed'),
                            'fy': f.get('fy'),
                            'fp': f.get('fp'),
                            'frame': f.get('frame', ''),
                            'form': f.get('form'),
                        })
        
        facts_df = pd.DataFrame(all_facts)
        meta_df = pd.DataFrame(fact_meta)
        return facts_df, meta_df

    def get_filings_for_quarter(self, year: int, qtr: int) -> pd.DataFrame:
        url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.zip"
        try:
            resp = self._get(url)
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                with z.open("master.idx") as f:
                    content = f.read()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return pd.DataFrame()
            raise

        # Find start of data dynamically using regex across byte stream, optimized
        data_start_idx = content.find(b"\n---")
        if data_start_idx == -1:
            return pd.DataFrame()
        
        # Skip to the newline after "---"
        second_newline = content.find(b"\n", data_start_idx + 1)
        data_chunk = content[second_newline + 1:]

        # High-performance Pandas TSV parsing, avoiding pure python loops
        df = pd.read_csv(
            io.BytesIO(data_chunk),
            sep='|',
            encoding='latin1',
            names=['cik', 'company', 'form', 'filing_date', 'filename'],
            dtype={'cik': int, 'company': str, 'form': str, 'filing_date': str, 'filename': str},
            engine='c'
        )
        
        if df.empty:
            return pd.DataFrame()

        # Vectorized dataframe operations instead of per-row string ops
        df['accession_number'] = df['filename'].str.split('/').str[-1].str.replace('.txt', '')
        df['document_url'] = "https://www.sec.gov/Archives/" + df['filename']
        df['text_url'] = df['document_url']
        df['homepage_url'] = (
            "https://www.sec.gov/Archives/edgar/data/" + 
            df['cik'].astype(str) + "/" + 
            df['accession_number'].str.replace('-', '') + "/" + 
            df['accession_number'] + "-index.htm"
        )
        return df

    def get_filings(self, years: List[int]) -> pd.DataFrame:
        dfs = []
        for year in years:
            for qtr in range(1, 5):
                df = self.get_filings_for_quarter(year, qtr)
                if not df.empty:
                    dfs.append(df)
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def get_filing_sgml_header(self, raw_txt_url: str) -> Tuple[Optional[int], Optional[str], Dict[str, Any]]:
        """
        Uses Cython-optimized secsgml2 for ultra-fast metadata extraction.
        Uses a Range request to fetch the header segment (first ~32KB).
        Returns (doc_count, acceptance_datetime, full_metadata_dict).
        """
        try:
            self.limiter.wait()
            # Fetch the first 32KB
            resp = self.client.get(raw_txt_url, headers={"Range": "bytes=0-32768"})
            header_bytes = resp.content

            # Parse with secsgml2
            metadata, _ = secsgml2.parse_sgml_content_into_memory(header_bytes)
            
            # Map standard keys
            doc_count = metadata.get('public_document_count') or metadata.get('PUBLIC_DOCUMENT_COUNT')
            if doc_count:
                try:
                    doc_count = int(doc_count)
                except (ValueError, TypeError):
                    doc_count = None
            
            accept_dt = metadata.get('acceptance_datetime') or metadata.get('ACCEPTANCE_DATETIME')
            
            return doc_count, accept_dt, metadata
            
        except Exception:
            return None, None, {}

sec_api = SecAPI()

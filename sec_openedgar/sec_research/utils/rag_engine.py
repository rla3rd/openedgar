import os
import pandas as pd
import numpy as np
import hyperstreamdb as hdb
from sentence_transformers import SentenceTransformer

# Constants
HDB_PATH = "/home/ralbright/projects/openedgar/hdb_data"
EMBED_MODEL = "BAAI/bge-large-en-v1.5"

_model = None
_table = None

def get_hdb():
    global _table
    if _table is None:
        _table = hdb.Table(HDB_PATH)
    return _table

def get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBED_MODEL)
    return _model

def query_filings(question, cik=None, accession=None, date_start=None, date_end=None, k=5):
    """
    Unified RAG query function.
    Performs vector search + metadata filtering.
    """
    model = get_model()
    table = get_hdb()
    # Embed question (clean it first)
    clean_question = question.replace('[NULL]', '').strip()
    query_vec = model.encode(clean_question, convert_to_numpy=True).tolist()
    query = table.query()
    
    if cik:
        query = query.filter(f"cik = {cik}")
    if accession:
        query = query.filter(f"accession_number = '{accession}'")
    if date_start:
        query = query.filter(f"date_filed >= DATE '{date_start}'")
    if date_end:
        query = query.filter(f"date_filed <= DATE '{date_end}'")
        
    results = (query
               .vector_search(query_vec, column="embedding", k=k)
               .execute())
    
    if hasattr(results, 'to_pandas'):
        df = results.to_pandas()
    else:
        df = results
        
    return df

def get_context_for_accession(accession):
    """
    Retrieves all sections for a specific accession number, ordered for reconstruction.
    """
    table = get_hdb()
    results = (table.query()
               .filter(f"accession_number = '{accession}'")
               .execute())
    
    if hasattr(results, 'to_pandas'):
        df = results.to_pandas()
    else:
        df = results
    
    if df.empty:
        return ""
        
    # Sort by monotonic chunk_index for correct reconstruction
    df = df.sort_values(['chunk_index'])
    
    # Filter out null text
    texts = df['text'].dropna().astype(str).tolist()
    return "\n\n".join(texts)

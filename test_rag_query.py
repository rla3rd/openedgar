import sys
import os

# Add sec_openedgar to path
sys.path.append(os.path.join(os.getcwd(), 'sec_openedgar'))

# Mock Django settings if needed, but rag_pipeline should work standalone
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.local')

try:
    from openedgar.processes.rag_pipeline import ModernRAGPipeline
    import pandas as pd
    
    from sentence_transformers import SentenceTransformer
    
    def run_test_query(query_text, cik=None):
        print(f"--- Querying RAG: '{query_text}' ---")
        pipeline = ModernRAGPipeline()
        
        try:
            # Fallback for manual embedding generation in the test script
            print("Generating embedding...")
            model = SentenceTransformer("BAAI/bge-m3")
            query_embedding = model.encode(query_text).tolist()
            
            results = pipeline.table.query().vector_search(query_embedding, column="embedding", k=3).to_pandas()
            
            if results.empty:
                print("No results found. (Did you ingest any data yet?)")
            else:
                for i, row in results.iterrows():
                    print(f"\n[Result {i+1}] (CIK: {row['cik']} | Date: {row['date_filed']})")
                    print(f"Content Snippet: {row['content'][:300]}...")
                    print("-" * 40)
        except Exception as e:
            print(f"Query failed: {e}")

    if __name__ == "__main__":
        q = sys.argv[1] if len(sys.argv) > 1 else "What are the risk factors?"
        cik = sys.argv[2] if len(sys.argv) > 2 else None
        run_test_query(q, cik)

except ImportError as e:
    print(f"Import Error: {e}")
except Exception as e:
    print(f"Error: {e}")

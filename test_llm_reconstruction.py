import sys
import os
import json

# Add sec_openedgar to path
sys.path.append(os.path.join(os.getcwd(), 'sec_openedgar'))

# Mock Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.local')

try:
    from openedgar.parsers.registry import registry
    from openedgar.clients.local import LocalClient
    from openedgar.parsers.openedgar import parse_filing
    
    def run_benchmark(accession_number, file_path):
        """
        The System Test:
        1. Extract Ground Truth (Rules-based).
        2. Generate Markdown (Input).
        3. Placeholder for LLM call.
        4. Compare (Manual/Auto).
        """
        print(f"--- System Test: {accession_number} ---")
        client = LocalClient()
        buffer = client.get_buffer(file_path)
        
        # 1. Get Filing Type
        filing_data = parse_filing(buffer)
        form_type = filing_data.get('form_type')
        print(f"Form Type: {form_type}")
        
        parser = registry.get_parser(form_type)
        if not parser:
            print(f"No specialized parser for {form_type}. Proceeding with generic hybrid logic.")
            return

        # 2. Extract Ground Truth
        truth = parser.extract_ground_truth(buffer)
        print("\n[GROUND TRUTH EXTRACTED]")
        print(json.dumps(truth, indent=2))
        
        # 3. Generate Visual Markdown (Input for LLM)
        markdown = parser.to_markdown(buffer)
        print("\n[INPUT MARKDOWN FOR LLM]")
        print("-" * 20)
        print(markdown)
        print("-" * 20)
        
        print("\n[NEXT STEP]")
        print("Pass the Markdown above to your LLM and ask for a CSV reconstruction.")
        print("Then compare the CSV rows against the GROUND TRUTH JSON above.")

    if __name__ == "__main__":
        if len(sys.argv) < 3:
            print("Usage: python test_llm_reconstruction.py <accession_number> <local_file_path>")
        else:
            run_benchmark(sys.argv[1], sys.argv[2])

except ImportError as e:
    print(f"Import Error: {e}")

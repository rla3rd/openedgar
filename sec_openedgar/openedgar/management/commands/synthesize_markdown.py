import os
import pathlib
import pyzstd
import tempfile
import logging
from django.core.management.base import BaseCommand
from openedgar.models import Filing
from openedgar.parsers.openedgar import process_accession_synthesis
from openedgar.processes.rag_pipeline import ModernRAGPipeline

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'On-demand high-fidelity synthesis of SEC filings into Markdown fragments'

    def add_arguments(self, parser):
        parser.add_argument('--year', type=int, help='Year to filter by')
        parser.add_argument('--qtr', type=int, help='Quarter to filter by')
        parser.add_argument('--accession', type=str, help='Specific accession number to process')
        parser.add_argument('--form-type', type=str, help='Filter by form type (e.g. 10-K)')
        parser.add_argument('--engine', type=str, choices=['auto', 'secmd', 'docling'], default='auto',
                            help='Synthesis engine (auto results in secmd for HTML, docling for PDF)')
        parser.add_argument('--chunk-size', type=int, default=500, help='Fragment chunk size in KB')
        parser.add_argument('--force', action='store_true', help='Overwrite existing synthesis fragments')
        parser.add_argument('--push-hdb', action='store_true', default=True, help='Automatically index into HyperStreamDB')

    def handle(self, *args, **options):
        # 1. Gather Filings
        queryset = Filing.objects.all()
        if options['accession']:
            queryset = queryset.filter(accession_number=options['accession'])
        if options['year']:
            queryset = queryset.filter(date_filed__year=options['year'])
        if options['qtr']:
            queryset = queryset.filter(date_filed__quarter=options['qtr'])
        if options['form_type']:
            queryset = queryset.filter(form_type__form=options['form_type'])

        count = queryset.count()
        self.stdout.write(f"Found {count} filings for synthesis.")
        
        rag = ModernRAGPipeline() if options['push_hdb'] else None

        for filing in queryset:
            acc_num = filing.accession_number
            self.stdout.write(f"Processing {acc_num}...")
            
            try:
                # 2. Reconstruct Sandbox
                with tempfile.TemporaryDirectory() as sandbox:
                    sandbox_path = pathlib.Path(sandbox)
                    primary_doc = None
                    
                    index_data = filing.documents_index
                    if not index_data or "documents" not in index_data:
                        self.stderr.write(f"  Missing documents_index for {acc_num}. Skipping.")
                        continue
                        
                    # Find primary doc in index
                    # Heuristic: first document or matching form type
                    for doc_info in index_data["documents"]:
                        fname = doc_info["filename"]
                        chunk_rel_path = doc_info["path"]
                        
                        # Full path to the zstd chunk
                        archive_root = pathlib.Path(filing.path).parent
                        chunk_abs_path = archive_root / chunk_rel_path
                        
                        if not chunk_abs_path.exists():
                            self.stderr.write(f"  Chunk missing: {chunk_abs_path}")
                            continue
                            
                        # Decompress to sandbox
                        with open(chunk_abs_path, "rb") as f_in:
                            decompressed = pyzstd.decompress(f_in.read())
                            with open(sandbox_path / fname, "wb") as f_out:
                                f_out.write(decompressed)
                        
                        # Primary discovery logic
                        if not primary_doc:
                            # If it matches form type or is a standard primary extension
                            if fname.lower().endswith((".htm", ".html", ".pdf", ".txt")):
                                # Basic check for type if available in index
                                # (Assuming docling/secmd can handle it)
                                primary_doc = fname

                    if not primary_doc:
                        self.stderr.write(f"  No primary document identified for {acc_num}. Skipping.")
                        continue

                    # 3. Perform Synthesis
                    fragments = process_accession_synthesis(
                        sandbox_dir=sandbox,
                        primary_filename=primary_doc,
                        engine=options['engine'],
                        chunk_size_kb=options['chunk_size']
                    )
                    
                    if not fragments:
                        self.stdout.write(self.style.WARNING(f"  Synthesis empty for {acc_num}."))
                        continue

                    # 4. Save Sidecars (.out.##.md.zst)
                    output_dir = archive_root
                    for i, frag in enumerate(fragments):
                        frag_num = str(i+1).zfill(2)
                        frag_path = output_dir / f"{acc_num}.out.{frag_num}.md.zst"
                        
                        if frag_path.exists() and not options['force']:
                            self.stdout.write(f"  Fragment {frag_num} exists. Use --force to overwrite.")
                        else:
                            with open(frag_path, "wb") as f_out:
                                f_out.write(pyzstd.compress(frag["content"].encode('utf-8')))
                        
                        # 5. Push to HyperStreamDB (Math-First Promotion)
                        if rag:
                            rag.ingest_filing_chunks(
                                cik=str(filing.cik_id),
                                accession_number=acc_num,
                                form_type=filing.form_type_id,
                                date_filed=str(filing.date_filed),
                                markdown=frag["content"] # Note: offset logic handled inside rag.py
                            )

                self.stdout.write(self.style.SUCCESS(f"  Successfully synthesized {len(fragments)} fragments for {acc_num}."))

            except Exception as e:
                self.stderr.write(self.style.ERROR(f"  Failed processing {acc_num}: {e}"))
                logger.exception(f"Synthesis tool error for {acc_num}")

        self.stdout.write(self.style.SUCCESS("All processing complete."))

import os
import pathlib
from django.core.management.base import BaseCommand
from django.conf import settings

class Command(BaseCommand):
    help = 'Locate and cluster all files associated with an SEC Accession Number'

    def add_arguments(self, parser):
        parser.add_argument('accession', type=str, help='The SEC Accession Number (e.g., 0000004977-21-000005)')
        parser.add_argument('--peek', action='store_true', help='Show a brief preview of the high-fidelity Markdown if found')

    def handle(self, *args, **options):
        accession = options['accession'].strip()
        
        base_dir_str = os.getenv("EDGAR_LOCAL_DATA_DIR", getattr(settings, "EDGAR_LOCAL_DATA_DIR", "/media/data"))
        base_dir = pathlib.Path(base_dir_str)
        
        self.stdout.write(self.style.NOTICE(f"Searching for Accession: {accession} in {base_dir}..."))
        
        # Search for any file matching the accession prefix
        found_files = list(base_dir.rglob(f"{accession}*"))
        
        if not found_files:
            self.stdout.write(self.style.ERROR(f"No files found for accession {accession}."))
            return

        # Group by directory
        dirs = {}
        for f in found_files:
            d = f.parent
            if d not in dirs: dirs[d] = []
            dirs[d].append(f)

        for d, files in dirs.items():
            self.stdout.write(f"\n[DIR] {d}")
            for f in sorted(files):
                size_kb = f.stat().st_size / 1024
                
                # Descriptive labels
                label = "Unknown"
                if f.name.endswith(".sgml.zst"): label = "SEC Header (Raw SGML)"
                elif f.name.endswith(".sgml.md.zst"): label = "Metadata Manifest (Sidecar)"
                elif f.name.endswith(".out.md.zst"): label = "Fidelity Synthesis (Markdown)"
                elif f.name.endswith(".zst"): label = "Filing Data Chunk"
                
                self.stdout.write(f"  - {f.name:45} | {size_kb:8.2f} KB | {label}")

        if options['peek']:
            # Find the synthesis file
            synth = next((f for f in found_files if ".md.zst" in f.name and ".sgml." not in f.name), None)
            if synth:
                self.stdout.write(self.style.NOTICE("\n--- Quick Peek (Synthesis) ---"))
                import pyzstd
                try:
                    with open(synth, "rb") as f:
                        content = pyzstd.decompress(f.read()).decode('utf-8', errors='ignore')
                        # Show first 20 lines
                        lines = content.splitlines()[:20]
                        for line in lines:
                            self.stdout.write(f"  {line}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Could not peek: {e}"))

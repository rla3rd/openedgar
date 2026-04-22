import os
import pathlib
import pyzstd
from django.core.management.base import BaseCommand
from openedgar.models import Filing
from openedgar.parsers.ownership import OwnershipParser
from tqdm import tqdm

class Command(BaseCommand):
    help = 'Regenerate high-fidelity markdown sidecars for specific accessions.'

    def add_arguments(self, parser):
        parser.add_argument('acc_file', type=str, help='Path to accession numbers list.')
        parser.add_argument('--limit', type=int, default=500)

    def handle(self, *args, **options):
        base_dir = pathlib.Path("/home/ralbright/data/openedgar/edgar/data")
        parser = OwnershipParser()
        
        with open(options['acc_file'], 'r') as f:
            accs = [l.strip() for l in f if l.strip()]
            
        accs = accs[:options['limit']]
        self.stdout.write(f"Regenerating markdown for {len(accs)} filings...")
        
        success = 0
        missing = 0
        failed = 0
        
        for acc in tqdm(accs):
            try:
                filing = Filing.objects.filter(accession_number=acc).first()
                if not filing:
                    missing += 1
                    continue
                    
                archive_root = pathlib.Path("/home/ralbright/data/openedgar/edgar") / pathlib.Path(filing.path).parent
                
                # Find the chunk containing the XML
                xml_chunk_path = None
                raw_xml = None
                
                # Try .401.zst first as it is the most common XML chunk for ownership
                primary_chunk = archive_root / f"{acc}.401.zst"
                if primary_chunk.exists():
                    with open(primary_chunk, "rb") as f_in:
                        raw_xml = pyzstd.decompress(f_in.read()).decode('utf-8', errors='ignore')
                        xml_chunk_path = primary_chunk
                
                if not xml_chunk_path:
                    for p in archive_root.glob(f"{acc}.*.zst"):
                        if ".out.md.zst" in str(p) or ".sgml.md.zst" in str(p) or ".sgml.zst" in str(p): continue
                        with open(p, "rb") as f_in:
                            raw = pyzstd.decompress(f_in.read()).decode('utf-8', errors='ignore')
                            if "<ownershipDocument" in raw or "<XML" in raw:
                                xml_chunk_path = p
                                raw_xml = raw
                                break
                
                if not xml_chunk_path:
                    # Final fallback to .sgml.zst
                    sgml_path = archive_root / f"{acc}.sgml.zst"
                    if sgml_path.exists():
                        with open(sgml_path, "rb") as f_in:
                            raw_xml = pyzstd.decompress(f_in.read()).decode('utf-8', errors='ignore')
                    else:
                        missing += 1
                        continue
                    
                md_content = parser.synthesize(raw_xml, accession=acc)
                
                if md_content.startswith("Error") or "No XML block found" in md_content:
                    failed += 1
                    continue
                    
                out_path = archive_root / f"{acc}.out.md.zst"
                with open(out_path, "wb") as f_out:
                    f_out.write(pyzstd.compress(md_content.encode('utf-8')))
                
                success += 1
                
            except Exception as e:
                self.stderr.write(f"Error on {acc}: {e}")
                failed += 1
                
        self.stdout.write(self.style.SUCCESS(f"Complete: {success} success, {missing} missing, {failed} failed."))

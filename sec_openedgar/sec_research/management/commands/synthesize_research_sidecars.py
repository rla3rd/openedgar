import os
import pathlib
import pyzstd
from django.core.management.base import BaseCommand
from django.conf import settings
from openedgar.parsers.ownership_parser import OwnershipParser as OwnershipMarkdownSynthesizer
from tqdm import tqdm
import re

class Command(BaseCommand):
    help = 'On-demand synthesis of high-fidelity Markdown from SEC Ownership Forms'

    def add_arguments(self, parser):
        parser.add_argument('--year', type=int, help='Year to process (from DB)')
        parser.add_argument('--qtr', type=int, help='Quarter to process (from DB)')
        parser.add_argument('--cik', type=int, help='Issuer CIK to process')
        parser.add_argument('--rptcik', type=int, help='Reporting Owner CIK to process')
        parser.add_argument('--forms', nargs='+', default=['3', '4', '5', '3/A', '4/A', '5/A'], help='Form types to process')
        parser.add_argument('--accession', nargs='+', help='Specific Accession Number(s)')
        parser.add_argument('--holdout', type=str, help='Path to file containing accession numbers')
        parser.add_argument('--force', action='store_true', help='Re-process even if already marked as processed in DB')
        parser.add_argument('--replace', action='store_true', help='Overwrite existing sidecars on disk')
        parser.add_argument('--limit', type=int, help='Limit the number of filings to process')
        parser.add_argument('--random', action='store_true', help='Select filings randomly (use with --limit)')
        parser.add_argument('--seed', type=int, default=3836, help='Seed for random selection')

    def handle(self, *args, **options):
        from openedgar.models import Filing
        synthesizer = OwnershipMarkdownSynthesizer()
        
        base_dir_str = os.getenv("EDGAR_LOCAL_DATA_DIR", getattr(settings, "EDGAR_LOCAL_DATA_DIR", "/media/data"))
        base_dir = pathlib.Path(base_dir_str)
        
        queryset = Filing.objects.all()
        if options['holdout']:
            with open(options['holdout'], 'r') as f:
                target_accs = [l.strip() for l in f if l.strip()]
            queryset = queryset.filter(accession_number__in=target_accs)
        elif options['accession']:
            queryset = queryset.filter(accession_number__in=options['accession'])
        else:
            if options.get('year'):
                queryset = queryset.filter(date_filed__year=options['year'])
            if options.get('qtr'):
                queryset = queryset.filter(path__contains=f"/QTR{options['qtr']}/")
            
            queryset = queryset.filter(form_type_id__in=options['forms'])
            
            if options['cik']:
                from django.db.models import Q
                queryset = queryset.filter(
                    Q(cik_id=options['cik']) | 
                    Q(ownershipsubmission__issuer_cik_id=options['cik'])
                )
            if options['rptcik']:
                queryset = queryset.filter(ownershipsubmission__reporting_owners__rptowner_cik=options['rptcik'])
            
            if not options['force']:
                queryset = queryset.filter(is_processed=False)

        if options['random'] and options['limit']:
            import random
            pks = list(queryset.values_list('pk', flat=True))
            if len(pks) > options['limit']:
                random.seed(options['seed'])
                pks = random.sample(pks, options['limit'])
            queryset = Filing.objects.filter(pk__in=pks)
        elif options['limit']:
            queryset = queryset[:options['limit']]

        count = queryset.count()
        self.stdout.write(f"Processing {count} filings via Research Synthesizer...")

        for filing in tqdm(queryset):
            path = base_dir / filing.path
            xml_content, xml_filename = synthesizer.find_ownership_xml(path)
            if not xml_content: continue
                
            markdown = synthesizer.synthesize(xml_content, accession=filing.accession_number)
            
            if options['replace'] or not (path.parent / f"{filing.accession_number}.out.md.zst").exists():
                out_path = path.parent / f"{filing.accession_number}.out.md.zst"
                with open(out_path, "wb") as f:
                    f.write(pyzstd.compress(markdown.encode('utf-8')))
            
            filing.is_processed = True
            filing.save()

        self.stdout.write(self.style.SUCCESS(f"Research Synthesis Complete! Generated {count} sidecars."))

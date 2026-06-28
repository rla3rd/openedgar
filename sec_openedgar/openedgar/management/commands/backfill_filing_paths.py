import os
from django.core.management.base import BaseCommand
from django.db import transaction
from django.conf import settings
from openedgar.models import Filing

class Command(BaseCommand):
    help = 'Populates year, qtr, month, day, and markdown_path for existing Filing records if sidecar exists.'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, help='Limit the number of records to process', default=None)
        parser.add_argument('--accession', type=str, help='Process a specific accession number', default=None)
        parser.add_argument('--file', type=str, help='Path to a file containing accession numbers (one per line)', default=None)
        parser.add_argument('--all', action='store_true', help='Check all filings, not just is_processed=True', default=False)
        parser.add_argument('--verbose', action='store_true', help='Print skipped files', default=False)

    def handle(self, *args, **options):
        limit = options['limit']
        accession = options['accession']
        acc_file = options['file']
        check_all = options['all']
        verbose = options['verbose']

        data_root = getattr(settings, 'EDGAR_LOCAL_DATA_DIR', '/home/ralbright/data/openedgar/edgar')

        filings = Filing.objects.all()
        
        if acc_file:
            with open(acc_file, 'r') as f:
                accs = [line.strip() for line in f if line.strip()]
            filings = filings.filter(accession_number__in=accs)
        elif not check_all:
            filings = filings.filter(is_processed=True)
            
        if accession:
            filings = filings.filter(accession_number=accession)
        
        # Order by date_filed descending to get recent ones first
        filings = filings.order_by('-date_filed')

        if limit:
            filings = filings[:limit]

        count = filings.count()
        self.stdout.write(f"Starting backfill for {count} filings (data_root={data_root})...")

        batch_size = 1000
        processed = 0
        linked = 0

        for i in range(0, count, batch_size):
            batch = filings[i:i+batch_size]
            with transaction.atomic():
                for filing in batch:
                    if not filing.date_filed:
                        continue
                    
                    d = filing.date_filed
                    year = d.year
                    qtr = (d.month - 1) // 3 + 1
                    month = d.month
                    day = d.day
                    
                    acc = filing.accession_number
                    rel_path = os.path.join(
                        "data",
                        str(year),
                        f"QTR{qtr}",
                        f"{month:02d}",
                        f"{day:02d}",
                        f"{acc}.out.md.zst"
                    )
                    
                    full_path = os.path.join(data_root, rel_path)
                    
                    if os.path.exists(full_path):
                        filing.year = year
                        filing.qtr = qtr
                        filing.month = month
                        filing.day = day
                        filing.markdown_path = rel_path
                        filing.save()
                        linked += 1
                    elif verbose:
                        self.stdout.write(f"Missing: {full_path}")
            
            processed += len(batch)
            self.stdout.write(f"Processed {processed}/{count}... (Linked: {linked})")

        self.stdout.write(self.style.SUCCESS(f'Backfill complete! Linked {linked} of {processed} filings.'))

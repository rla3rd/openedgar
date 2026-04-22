import os
import pathlib
import datetime
from django.core.management.base import BaseCommand
from openedgar.tasks import process_bulk_filings

class Command(BaseCommand):
    help = 'Processes locally cached SEC daily archives into individual filings and high-fidelity sidecars.'

    def add_arguments(self, parser):
        parser.add_argument('--year', type=int, help='Year to process', default=None)
        parser.add_argument('--qtr', type=int, help='Quarter to process', default=None)
        parser.add_argument('--days', type=int, nargs='+', help='Specific days to process (YYYYMMDD)', default=None)
        parser.add_argument('--forms', type=str, nargs='+', help='Filter by form types', default=None)
        parser.add_argument('--replace', action='store_true', help='Replace existing filings', default=False)
        parser.add_argument('--verbose', action='store_true', help='Verbose output', default=False)

    def handle(self, *args, **options):
        year = options['year']
        qtr = options['qtr']
        days = options['days']
        forms = options['forms']
        replace = options['replace']
        verbose = options['verbose']

        if verbose:
            self.stdout.write(f"Starting local bulk process: year={year}, qtr={qtr}, days={days}, forms={forms}, replace={replace}")

        process_bulk_filings(
            year=year,
            qtr=qtr,
            days=days,
            forms=forms,
            replace=replace,
            verbose=verbose
        )
        
        self.stdout.write(self.style.SUCCESS('Processing complete!'))

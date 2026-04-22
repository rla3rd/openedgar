from django.core.management.base import BaseCommand
from openedgar.tasks import download_bulk_filings

class Command(BaseCommand):
    help = 'Download bulk filings from SEC EDGAR'

    def add_arguments(self, parser):
        parser.add_argument('--year', type=int, help='Year to download', default=None)
        parser.add_argument('--qtr', type=int, help='Quarter to download (1-4)', default=None)
        parser.add_argument(
            '--days',
            nargs='+',
            help='Specific days to download (format YYYYMMDD)',
            default=None,
        )
        parser.add_argument(
            '--no-backfill',
            action='store_true',
            help='Do not backfill from 1997',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Verbose output',
        )
        parser.add_argument(
            '--replace',
            action='store_true',
            help='Replace existing downloaded files on disk',
        )
        parser.add_argument(
            '--download-only',
            action='store_true',
            help='Download files to disk but do not extract yet',
        )
        parser.add_argument(
            '--forms',
            nargs='+',
            default=None,
            help='Limit processing to specific form types (e.g. 3 4 5 10-K)'
        )

    def handle(self, *args, **options):
        year = options['year']
        qtr = options['qtr']
        days = options['days']
        backfill = not options['no_backfill']
        verbose = options['verbose']
        replace = options['replace']
        forms = options['forms']
        download_only = options['download_only']

        self.stdout.write(self.style.SUCCESS(
            f'Starting bulk download: year={year}, qtr={qtr}, days={days}, forms={forms}, download_only={download_only}, replace={replace}'
        ))
        
        # Call the existing function from tasks.py
        download_bulk_filings(
            year=year,
            qtr=qtr,
            backfill=backfill,
            verbose=verbose,
            replace=replace,
            days=days,
            forms=forms,
            download_only=download_only
        )
        
        self.stdout.write(self.style.SUCCESS('Download complete!'))

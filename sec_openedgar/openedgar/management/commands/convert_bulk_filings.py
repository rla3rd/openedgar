import os
import pathlib
import multiprocessing
from django.core.management.base import BaseCommand
from django.conf import settings
from openedgar.tasks import extract_and_compress_tar_feed

def django_setup():
    import django
    django.setup()

class Command(BaseCommand):
    help = 'Converts unextracted SEC feed tar.gz files into zstd chunks'

    def add_arguments(self, parser):
        parser.add_argument('--year', type=int, help='Limit to a specific year', default=None)
        parser.add_argument('--qtr', type=int, help='Limit to a specific quarter', default=None)
        parser.add_argument(
            '--keep',
            action='store_true',
            help='Do not delete tar.gz files after successfully extracting and compressing',
        )
        parser.add_argument(
            '--forms',
            nargs='+',
            default=None,
            help='Limit processing to specific form types (e.g. 3 4 5 10-K)'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=4,
            help='Number of parallel workers'
        )

    def handle(self, *args, **options):
        # Set start method to 'spawn' to avoid CUDA multiprocessing issues
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass

        # We look in the EDGAR_LOCAL_DATA_DIR 
        base_dir = os.getenv("EDGAR_LOCAL_DATA_DIR", getattr(settings, 'EDGAR_LOCAL_DATA_DIR', None))
        if not base_dir:
            self.stderr.write(self.style.ERROR("EDGAR_LOCAL_DATA_DIR not provided via environment or settings."))
            return

        base_path = pathlib.Path(base_dir) / "data"
        if not base_path.exists():
            self.stderr.write(self.style.ERROR(f"Data path {base_path} does not exist."))
            return

        year = options['year']
        qtr = options['qtr']
        remove_after = not options['keep']
        forms = options['forms']
        workers = options['workers']

        tarballs_queued = 0
        from concurrent.futures import ProcessPoolExecutor
        
        with ProcessPoolExecutor(max_workers=workers, initializer=django_setup) as executor:
            import functools
            # Use partial to pass the forms argument to the worker
            worker_func = functools.partial(extract_and_compress_tar_feed, forms=forms, remove_after=remove_after, replace=True)
            
            for year_dir in base_path.iterdir():
                if not year_dir.is_dir():
                    continue
                try:
                    dir_year = int(year_dir.name)
                except ValueError:
                    continue
    
                if year and dir_year != year:
                    continue
    
                for qtr_dir in year_dir.iterdir():
                    if not qtr_dir.is_dir():
                        continue
                    if qtr and qtr_dir.name != f"QTR{qtr}":
                        continue
                    
                    self.stdout.write(f"Scanning directory: {qtr_dir} ...")
                    for tar_path in qtr_dir.glob("*.tar.gz"):
                        self.stdout.write(f"Processing {tar_path}")
                        executor.submit(worker_func, str(tar_path))
                        tarballs_queued += 1

        if tarballs_queued > 0:
            self.stdout.write(self.style.SUCCESS(f'Processed {tarballs_queued} tarballs for extraction!'))
        else:
            self.stdout.write(self.style.WARNING('Found 0 tarballs to process.'))

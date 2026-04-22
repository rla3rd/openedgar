import os
import pathlib
import concurrent.futures
import multiprocessing
from django.core.management.base import BaseCommand
from django.conf import settings
from openedgar.tasks import convert_legacy_zstd_worker
import functools

def django_setup():
    import django
    django.setup()

class Command(BaseCommand):
    help = 'Processes legacy .zstd files to strip SGML headers and encode to clean .zst chunks'

    def add_arguments(self, parser):
        parser.add_argument(
            '--workers',
            type=int,
            default=os.cpu_count() or 4,
            help='Number of parallel workers'
        )
        parser.add_argument(
            '--file',
            type=str,
            default=None,
            help='Path to a specific .zstd file to process'
        )
        parser.add_argument(
            '--year',
            type=int,
            default=None,
            help='Limit processing to a specific year'
        )
        parser.add_argument(
            '--qtr',
            type=int,
            default=None,
            help='Limit processing to a specific quarter (1-4)'
        )
        parser.add_argument(
            '--forms',
            nargs='+',
            default=None,
            help='Limit processing to specific form types (e.g. 3 4 5 10-K)'
        )

    def handle(self, *args, **options):
        # Set start method to 'spawn' to avoid CUDA multiprocessing issues
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass

        workers = options['workers']
        target_file = options['file']
        target_year = options['year']
        target_qtr = options['qtr']
        forms = options['forms']
        
        data_dir = os.getenv("EDGAR_LOCAL_DATA_DIR", getattr(settings, "EDGAR_LOCAL_DATA_DIR", "/media/data"))
        base_path = pathlib.Path(data_dir) / "data"
        
        if not base_path.exists():
            self.stdout.write(self.style.ERROR(f"Data directory not found at {base_path}"))
            return

        # Gather all .zstd files existing recursively across all quarters
        zstd_files = []

        if target_file:
            path = pathlib.Path(target_file)
            if path.exists() and path.suffix == ".zstd":
                zstd_files.append(str(path))
            else:
                self.stdout.write(self.style.ERROR(f"Specified file not found or invalid: {target_file}"))
                return
        else:
            self.stdout.write(self.style.SUCCESS(f"Scanning directory: {base_path} for legacy .zstd files..."))
            for year_dir in base_path.iterdir():
                if year_dir.is_dir() and year_dir.name.isdigit():
                    if target_year and int(year_dir.name) != target_year:
                        continue
                    for qtr_dir in year_dir.iterdir():
                        if qtr_dir.is_dir() and qtr_dir.name.startswith("QTR"):
                            if target_qtr and qtr_dir.name != f"QTR{target_qtr}":
                                continue
                            for f in qtr_dir.glob("*.zstd"):
                                zstd_files.append(str(f))

        if not zstd_files:
            self.stdout.write(self.style.WARNING("No .zstd files found to process matching the criteria."))
            return
            
        self.stdout.write(self.style.SUCCESS(f"Found {len(zstd_files)} .zstd files for extraction."))

        success_count = 0
        failure_count = 0
        
        # If processing a single file, run it directly to avoid pool overhead
        if len(zstd_files) == 1:
            zstd_path = zstd_files[0]
            if convert_legacy_zstd_worker(zstd_path, forms=forms):
                success_count = 1
            else:
                self.stdout.write(self.style.ERROR(f"Failed to process {zstd_path}"))
                failure_count = 1
        else:
            self.stdout.write(self.style.SUCCESS(f"Creating pool with {workers} workers..."))
            with concurrent.futures.ProcessPoolExecutor(max_workers=workers, initializer=django_setup) as executor:
                # Use partial to pass the forms argument to the worker
                worker_func = functools.partial(convert_legacy_zstd_worker, forms=forms)
                # Map the conversion worker across all found zstd files natively
                results = executor.map(worker_func, zstd_files)
                
                for zstd_path, result in zip(zstd_files, results):
                    if result:
                        success_count += 1
                    else:
                        self.stdout.write(self.style.ERROR(f"Failed to process {zstd_path}"))
                        failure_count += 1

        self.stdout.write(self.style.SUCCESS(
            f"Extraction Complete! Successfully processed and cleaned {success_count} files. ({failure_count} failures)"
        ))

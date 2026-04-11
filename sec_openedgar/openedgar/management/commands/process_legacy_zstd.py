import os
import pathlib
import concurrent.futures
import multiprocessing
from django.core.management.base import BaseCommand
from django.conf import settings
from openedgar.tasks import convert_legacy_zstd_worker

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

    def handle(self, *args, **options):
        # Set start method to 'spawn' to avoid CUDA multiprocessing issues
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass

        workers = options['workers']
        
        data_dir = os.getenv("EDGAR_LOCAL_DATA_DIR", getattr(settings, "EDGAR_LOCAL_DATA_DIR", "/media/data"))
        base_path = pathlib.Path(data_dir) / "data"
        
        if not base_path.exists():
            self.stdout.write(self.style.ERROR(f"Data directory not found at {base_path}"))
            return

        self.stdout.write(self.style.SUCCESS(f"Scanning directory: {base_path} for legacy .zstd files..."))
        
        # Gather all .zstd files existing recursively across all quarters
        zstd_files = []
        for year_dir in base_path.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                for qtr_dir in year_dir.iterdir():
                    if qtr_dir.is_dir() and qtr_dir.name.startswith("QTR"):
                        for f in qtr_dir.glob("*.zstd"):
                            zstd_files.append(str(f))

        if not zstd_files:
            self.stdout.write(self.style.WARNING("No .zstd files found to process."))
            return
            
        self.stdout.write(self.style.SUCCESS(f"Found {len(zstd_files)} .zstd files for extraction. Creating pool with {workers} workers..."))

        success_count = 0
        failure_count = 0
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers, initializer=django_setup) as executor:
            # Map the conversion worker across all found zstd files natively
            results = executor.map(convert_legacy_zstd_worker, zstd_files)
            
            for zstd_path, result in zip(zstd_files, results):
                if result:
                    success_count += 1
                else:
                    self.stdout.write(self.style.ERROR(f"Failed to process {zstd_path}"))
                    failure_count += 1

        self.stdout.write(self.style.SUCCESS(
            f"Extraction Complete! Successfully processed and cleaned {success_count} files. ({failure_count} failures)"
        ))

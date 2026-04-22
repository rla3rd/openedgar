import os
import pathlib
from django.core.management.base import BaseCommand
from django.conf import settings
from openedgar.tasks import migrate_archives_to_zstd_worker

class Command(BaseCommand):
    help = 'Migrates legacy .tar.gz archives to modernized .tar.zst format'

    def add_arguments(self, parser):
        parser.add_argument(
            '--threads',
            type=int,
            default=0,
            help='Number of threads for Zstd compression (0 for all cores)'
        )
        parser.add_argument(
            '--keep-original',
            action='store_false',
            dest='replace',
            default=True,
            help='Keep the original .tar.gz files after successful conversion'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Verbose output'
        )

    def handle(self, *args, **options):
        threads = options['threads']
        replace = options['replace']
        verbose = options['verbose']
        
        data_dir_str = os.getenv("EDGAR_LOCAL_DATA_DIR", getattr(settings, "EDGAR_LOCAL_DATA_DIR", "/media/data"))
        if not data_dir_str:
            self.stdout.write(self.style.ERROR("EDGAR_LOCAL_DATA_DIR environment variable not set."))
            return
            
        data_dir = pathlib.Path(data_dir_str)
        if not data_dir.exists():
            self.stdout.write(self.style.ERROR(f"Data directory {data_dir} does not exist."))
            return

        self.stdout.write(self.style.SUCCESS(f"Scanning {data_dir} for .tar.gz archives..."))
        archives = sorted(list(data_dir.rglob("*.tar.gz")))
        
        if not archives:
            self.stdout.write(self.style.WARNING("No .tar.gz archives found."))
            return

        self.stdout.write(self.style.SUCCESS(f"Found {len(archives)} legacy archives to convert."))

        self.stdout.write(self.style.SUCCESS("Starting sequential migration optimized for HDD..."))
        
        success_count = 0
        failure_count = 0
        
        from tqdm import tqdm
        for archive in tqdm(archives, desc="Migrating"):
            try:
                # One copy of the logic in tasks.py, called from here
                if migrate_archives_to_zstd_worker(archive, verbose=verbose, replace_original=replace, threads=threads):
                    success_count += 1
                else:
                    self.stdout.write(self.style.ERROR(f"Failed to migrate: {archive}"))
                    failure_count += 1
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error migrating {archive}: {e}"))
                failure_count += 1

        self.stdout.write(self.style.SUCCESS(
            f"Migration Complete! Successfully converted {success_count} files. ({failure_count} failures)"
        ))

import random
from django.core.management.base import BaseCommand
from openedgar.models import OwnershipSubmission

class Command(BaseCommand):
    help = 'Exports a randomized list of 1,500 training accessions, excluding the holdout set.'

    def add_arguments(self, parser):
        parser.add_argument('--holdout', type=str, required=True, help='Path to holdout accessions text file')
        parser.add_argument('--out', type=str, required=True, help='Path to output training accessions text file')
        parser.add_argument('--limit', type=int, default=1500)
        parser.add_argument('--seed', type=int, default=3836)

    def handle(self, *args, **options):
        # Load holdout set
        with open(options['holdout'], 'r') as f:
            holdout_accs = set(line.strip() for line in f if line.strip())

        self.stdout.write(f"Loaded {len(holdout_accs)} holdout accessions.")

        # Get all candidates
        all_accs = list(OwnershipSubmission.objects.values_list('accession_number_id', flat=True))
        candidates = [acc for acc in all_accs if acc not in holdout_accs]

        self.stdout.write(f"Found {len(candidates)} potential training candidates.")

        # Shuffle and select
        random.seed(options['seed'])
        random.shuffle(candidates)
        train_set = candidates[:options['limit']]

        # Write to file
        with open(options['out'], 'w') as f:
            for acc in train_set:
                f.write(f"{acc}\n")

        self.stdout.write(self.style.SUCCESS(f"Successfully exported {len(train_set)} training accessions to {options['out']}"))

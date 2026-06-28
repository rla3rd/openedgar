import pathlib
import tempfile
from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Run a quick peek evaluation on a small subset of a holdout accession list."

    def add_arguments(self, parser):
        parser.add_argument("--holdout", type=str, required=True, help="Path to accession numbers list.")
        parser.add_argument("--peek-limit", type=int, default=25, help="Number of holdout accessions to evaluate.")
        parser.add_argument("--model", type=str, default="qwen2.5-35b-instruct")
        parser.add_argument("--url", type=str, default="http://localhost:1234/v1/chat/completions")
        parser.add_argument("--cache-dir", type=str, default="scratch/hp_cache_peek")
        parser.add_argument("--summary-out", type=str, default="scratch/baseline_summary_peek.json")

    def handle(self, *args, **options):
        holdout_path = pathlib.Path(options["holdout"])
        if not holdout_path.exists():
            self.stdout.write(self.style.ERROR(f"Holdout file not found: {holdout_path}"))
            return

        with open(holdout_path, "r", encoding="utf-8") as f:
            accessions = [line.strip() for line in f if line.strip()]

        peek_limit = max(1, options["peek_limit"])
        subset = accessions[:peek_limit]

        if not subset:
            self.stdout.write(self.style.ERROR("No accession numbers found in holdout file."))
            return

        self.stdout.write(
            self.style.WARNING(
                f"Running peek evaluation for {len(subset)} accession(s) from {holdout_path}"
            )
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as tf:
            subset_path = pathlib.Path(tf.name)
            tf.write("\n".join(subset) + "\n")

        self.stdout.write(f"Subset list written to {subset_path}")

        call_command(
            "evaluate_ownership_llm",
            holdout=str(subset_path),
            model=options["model"],
            url=options["url"],
            cache_dir=options["cache_dir"],
            summary_out=options["summary_out"],
        )

        self.stdout.write(self.style.SUCCESS(f"Peek summary saved to {options['summary_out']}"))

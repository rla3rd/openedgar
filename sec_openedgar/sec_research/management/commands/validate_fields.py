from django.core.management.base import BaseCommand
from openedgar.models import OwnershipSubmission
import json


class Command(BaseCommand):
    help = "Validate all fields are populated in ground truth database"

    def add_arguments(self, parser):
        parser.add_argument("accession", type=str, help="Accession number to validate")
        parser.add_argument("--output", type=str, help="JSON output file for detailed results")

    def handle(self, *args, **options):
        accession = options["accession"]
        
        try:
            submission = OwnershipSubmission.objects.get(accession_number_id=accession)
        except OwnershipSubmission.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Filing not found: {accession}"))
            return
        
        self.stdout.write(f"\n{'='*80}")
        self.stdout.write(self.style.SUCCESS(f"ACCESSION: {accession}"))
        self.stdout.write(f"ISSUER: {submission.issuer_name}")
        self.stdout.write(f"{'='*80}\n")
        
        checks = {}
        
        # ISSUER FIELDS
        self.stdout.write(self.style.HTTP_INFO("[ISSUER FIELDS]"))
        
        fields_to_check = [
            ("issuer_name", submission.issuer_name, "Issuer Name"),
            ("issuer_trading_symbol", submission.issuer_trading_symbol, "Trading Symbol"),
            ("issuer_foreign_trading_symbol", submission.issuer_foreign_trading_symbol, "Foreign Trading Symbol"),
            ("schema_version", submission.schema_version, "Schema Version"),
            ("form_type", submission.form_type, "Form Type"),
            ("not_subject_to_section_16", submission.not_subject_to_section_16, "Not Subject to Section 16"),
            ("is_rule_10b5_1_plan", submission.is_rule_10b5_1_plan, "Is Rule 10b5-1 Plan"),
            ("no_securities_owned", submission.no_securities_owned, "No Securities Owned"),
        ]
        
        for key, value, label in fields_to_check:
            if value:
                checks[f"issuer_{key}"] = "✓"
                self.stdout.write(f"  ✓ {label}: {str(value)[:60]}")
            else:
                checks[f"issuer_{key}"] = "✗"
        
        # REPORTING OWNERS
        self.stdout.write(self.style.HTTP_INFO("\n[REPORTING OWNERS]"))
        owner_count = submission.reporting_owners.count()
        self.stdout.write(f"  Total owners: {owner_count}")
        
        for idx, owner in enumerate(submission.reporting_owners.all()[:2]):  # First 2
            self.stdout.write(f"\n  Owner {idx+1}: {owner.rptowner_name}")
            
            owner_fields = [
                ("rptowner_cik", owner.rptowner_cik, "CIK"),
                ("rptowner_ccc", owner.rptowner_ccc, "CCC"),
                ("is_director", owner.is_director, "Director"),
                ("is_officer", owner.is_officer, "Officer"),
                ("is_10pctowner", owner.is_10pctowner, "10% Owner"),
                ("rptowner_country", owner.rptowner_country, "Country"),
                ("rptowner_street1", owner.rptowner_street1, "Street 1"),
                ("rptowner_city", owner.rptowner_city, "City"),
                ("rptowner_state", owner.rptowner_state, "State"),
                ("rptowner_good_address", owner.rptowner_good_address, "Good Address"),
            ]
            
            for key, value, label in owner_fields:
                if value:
                    checks[f"owner_{idx}_{key}"] = "✓"
                    self.stdout.write(f"    ✓ {label}: {str(value)[:50]}")
        
        # NON-DERIVATIVE TRANSACTIONS
        self.stdout.write(self.style.HTTP_INFO("\n[NON-DERIVATIVE TRANSACTIONS]"))
        nonderiv_count = submission.non_deriv_transactions.count()
        self.stdout.write(f"  Total transactions: {nonderiv_count}")
        
        for idx, txn in enumerate(submission.non_deriv_transactions.all()[:2]):  # First 2
            self.stdout.write(f"\n  Transaction {idx+1}: {txn.security_title}")
            
            txn_fields = [
                ("security_title", txn.security_title, "Security"),
                ("transaction_code", txn.transaction_code, "Code"),
                ("transaction_date", txn.transaction_date, "Date"),
                ("transaction_shares", txn.transaction_shares, "Shares"),
                ("transaction_price_per_share", txn.transaction_price_per_share, "Price"),
                ("transaction_total_value", txn.transaction_total_value, "Total Value"),
                ("shares_owned_following_transaction", txn.shares_owned_following_transaction, "Shares After"),
                ("value_owned_following_transaction", txn.value_owned_following_transaction, "Value After"),
                ("nature_of_ownership", txn.nature_of_ownership, "Nature of Ownership"),
                ("direct_or_indirect_ownership", txn.direct_or_indirect_ownership, "Direct/Indirect"),
            ]
            
            for key, value, label in txn_fields:
                if value is not None and value != '':
                    checks[f"nonderiv_{idx}_{key}"] = "✓"
                    self.stdout.write(f"    ✓ {label}: {str(value)[:50]}")
        
        # DERIVATIVE TRANSACTIONS
        self.stdout.write(self.style.HTTP_INFO("\n[DERIVATIVE TRANSACTIONS]"))
        deriv_count = submission.deriv_transactions.count()
        self.stdout.write(f"  Total derivatives: {deriv_count}")
        
        for idx, deriv in enumerate(submission.deriv_transactions.all()[:2]):  # First 2
            self.stdout.write(f"\n  Derivative {idx+1}: {deriv.security_title}")
            
            deriv_fields = [
                ("security_title", deriv.security_title, "Security"),
                ("underlying_security_title", deriv.underlying_security_title, "Underlying"),
                ("conversion_or_exercise_price", deriv.conversion_or_exercise_price, "Conversion Price"),
                ("exercise_date", deriv.exercise_date, "Exercise Date"),
                ("expiration_date", deriv.expiration_date, "Expiration Date"),
                ("underlying_security_shares", deriv.underlying_security_shares, "Underlying Shares"),
                ("underlying_security_value", deriv.underlying_security_value, "Underlying Value"),
                ("shares_owned_following_transaction", deriv.shares_owned_following_transaction, "Shares After"),
                ("value_owned_following_transaction", deriv.value_owned_following_transaction, "Value After"),
            ]
            
            for key, value, label in deriv_fields:
                if value is not None and value != '':
                    checks[f"deriv_{idx}_{key}"] = "✓"
                    self.stdout.write(f"    ✓ {label}: {str(value)[:50]}")
        
        # SUMMARY
        self.stdout.write("\n" + "="*80)
        passes = sum(1 for v in checks.values() if v == "✓")
        total = len(checks)
        pct = 100 * passes / total if total > 0 else 0
        
        self.stdout.write(self.style.SUCCESS(f"✓ Database Fields: {passes}/{total} populated ({pct:.1f}%)"))
        self.stdout.write("="*80 + "\n")
        
        # Save output
        if options["output"]:
            with open(options["output"], 'w') as f:
                json.dump({
                    "accession": accession,
                    "issuer": submission.issuer_name,
                    "counts": {
                        "reporting_owners": owner_count,
                        "non_deriv_transactions": nonderiv_count,
                        "derivative_transactions": deriv_count,
                    },
                    "checks": checks,
                    "summary": {
                        "populated": passes,
                        "total": total,
                        "percentage": pct
                    }
                }, f, indent=2)
            self.stdout.write(f"✓ Results saved to {options['output']}")

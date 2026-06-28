from django.core.management.base import BaseCommand
from openedgar.models import OwnershipSubmission
from openedgar.parsers.ownership_parser import OwnershipParser as OwnershipMarkdownSynthesizer
import json


class Command(BaseCommand):
    help = "Validate markdown output against ground truth for a sample filing"

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
        
        # Generate markdown
        synthesizer = OwnershipMarkdownSynthesizer()
        markdown = synthesizer.to_markdown(submission)
        
        self.stdout.write(f"\n{'='*80}")
        self.stdout.write(self.style.SUCCESS(f"ACCESSION: {accession}"))
        self.stdout.write(f"ISSUER: {submission.issuer_name}")
        self.stdout.write(f"{'='*80}\n")
        
        # Check fields
        checks = {}
        
        def check_field(name, value):
            if value is None or value == '' or value == False:
                return None, "N/A"
            found = str(value).lower() in markdown.lower()
            return str(value), "✓" if found else "✗"
        
        # ISSUER FIELDS
        self.stdout.write(self.style.HTTP_INFO("[ISSUER FIELDS]"))
        
        val, status = check_field("issuer_name", submission.issuer_name)
        checks["issuer_name"] = status
        if val:
            self.stdout.write(f"  Issuer Name: {status} - {val[:60]}")
        
        val, status = check_field("issuer_trading_symbol", submission.issuer_trading_symbol)
        checks["issuer_trading_symbol"] = status
        if val:
            self.stdout.write(f"  Trading Symbol: {status} - {val}")
        
        val, status = check_field("issuer_foreign_trading_symbol", submission.issuer_foreign_trading_symbol)
        checks["issuer_foreign_trading_symbol"] = status
        if val:
            self.stdout.write(f"  Foreign Trading Symbol: {status} - {val}")
        
        val, status = check_field("schema_version", submission.schema_version)
        checks["schema_version"] = status
        if val:
            self.stdout.write(f"  Schema Version: {status} - {val}")
        
        # REPORTING OWNERS
        self.stdout.write(self.style.HTTP_INFO("\n[REPORTING OWNERS]"))
        for idx, owner in enumerate(submission.reporting_owners.all()[:1]):  # First owner
            self.stdout.write(f"\n  Owner: {owner.rptowner_name}")
            
            val, status = check_field("name", owner.rptowner_name)
            checks[f"owner_{idx}_name"] = status
            self.stdout.write(f"    Name: {status}")
            
            val, status = check_field("ccc", owner.rptowner_ccc)
            checks[f"owner_{idx}_ccc"] = status
            if val:
                self.stdout.write(f"    CCC: {status} - {val}")
            
            val, status = check_field("country", owner.rptowner_country)
            checks[f"owner_{idx}_country"] = status
            if val:
                self.stdout.write(f"    Country: {status} - {val}")
            
            if owner.rptowner_good_address:
                addr_str = f"{owner.rptowner_street1 or ''}, {owner.rptowner_city or ''}, {owner.rptowner_state or ''}".strip()
                val, status = check_field("address", addr_str)
                checks[f"owner_{idx}_address"] = status
                if val:
                    self.stdout.write(f"    Address: {status} - {val[:50]}")
        
        # NON-DERIVATIVE TRANSACTIONS
        self.stdout.write(self.style.HTTP_INFO("\n[NON-DERIVATIVE TRANSACTIONS]"))
        for idx, txn in enumerate(submission.non_deriv_transactions.all()[:1]):  # First txn
            self.stdout.write(f"\n  Transaction {idx+1}: {txn.security_title}")
            
            val, status = check_field("security_title", txn.security_title)
            checks[f"nonderiv_{idx}_security"] = status
            self.stdout.write(f"    Security: {status}")
            
            val, status = check_field("transaction_code", txn.transaction_code)
            checks[f"nonderiv_{idx}_code"] = status
            if val:
                self.stdout.write(f"    Code: {status} - {val}")
            
            val, status = check_field("shares", txn.transaction_shares)
            checks[f"nonderiv_{idx}_shares"] = status
            if val:
                self.stdout.write(f"    Shares: {status} - {val}")
            
            val, status = check_field("price", txn.transaction_price_per_share)
            checks[f"nonderiv_{idx}_price"] = status
            if val:
                self.stdout.write(f"    Price: {status} - {val}")
            
            val, status = check_field("transaction_total_value", txn.transaction_total_value)
            checks[f"nonderiv_{idx}_total_value"] = status
            if val:
                self.stdout.write(f"    Transaction Total Value: {status} - {val}")
            
            val, status = check_field("value_owned_following", txn.value_owned_following_transaction)
            checks[f"nonderiv_{idx}_value_after"] = status
            if val:
                self.stdout.write(f"    Value After Txn: {status} - {val}")
            
            val, status = check_field("nature_of_ownership", txn.nature_of_ownership)
            checks[f"nonderiv_{idx}_nature"] = status
            if val:
                self.stdout.write(f"    Nature of Ownership: {status} - {val[:40]}")
        
        # DERIVATIVE TRANSACTIONS
        self.stdout.write(self.style.HTTP_INFO("\n[DERIVATIVE TRANSACTIONS]"))
        for idx, deriv in enumerate(submission.deriv_transactions.all()[:1]):  # First deriv
            self.stdout.write(f"\n  Derivative {idx+1}: {deriv.security_title}")
            
            val, status = check_field("security_title", deriv.security_title)
            checks[f"deriv_{idx}_security"] = status
            self.stdout.write(f"    Security: {status}")
            
            val, status = check_field("underlying_security_title", deriv.underlying_security_title)
            checks[f"deriv_{idx}_underlying_title"] = status
            if val:
                self.stdout.write(f"    Underlying Security: {status} - {val}")
            
            val, status = check_field("conversion_price", deriv.conversion_or_exercise_price)
            checks[f"deriv_{idx}_conversion_price"] = status
            if val:
                self.stdout.write(f"    Conversion Price: {status} - {val}")
            
            val, status = check_field("exercise_date", deriv.exercise_date)
            checks[f"deriv_{idx}_exercise_date"] = status
            if val:
                self.stdout.write(f"    Exercise Date: {status} - {val}")
            
            val, status = check_field("expiration_date", deriv.expiration_date)
            checks[f"deriv_{idx}_expiration_date"] = status
            if val:
                self.stdout.write(f"    Expiration Date: {status} - {val}")
            
            val, status = check_field("underlying_shares", deriv.underlying_security_shares)
            checks[f"deriv_{idx}_underlying_shares"] = status
            if val:
                self.stdout.write(f"    Underlying Shares: {status} - {val}")
            
            val, status = check_field("underlying_value", deriv.underlying_security_value)
            checks[f"deriv_{idx}_underlying_value"] = status
            if val:
                self.stdout.write(f"    Underlying Value: {status} - {val}")
            
            val, status = check_field("value_after_txn", deriv.value_owned_following_transaction)
            checks[f"deriv_{idx}_value_after"] = status
            if val:
                self.stdout.write(f"    Value After Txn: {status} - {val}")
        
        # SUMMARY
        self.stdout.write("\n" + "="*80)
        passes = sum(1 for v in checks.values() if v == "✓")
        total = len(checks)
        pct = 100 * passes / total if total > 0 else 0
        
        if passes == total:
            self.stdout.write(self.style.SUCCESS(f"✓ ALL CHECKS PASSED ({passes}/{total}, 100%)"))
        else:
            self.stdout.write(self.style.WARNING(f"⚠ {passes}/{total} checks passed ({pct:.1f}%)"))
        
        self.stdout.write("="*80 + "\n")
        
        # Save output
        if options["output"]:
            with open(options["output"], 'w') as f:
                json.dump({
                    "accession": accession,
                    "issuer": submission.issuer_name,
                    "checks": checks,
                    "summary": {
                        "passed": passes,
                        "total": total,
                        "percentage": pct
                    }
                }, f, indent=2)
            self.stdout.write(f"✓ Results saved to {options['output']}")

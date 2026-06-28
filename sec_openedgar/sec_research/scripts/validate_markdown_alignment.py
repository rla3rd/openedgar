#!/usr/bin/env python3
"""
Validate that markdown synthesizer output aligns correctly with ground truth.
Compares markdown rendering against database ground truth without LLM parsing.
"""

import os
import sys
import json
import pathlib
import django
from decimal import Decimal

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'openedgar.settings')
sys.path.insert(0, '/home/ralbright/projects/openedgar/sec_openedgar')
django.setup()

from openedgar.models import OwnershipSubmission, Filing
from openedgar.parsers.ownership_parser import OwnershipParser as OwnershipMarkdownSynthesizer

def compare_values(field_name, ground_truth, markdown_contains_check):
    """Compare a field value between ground truth and markdown check."""
    if ground_truth is None or ground_truth == '' or ground_truth == False:
        return None, "N/A (empty)", "PASS"
    
    gt_str = str(ground_truth).strip()
    found = markdown_contains_check(gt_str)
    status = "✓ PASS" if found else "✗ FAIL"
    
    return gt_str, found, status


def validate_filing(submission: OwnershipSubmission):
    """Validate a single filing's markdown output against ground truth."""
    print(f"\n{'='*80}")
    print(f"ACCESSION: {submission.accession_number_id}")
    print(f"ISSUER: {submission.issuer_name}")
    print(f"{'='*80}")
    
    # Generate markdown
    synthesizer = OwnershipMarkdownSynthesizer()
    markdown = synthesizer.to_markdown(submission)
    
    results = {
        "accession": submission.accession_number_id,
        "issuer": submission.issuer_name,
        "checks": {}
    }
    
    # Markdown search function
    def md_contains(text):
        return text.lower() in markdown.lower()
    
    # 1. ISSUER FIELDS
    print("\n[ISSUER FIELDS]")
    checks = {}
    
    gt, found, status = compare_values("issuer_name", submission.issuer_name, md_contains)
    checks["issuer_name"] = {"ground_truth": gt, "in_markdown": found, "status": status}
    print(f"  Issuer Name: {status}")
    if gt: print(f"    GT: {gt}")
    
    gt, found, status = compare_values("issuer_trading_symbol", submission.issuer_trading_symbol, md_contains)
    checks["issuer_trading_symbol"] = {"ground_truth": gt, "in_markdown": found, "status": status}
    print(f"  Trading Symbol: {status}")
    if gt: print(f"    GT: {gt}")
    
    gt, found, status = compare_values("issuer_foreign_trading_symbol", submission.issuer_foreign_trading_symbol, md_contains)
    checks["issuer_foreign_trading_symbol"] = {"ground_truth": gt, "in_markdown": found, "status": status}
    print(f"  Foreign Trading Symbol: {status}")
    if gt: print(f"    GT: {gt}")
    
    gt, found, status = compare_values("schema_version", submission.schema_version, md_contains)
    checks["schema_version"] = {"ground_truth": gt, "in_markdown": found, "status": status}
    print(f"  Schema Version: {status}")
    if gt: print(f"    GT: {gt}")
    
    results["checks"]["issuer"] = checks
    
    # 2. REPORTING OWNERS
    print("\n[REPORTING OWNERS]")
    owner_checks = []
    for idx, owner in enumerate(submission.reporting_owners.all()):
        print(f"\n  Owner {idx+1}: {owner.rptowner_name}")
        owner_data = {}
        
        gt, found, status = compare_values("name", owner.rptowner_name, md_contains)
        owner_data["name"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Name: {status}")
        
        gt, found, status = compare_values("ccc", owner.rptowner_ccc, md_contains)
        owner_data["ccc"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    CCC: {status}")
        
        gt, found, status = compare_values("country", owner.rptowner_country, md_contains)
        owner_data["country"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Country: {status}")
        
        if owner.rptowner_good_address:
            gt, found, status = compare_values("address", owner.rptowner_good_address, md_contains)
            owner_data["address"] = {"ground_truth": gt, "in_markdown": found, "status": status}
            print(f"    Address: {status}")
        
        owner_checks.append(owner_data)
    
    results["checks"]["reporting_owners"] = owner_checks
    
    # 3. NON-DERIVATIVE TRANSACTIONS
    print("\n[NON-DERIVATIVE TRANSACTIONS]")
    nonderiv_checks = []
    for idx, txn in enumerate(submission.non_deriv_transactions.all()[:3]):  # Sample first 3
        print(f"\n  Transaction {idx+1}: {txn.security_title}")
        txn_data = {}
        
        gt, found, status = compare_values("security_title", txn.security_title, md_contains)
        txn_data["security_title"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Security: {status}")
        
        gt, found, status = compare_values("transaction_code", txn.transaction_code, md_contains)
        txn_data["transaction_code"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Code: {status}")
        
        gt, found, status = compare_values("shares", txn.transaction_shares, md_contains)
        txn_data["shares"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Shares: {status}")
        
        gt, found, status = compare_values("price", txn.transaction_price_per_share, md_contains)
        txn_data["price"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Price: {status}")
        
        gt, found, status = compare_values("value_owned_following", txn.value_owned_following_transaction, md_contains)
        txn_data["value_after_txn"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Value After Txn: {status}")
        
        gt, found, status = compare_values("nature_of_ownership", txn.nature_of_ownership, md_contains)
        txn_data["nature_of_ownership"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Nature of Ownership: {status}")
        
        nonderiv_checks.append(txn_data)
    
    results["checks"]["non_deriv_transactions"] = nonderiv_checks
    
    # 4. DERIVATIVE TRANSACTIONS
    print("\n[DERIVATIVE TRANSACTIONS]")
    deriv_checks = []
    for idx, deriv in enumerate(submission.deriv_transactions.all()[:2]):  # Sample first 2
        print(f"\n  Derivative {idx+1}: {deriv.security_title}")
        deriv_data = {}
        
        gt, found, status = compare_values("security_title", deriv.security_title, md_contains)
        deriv_data["security_title"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Security: {status}")
        
        gt, found, status = compare_values("underlying_security_title", deriv.underlying_security_title, md_contains)
        deriv_data["underlying_security_title"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Underlying Security: {status}")
        
        gt, found, status = compare_values("conversion_or_exercise_price", deriv.conversion_or_exercise_price, md_contains)
        deriv_data["conversion_price"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Conversion Price: {status}")
        
        gt, found, status = compare_values("exercise_date", deriv.exercise_date, md_contains)
        deriv_data["exercise_date"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Exercise Date: {status}")
        
        gt, found, status = compare_values("expiration_date", deriv.expiration_date, md_contains)
        deriv_data["expiration_date"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Expiration Date: {status}")
        
        gt, found, status = compare_values("underlying_security_shares", deriv.underlying_security_shares, md_contains)
        deriv_data["underlying_shares"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Underlying Shares: {status}")
        
        gt, found, status = compare_values("value_owned_following", deriv.value_owned_following_transaction, md_contains)
        deriv_data["value_after_txn"] = {"ground_truth": gt, "in_markdown": found, "status": status}
        print(f"    Value After Txn: {status}")
        
        deriv_checks.append(deriv_data)
    
    results["checks"]["derivative_transactions"] = deriv_checks
    
    # 5. SPECIAL FLAGS
    print("\n[SPECIAL FLAGS]")
    flag_checks = {}
    
    if submission.no_securities_owned:
        flag_checks["no_securities_owned"] = {"status": "✓ PASS" if "no securities" in markdown.lower() else "✗ FAIL"}
        print(f"  No Securities Owned: {flag_checks['no_securities_owned']['status']}")
    
    if submission.not_subject_to_section_16:
        flag_checks["not_subject_to_section_16"] = {"status": "✓ PASS" if "not subject" in markdown.lower() else "✗ FAIL"}
        print(f"  Not Subject to Section 16: {flag_checks['not_subject_to_section_16']['status']}")
    
    results["checks"]["flags"] = flag_checks
    
    # Summary
    print("\n" + "="*80)
    all_checks = []
    def collect_status(obj):
        if isinstance(obj, dict):
            if "status" in obj:
                all_checks.append(obj["status"])
            for v in obj.values():
                collect_status(v)
        elif isinstance(obj, list):
            for item in obj:
                collect_status(item)
    
    collect_status(results["checks"])
    
    passes = sum(1 for c in all_checks if "PASS" in c)
    fails = sum(1 for c in all_checks if "FAIL" in c)
    total = passes + fails
    
    print(f"\nSUMMARY: {passes}/{total} checks passed ({100*passes/total:.1f}%)")
    if fails > 0:
        print(f"⚠ {fails} field(s) missing or misaligned in markdown")
    print("="*80)
    
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--accessions", type=str, help="Comma-separated accession numbers or path to file")
    parser.add_argument("--limit", type=int, default=3, help="Number of random filings to check")
    parser.add_argument("--output", type=str, help="JSON file to save detailed results")
    
    args = parser.parse_args()
    
    # Get accessions
    if args.accessions:
        if args.accessions.endswith('.txt'):
            with open(args.accessions, 'r') as f:
                accs = [l.strip() for l in f if l.strip()]
        else:
            accs = [a.strip() for a in args.accessions.split(',')]
        
        queryset = OwnershipSubmission.objects.filter(accession_number_id__in=accs)
    else:
        queryset = OwnershipSubmission.objects.all().order_by('?')[:args.limit]
    
    all_results = []
    for sub in queryset:
        try:
            result = validate_filing(sub)
            all_results.append(result)
        except Exception as e:
            print(f"\n✗ ERROR validating {sub.accession_number_id}: {e}")
            import traceback
            traceback.print_exc()
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(all_results, f, indent=2)
        print(f"\n✓ Detailed results saved to {args.output}")


if __name__ == "__main__":
    main()

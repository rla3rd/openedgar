import json
import re
import sys
import argparse
from collections import defaultdict

def aggregate(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    fields = data['fields']
    metrics = {
        "Filing Metadata": {"prefixes": ["issuer_", "period_of_report", "form_type", "is_rule_10b5_plan", "no_securities_owned", "not_subject_to_section_16", "date_of_original_submission"], "exclude": []},
        "Reporting Owners": {"prefixes": ["reporting_owners_"], "exclude": []},
        "Table I: Non-Derivative Transactions": {"prefixes": ["non_derivative_transactions_"], "exclude": ["_fn"]},
        "Table I: Non-Derivative Holdings": {"prefixes": ["non_derivative_holdings_"], "exclude": ["_fn"]},
        "Table I: Footnote Alignment": {"prefixes": ["non_derivative_"], "exclude": [], "include_only": ["_fn"]},
        "Table II: Derivative Transactions": {"prefixes": ["derivative_transactions_"], "exclude": ["_fn"]},
        "Table II: Derivative Holdings": {"prefixes": ["derivative_holdings_"], "exclude": ["_fn"]},
        "Table II: Footnote Alignment": {"prefixes": ["derivative_"], "exclude": [], "include_only": ["_fn"]},
        "Signatures": {"prefixes": ["signatures_"], "exclude": []},
        "Footnotes": {"prefixes": ["footnotes_"], "exclude": []},
        "Remarks": {"prefixes": ["remarks"], "exclude": []},
    }

    results = {}
    for section, cfg in metrics.items():
        tp, fp, fn = 0, 0, 0
        support = 0
        for k, v in fields.items():
            if any(k.startswith(p) for p in cfg["prefixes"]):
                if cfg.get("include_only") and not any(k.endswith(s) for s in cfg["include_only"]):
                    continue
                if any(k.endswith(s) for s in cfg["exclude"]):
                    continue
                tp += v.get('tp', 0)
                fp += v.get('fp', 0)
                fn += v.get('fn', 0)
                support += v.get('support', 0)
        
        p = tp / (tp + fp) if (tp + fp) > 0 else 0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0
        results[section] = {"p": p, "r": r, "f1": f1, "support": support, "tp": tp, "fp": fp, "fn": fn}
    
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--json', default="sec_openedgar/sec_research/evaluation/hp_metrics_final_1638.json")
    args = parser.parse_args()
    
    results = aggregate(args.json)
    
    print(f"--- Results for {args.json} ---")
    total_tp, total_fp, total_fn = 0, 0, 0
    total_support = 0
    for section, m in results.items():
        print(f"{section} & {m['p']:.3f} & {m['r']:.3f} & {m['f1']:.3f} & {m['support']:,} \\\\")
        total_tp += m['tp']
        total_fp += m['fp']
        total_fn += m['fn']
        total_support += m['support']
    
    p = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
    r = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
    f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0
    print(f"\\textbf{{Overall Weighted Average}} & \\textbf{{{p:.3f}}} & \\textbf{{{r:.3f}}} & \\textbf{{{f1:.3f}}} & \\textbf{{{total_support:,}}} \\\\")

if __name__ == "__main__":
    main()

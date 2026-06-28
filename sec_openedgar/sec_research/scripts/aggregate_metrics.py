import json
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Aggregate SEC extraction metrics by filing section.')
    parser.add_argument('json_file', help='Path to the evaluation summary JSON file')
    parser.add_argument('--format', choices=['md', 'latex'], default='md', help='Output format')
    args = parser.parse_args()

    try:
        with open(args.json_file, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return

    fields = data.get('fields', {})

    def get_f1(prefixes, suffix=None, exclude_suffix=None):
        vals = []
        for k, v in fields.items():
            if any(k.startswith(p) for p in prefixes):
                if suffix and not k.endswith(suffix):
                    continue
                if exclude_suffix and k.endswith(exclude_suffix):
                    continue
                
                # Filter out fields with no support in ground truth
                if v.get('support', 0) > 0:
                    vals.append(v.get('f1', 0))
        return sum(vals)/len(vals) if vals else 0

    # Section Definitions
    sections = [
        ("Filing Metadata", ["issuer_", "period_of_report", "form_type", "is_rule_10b5_plan", "no_securities_owned", "not_subject_to_section_16"], None, None),
        ("Reporting Owners", ["reporting_owners_"], None, None),
        ("Table I: Non-Derivative Transactions", ["non_derivative_transactions_"], None, "_fn"),
        ("Table I: Non-Derivative Holdings", ["non_derivative_holdings_"], None, "_fn"),
        ("Table I: Footnote Alignment", ["non_derivative_transactions_", "non_derivative_holdings_"], "_fn", None),
        ("Table II: Derivative Transactions", ["derivative_transactions_"], None, "_fn"),
        ("Table II: Derivative Holdings", ["derivative_holdings_"], None, "_fn"),
        ("Table II: Footnote Alignment", ["derivative_transactions_", "derivative_holdings_"], "_fn", None),
        ("Signatures", ["signatures_"], None, None),
        ("Footnotes", ["footnotes_"], None, None),
        ("Remarks", ["remarks"], None, None),
    ]

    def get_metrics(prefixes, suffix=None, exclude_suffix=None):
        total_p_weighted = 0
        total_r_weighted = 0
        total_f1_weighted = 0
        total_support = 0
        
        for k, v in fields.items():
            if any(k.startswith(p) for p in prefixes):
                if suffix and not k.endswith(suffix):
                    continue
                if exclude_suffix and k.endswith(exclude_suffix):
                    continue
                
                supp = v.get('support', 0)
                if supp > 0:
                    total_p_weighted += v.get('precision', 0) * supp
                    total_r_weighted += v.get('recall', 0) * supp
                    total_f1_weighted += v.get('f1', 0) * supp
                    total_support += supp
        
        if total_support == 0: return 0, 0, 0, 0
        return total_p_weighted / total_support, total_r_weighted / total_support, total_f1_weighted / total_support, total_support

    results = []
    grand_p_weighted, grand_r_weighted, grand_f1_weighted, grand_support = 0, 0, 0, 0
    for label, prefixes, suffix, exclude in sections:
        p, r, f1, support = get_metrics(prefixes, suffix, exclude)
        results.append((label, p, r, f1, support))
        grand_p_weighted += p * support
        grand_r_weighted += r * support
        grand_f1_weighted += f1 * support
        grand_support += support

    avg_p = grand_p_weighted / grand_support if grand_support > 0 else 0
    avg_r = grand_r_weighted / grand_support if grand_support > 0 else 0
    avg_f1 = grand_f1_weighted / grand_support if grand_support > 0 else 0

    if args.format == 'md':
        print(f"| {'Section':<35} | {'P':<8} | {'R':<8} | {'F1':<8} | {'Support':<10} |")
        print(f"|{'-'*37}|{'-'*10}|{'-'*10}|{'-'*10}|{'-'*12}|")
        for label, p, r, f1, support in results:
            print(f"| {label:<35} | {p:8.4f} | {r:8.4f} | {f1:8.4f} | {support:10,d} |")
        print(f"|{'-'*37}|{'-'*10}|{'-'*10}|{'-'*10}|{'-'*12}|")
        print(f"| {'Overall Weighted Average':<35} | {avg_p:8.4f} | {avg_r:8.4f} | {avg_f1:8.4f} | {grand_support:10,d} |")
    else:
        # LaTeX format
        print(r"\begin{tabular}{lcccc}")
        print(r"\toprule")
        print(r"\textbf{Section} & \textbf{P} & \textbf{R} & \textbf{F1} & \textbf{Support} \\")
        print(r"\midrule")
        for label, p, r, f1, support in results:
            print(f"{label} & {p:.3f} & {r:.3f} & {f1:.3f} & {support:,d} \\\\")
        print(r"\midrule")
        print(rf"\textbf{{Overall Weighted Average}} & \textbf{{{avg_p:.3f}}} & \textbf{{{avg_r:.3f}}} & \textbf{{{avg_f1:.3f}}} & \textbf{{{grand_support:,d}}} \\")
        print(r"\bottomrule")
        print(r"\end{tabular}")

if __name__ == "__main__":
    main()

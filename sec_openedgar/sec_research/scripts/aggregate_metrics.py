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
                vals.append(v.get('f1', 0))
        return sum(vals)/len(vals) if vals else 0

    # Section Definitions
    sections = [
        ("Filing Metadata (Header)", ["issuer_", "period_of_report", "form_type", "is_rule_10b5_plan", "no_securities_owned", "not_subject_to_section_16"], None, None),
        ("Reporting Owners", ["reporting_owners_"], None, None),
        ("Table I: Non-Derivative Data", ["non_derivative_transactions_"], None, "_fn"),
        ("Table I: Footnote Alignment", ["non_derivative_transactions_"], "_fn", None),
        ("Table II: Derivative Data", ["derivative_transactions_"], None, "_fn"),
        ("Table II: Footnote Alignment", ["derivative_transactions_"], "_fn", None),
        ("Signatures", ["signatures_"], None, None),
        ("Remarks", ["remarks"], None, None),
    ]

    results = []
    total_f1 = 0
    for label, prefixes, suffix, exclude in sections:
        score = get_f1(prefixes, suffix, exclude)
        results.append((label, score))
        total_f1 += score

    overall_avg = total_f1 / len(sections)

    if args.format == 'md':
        print(f"| {'Section':<35} | {'Aggregate F1-Score':<20} |")
        print(f"|{'-'*37}|{'-'*22}|")
        for label, score in results:
            print(f"| {label:<35} | {score:18.4f} |")
        print(f"|{'-'*37}|{'-'*22}|")
        print(f"| {'Overall Weighted Average':<35} | {overall_avg:18.4f} |")
    else:
        # LaTeX format
        print(r"\begin{tabular}{lc}")
        print(r"\toprule")
        print(r"\textbf{SEC Filing Section} & \textbf{Aggregate F1-Score} \\")
        print(r"\midrule")
        for label, score in results:
            print(f"{label} & {score:.4f} \\\\")
        print(r"\midrule")
        print(rf"\textbf{{Overall Weighted Average}} & \textbf{{{overall_avg:.4f}}} \\")
        print(r"\bottomrule")
        print(r"\end{tabular}")

if __name__ == "__main__":
    main()

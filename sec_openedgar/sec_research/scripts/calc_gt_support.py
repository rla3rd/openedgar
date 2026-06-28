import os
import sys
import django
import collections
import re
from typing import Dict, Any

# Setup Django
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'sec_openedgar'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.local')
django.setup()

from openedgar.models import Filing
from sec_research.management.commands.evaluate_ownership_llm import Command as EvalCommand

def is_nullish(v):
    if v is None:
        return True
    if isinstance(v, (list, dict, tuple, set)):
        return len(v) == 0
    sv = str(v).strip().lower()
    NULLISH_STRINGS = {
        "", "none", "null", "[none]", "[null]", "[]", "{}",
        "_none_", "_none._", "_none", "none_", "n/a", "na",
    }
    return sv in NULLISH_STRINGS

def flatten(x, p='', res=None):
    if res is None: res = {}
    if isinstance(x, dict):
        for k, v in x.items():
            flatten(v, p + k + '_', res)
    elif isinstance(x, list):
        if not x:
            if p.endswith('_'):
                res[p[:-1]] = ""
            else:
                res[p] = ""
        else:
            for i, v in enumerate(x):
                flatten(v, p + str(i) + '_', res)
    else:
        if p.endswith('_'):
            res[p[:-1]] = str(x).strip().lower()
        else:
            res[p] = str(x).strip().lower()
    return res

def get_gt_support(holdout_file):
    with open(holdout_file, 'r') as f:
        accs = [line.strip() for line in f if line.strip()]
    
    print(f"Analyzing ground truth for {len(accs)} filings...")
    cmd = EvalCommand()
    total_support = collections.defaultdict(int)
    entity_counts = collections.defaultdict(int)
    
    for i, acc in enumerate(accs):
        try:
            f = Filing.objects.get(accession_number=acc)
            submission = f.ownershipsubmission_set.first()
            if not submission:
                continue
            gt_data = cmd.serialize_ground_truth(submission)
            
            # Field-level support
            flat_gt = flatten(gt_data)
            canon_gt = cmd.canonicalize_flat_dict(flat_gt)
            norm_gt = cmd.normalize_field_names(canon_gt)
            
            for k, v in norm_gt.items():
                if not is_nullish(v):
                    total_support[k] += 1
            
            # Entity counts
            entity_counts['Reporting Owners'] += len(gt_data.get('reporting_owners', []))
            entity_counts['Non-Derivative Transactions'] += len(gt_data.get('non_derivative_transactions', []))
            entity_counts['Non-Derivative Holdings'] += len(gt_data.get('non_derivative_holdings', []))
            entity_counts['Derivative Transactions'] += len(gt_data.get('derivative_transactions', []))
            entity_counts['Derivative Holdings'] += len(gt_data.get('derivative_holdings', []))
            entity_counts['Signatures'] += len(gt_data.get('signatures', []))
            entity_counts['Footnotes'] += len(gt_data.get('footnotes', []))
            entity_counts['Filing Metadata'] += 1
            entity_counts['Remarks'] += 1 if gt_data.get('remarks') else 0

        except Filing.DoesNotExist:
            print(f"Error: Filing {acc} not found")
        except Exception as e:
            print(f"Error processing {acc}: {e}")
        if (i+1) % 100 == 0:
            print(f"Processed {i+1} filings...")

    print("\n[Ground Truth Metrics Support]")
    print("-" * 75)
    print(f"{'Section':35} | {'Field Support':15} | {'Row/Entity Count'}")
    print("-" * 75)
    
    sections = collections.defaultdict(int)
    for k, v in total_support.items():
        if k.endswith('_fn'):
            if k.startswith('non_derivative'):
                sections['Table I: Footnote Alignment'] += v
            elif k.startswith('derivative'):
                sections['Table II: Footnote Alignment'] += v
            else:
                sections['Other Footnote Alignment'] += v
        elif k.startswith('non_derivative_transactions'):
            sections['Non-Derivative Transactions'] += v
        elif k.startswith('non_derivative_holdings'):
            sections['Non-Derivative Holdings'] += v
        elif k.startswith('derivative_transactions'):
            sections['Derivative Transactions'] += v
        elif k.startswith('derivative_holdings'):
            sections['Derivative Holdings'] += v
        elif k.startswith('reporting_owners'):
            sections['Reporting Owners'] += v
        elif k.startswith('issuer') or k in ['form_type', 'period_of_report', 'is_rule_10b5_plan']:
            sections['Filing Metadata'] += v
        elif k == 'remarks':
            sections['Remarks'] += v
        elif k.startswith('signatures'):
            sections['Signatures'] += v
        elif k.startswith('footnotes_'):
            sections['Footnotes'] += v
    
    row_counts = {
        'Filing Metadata': entity_counts['Filing Metadata'],
        'Reporting Owners': entity_counts['Reporting Owners'],
        'Non-Derivative Transactions': entity_counts['Non-Derivative Transactions'],
        'Non-Derivative Holdings': entity_counts['Non-Derivative Holdings'],
        'Derivative Transactions': entity_counts['Derivative Transactions'],
        'Derivative Holdings': entity_counts['Derivative Holdings'],
        'Signatures': entity_counts['Signatures'],
        'Footnotes': entity_counts['Footnotes'],
        'Remarks': entity_counts['Remarks'],
        'Table I: Footnote Alignment': entity_counts['Non-Derivative Transactions'] + entity_counts['Non-Derivative Holdings'],
        'Table II: Footnote Alignment': entity_counts['Derivative Transactions'] + entity_counts['Derivative Holdings']
    }

    for s in sorted(sections.keys()):
        field_v = sections[s]
        row_v = row_counts.get(s, 0)
        print(f"{s:35} | {field_v:<15} | {row_v}")
    print("-" * 75)

if __name__ == "__main__":
    test_set = 'sec_openedgar/sec_research/evaluation/test_set_2000.txt'
    if not os.path.exists(test_set):
        test_set = 'sec_research/evaluation/test_set_2000.txt'
    get_gt_support(test_set)

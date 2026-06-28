import os
import django
import json
import pathlib
import sys
from tqdm import tqdm

# Setup Django
script_path = pathlib.Path(__file__).resolve()
project_root = script_path.parents[3]
sys.path.append(str(project_root / "sec_openedgar"))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.local')
django.setup()

from openedgar.models import OwnershipSubmission
from sec_research.management.commands.evaluate_ownership_llm import Command as EvalCommand

def export_gt():
    gt_dir = pathlib.Path("sec_openedgar/sec_research/evaluation/gt_cache")
    gt_dir.mkdir(parents=True, exist_ok=True)
    
    # CLEAR OLD CACHE
    for f in gt_dir.glob("*.json"):
        f.unlink()
    
    cmd = EvalCommand()
    
    # LOAD THE HOLDOUT LIST
    acc_list_path = pathlib.Path("sec_openedgar/sec_research/evaluation/test_set_accs_500.txt")
    with open(acc_list_path, 'r') as f:
        original_accs = [line.strip() for line in f if line.strip()]

    # Filter for Pure Form 4 from the list
    submissions = list(OwnershipSubmission.objects.filter(
        accession_number_id__in=original_accs,
        form_type='4'
    ))
    
    # ADD THE 2 REPLACEMENTS to hit 500
    replacements = ['0001104659-21-008733', '0001209191-21-029883']
    for acc in replacements:
        sub = OwnershipSubmission.objects.get(accession_number_id=acc)
        submissions.append(sub)

    print(f"Exporting {len(submissions)} pure Form 4 filings (including 2 replacements)...")
    
    for submission in tqdm(submissions):
        acc = submission.accession_number_id
        try:
            gt_json = cmd.serialize_ground_truth(submission)
            with open(gt_dir / f"{acc}.json", 'w') as f:
                json.dump(gt_json, f, indent=2)
        except Exception as e:
            print(f"Error {acc}: {e}")

if __name__ == "__main__":
    export_gt()

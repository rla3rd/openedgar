import os
import django
import json
import argparse
import pathlib
import subprocess
from django.core import serializers

# Setup Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.local")
django.setup()

from openedgar.models import (
    OwnershipSubmission, OwnershipReportingOwner,
    OwnershipNonDerivTransaction, OwnershipNonDerivHolding,
    OwnershipDerivTransaction, OwnershipDerivHolding
)

# Dataset Registry: Maps Type -> Models
DATASET_REGISTRY = {
    "Ownership": {
        "root": OwnershipSubmission,
        "related": [
            OwnershipReportingOwner,
            OwnershipNonDerivTransaction,
            OwnershipNonDerivHolding,
            OwnershipDerivTransaction,
            OwnershipDerivHolding
        ],
        "root_id_field": "accession_number_id",
        "related_fk": "submission"
    }
}

def get_accessions(input_source):
    accessions = set()
    if os.path.isfile(input_source):
        with open(input_source, "r") as f:
            for line in f:
                acc = line.strip()
                if acc: accessions.add(acc)
    else:
        # Assume comma-separated list
        accessions.update([a.strip() for a in input_source.split(",")])
    return accessions

def export_surgical_metadata(accessions, dataset_type, output_file):
    if dataset_type not in DATASET_REGISTRY:
        raise ValueError(f"Unknown dataset type: {dataset_type}")
    
    config = DATASET_REGISTRY[dataset_type]
    print(f"Exporting surgical metadata for {len(accessions)} accessions (Type: {dataset_type})...")
    
    # Get Root Records
    filter_kwargs = {f"{config['root_id_field']}__in": accessions}
    root_records = config["root"].objects.filter(**filter_kwargs)
    
    all_objects = list(root_records)
    
    # Get Related Records
    for model in config["related"]:
        related_kwargs = {f"{config['related_fk']}__in": root_records}
        all_objects.extend(list(model.objects.filter(**related_kwargs)))
    
    with open(output_file, "w") as f:
        serializers.serialize("json", all_objects, indent=2, stream=f)
    
    print(f"Exported {len(all_objects)} objects to {output_file}")
    return all_objects

def bundle_files(accessions, metadata_file, bundle_name, data_root):
    file_list_path = "sec_research/finetuning/bundle_files.txt"
    with open(file_list_path, "w") as f:
        f.write(f"{metadata_file}\n")
        
        # Find and add markdown sidecars
        found_count = 0
        for acc in accessions:
            found = False
            # Search in data/filings/YEAR/QX/ACC/
            for year_dir in pathlib.Path(data_root).glob("*"):
                if not year_dir.is_dir(): continue
                for q_dir in year_dir.glob("Q*"):
                    acc_dir = q_dir / acc
                    if acc_dir.exists():
                        f.write(f"{acc_dir}\n")
                        found = True
                        found_count += 1
                        break
                if found: break
    
    print(f"Bundled {found_count}/{len(accessions)} sidecar directories.")
    
    try:
        subprocess.run(["tar", "--zstd", "-cf", bundle_name, "-T", file_list_path], check=True)
        print(f"SUCCESS! Bundle created: {bundle_name}")
    except Exception as e:
        print(f"Error creating bundle: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenEDGAR Generic Research Bundler")
    parser.add_argument("--accessions", required=True, help="Path to file with accessions OR comma-separated list")
    parser.add_argument("--type", default="Ownership", help="Dataset type (default: Ownership)")
    parser.add_argument("--output", default="research_bundle.tar.zst", help="Output bundle filename")
    parser.add_argument("--data-root", default="data/filings", help="Path to filing sidecars root")
    
    args = parser.parse_args()
    
    acc_list = get_accessions(args.accessions)
    meta_file = "sec_research/finetuning/surgical_metadata.json"
    
    export_surgical_metadata(acc_list, args.type, meta_file)
    bundle_files(acc_list, meta_file, args.output, args.data_root)

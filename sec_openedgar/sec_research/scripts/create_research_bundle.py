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

def bundle_files(accessions, metadata_file, bundle_name, data_root, extra_files=None):
    file_list_path = "sec_research/finetuning/bundle_files.txt"
    with open(file_list_path, "w") as bundle_list_file:
        bundle_list_file.write(f"{metadata_file}\n")
        if extra_files:
            for ef in extra_files:
                if os.path.exists(ef):
                    bundle_list_file.write(f"{ef}\n")
        
        # Find and add markdown sidecars
        found_count = 0
        print(f"Indexing sidecars in {data_root}... (Single Pass Optimization)")
        
        # Build a map of accession -> path
        accession_map = {}
        for root, dirs, files in os.walk(data_root):
            # Check directories
            for d in dirs:
                if d in accessions:
                    accession_map[d] = os.path.join(root, d)
            
            # Check files (for the YEAR/QTR/MONTH/DAY/ACC.md.zst structure)
            for f in files:
                acc_prefix = f.split('.')[0]
                if acc_prefix in accessions:
                    file_path = os.path.join(root, f)
                    if acc_prefix not in accession_map:
                        accession_map[acc_prefix] = [file_path]
                    else:
                        if isinstance(accession_map[acc_prefix], list):
                            accession_map[acc_prefix].append(file_path)
        
        for acc in accessions:
            if acc in accession_map:
                paths = accession_map[acc]
                if isinstance(paths, list):
                    for p in paths:
                        bundle_list_file.write(f"{p}\n")
                else:
                    bundle_list_file.write(f"{paths}\n")
                found_count += 1
            else:
                # print(f"Warning: Could not find sidecar for {acc}")
                pass
    
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
    default_data_root = os.getenv("EDGAR_LOCAL_DATA_DIR", "data/filings")
    if os.path.exists(os.path.join(default_data_root, "data")):
        default_data_root = os.path.join(default_data_root, "data")
        
    parser.add_argument("--data-root", default=default_data_root, help="Path to filing sidecars root")
    parser.add_argument("--include", help="Comma-separated list of extra files to include")
    
    args = parser.parse_args()
    
    acc_list = get_accessions(args.accessions)
    meta_file = "sec_research/finetuning/surgical_metadata.json"
    
    extra_files = args.include.split(",") if args.include else []
    
    export_surgical_metadata(acc_list, args.type, meta_file)
    bundle_files(acc_list, meta_file, args.output, args.data_root, extra_files=extra_files)

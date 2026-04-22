import os
import subprocess

def main():
    print("Exporting research data from PostgreSQL to portable JSON format...")
    # We only dump the relevant research models to keep it fast and small
    models_to_dump = [
        "openedgar.OwnershipSubmission",
        "openedgar.OwnershipReportingOwner",
        "openedgar.OwnershipNonDerivTransaction",
        "openedgar.OwnershipNonDerivHolding",
        "openedgar.OwnershipDerivTransaction",
        "openedgar.OwnershipDerivHolding",
        "openedgar.OwnershipFootnote",
        "openedgar.OwnershipSignature"
    ]
    
    dump_file = "sec_research/finetuning/research_data_dump.json"
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(dump_file), exist_ok=True)
    
    cmd = [
        "python", "manage.py", "dumpdata",
        "--database=default",
        "--indent", "2",
        "-o", dump_file
    ] + models_to_dump
    
    try:
        subprocess.run(cmd, check=True)
        print(f"Successfully dumped research data to {dump_file}")
        print("\nTo use this on your M4 Max:")
        print("1. Set your Django settings to use an SQLite3 database.")
        print("2. Run: python manage.py migrate")
        print(f"3. Run: python manage.py loaddata {dump_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error dumping data: {e}")

if __name__ == "__main__":
    main()

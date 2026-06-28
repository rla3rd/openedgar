import json
import os
import sys

# Add the project root to the path so we can import from openedgar
sys.path.append('/home/ralbright/projects/openedgar/sec_openedgar')

def main():
    json_path = 'sec_openedgar/sec_research/evaluation/hp_metrics_final_1638.json'
    if not os.path.exists(json_path):
        print(f"Error: {json_path} not found")
        return

    with open(json_path, 'r') as f:
        metrics = json.load(f)

    fields_data = metrics.get('fields', {})
    sorted_fields = sorted(fields_data.items())
    
    print("% Exhaustive Metrics (Support > 0)")
    for k, v in sorted_fields:
        supp = v.get('support', 0)
        if supp > 0:
            name = k.replace('_', '\\_')
            p = v.get('precision', 0)
            r = v.get('recall', 0)
            f1 = v.get('f1', 0)
            print(f"{name} & {p:.3f} & {r:.3f} & {f1:.3f} & {supp:,} \\\\")

    print("\n% Fields with Zero Support (Always Null in Test Set)")
    null_fields = [k.replace('_', '\\_') for k, v in sorted_fields if v.get('support', 0) == 0]
    print("% " + ", ".join(null_fields))

if __name__ == "__main__":
    main()

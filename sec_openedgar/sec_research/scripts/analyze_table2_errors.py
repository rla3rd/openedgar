import json
import pathlib
import re
from collections import defaultdict

def analyze_errors(gt_dir, llm_dir, limit=50):
    gt_path = pathlib.Path(gt_dir)
    llm_path = pathlib.Path(llm_dir)
    
    common = set(f.name for f in gt_path.glob("*.json")).intersection(set(f.name for f in llm_path.glob("*.json")))
    
    mismatches = []
    
    for fname in sorted(list(common))[:limit]:
        with open(gt_path / fname, 'r') as f:
            gt_data = json.load(f)
        with open(llm_path / fname, 'r') as f:
            llm_data = json.load(f)
            
        gt_holdings = gt_data.get('derivative_holdings', [])
        llm_holdings = llm_data.get('derivative_holdings', [])
        
        if len(gt_holdings) != len(llm_holdings):
            mismatches.append({
                "acc": fname.replace('.json', ''),
                "type": "count_mismatch",
                "gt_count": len(gt_holdings),
                "llm_count": len(llm_holdings),
                "gt_titles": [h.get('security_title') for h in gt_holdings],
                "llm_titles": [h.get('security_title') for h in llm_holdings]
            })
            continue
            
        # If counts match, check for value mismatches in key fields
        for i, (g, l) in enumerate(zip(gt_holdings, llm_holdings)):
            for field in ['security_title', 'shares_owned_following_transaction', 'conversion_or_exercise_price']:
                gv = str(g.get(field, '')).strip().lower()
                lv = str(l.get(field, '')).strip().lower()
                
                # Basic normalization for numeric comparison
                if 'shares' in field or 'price' in field:
                    try:
                        gv = float(gv.replace(',', ''))
                        lv = float(lv.replace(',', ''))
                    except:
                        pass
                
                if gv != lv:
                    mismatches.append({
                        "acc": fname.replace('.json', ''),
                        "type": "value_mismatch",
                        "index": i,
                        "field": field,
                        "gt_val": gv,
                        "llm_val": lv
                    })
                    break

    return mismatches

if __name__ == "__main__":
    gt = "sec_research/evaluation/gt_cache"
    llm = "sec_research/evaluation/hp_cache_qwen3-coder-30b"
    errors = analyze_errors(gt, llm)
    
    print(f"Total files analyzed: 50")
    print(f"Total mismatches found: {len(errors)}")
    print("\nSample Errors:")
    for e in errors[:10]:
        print(json.dumps(e, indent=2))

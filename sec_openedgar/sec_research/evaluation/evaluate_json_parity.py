import json
import os
import pathlib
import re
from typing import Any, Dict, List, Set
from collections import defaultdict

class JSONEvaluator:
    def __init__(self, gt_dir: str, llm_dir: str):
        self.gt_dir = pathlib.Path(gt_dir)
        self.llm_dir = pathlib.Path(llm_dir)
        self.metrics = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})

    STATE_TO_ABBR = {
        "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
        "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
        "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
        "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
        "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
        "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
        "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
        "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
        "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
        "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
        "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC"
    }

    def normalize_value(self, key: str, value: Any) -> Any:
        if value is None:
            return None
        
        s = str(value).strip()
        
        # Numeric normalization for shares, price, value, amount
        if any(x in key for x in ["shares", "price", "value", "amount"]) and "cik" not in key:
            try:
                # Convert to float and round to 2 decimals
                return round(float(s.replace(',', '')), 2)
            except (ValueError, TypeError):
                pass
        
        # CIK padding
        if "cik" in key:
            return s.zfill(10).lower()

        s = s.lower()
        if "signature_name" in key:
            s = re.sub(r'^/s/\s*', '', s)
            
        # State normalization
        if ('_state' in key or key.endswith('state')) and 'state_description' not in key:
            compact = re.sub(r'[^a-z\s]', '', s).strip()
            if compact in self.STATE_TO_ABBR:
                s = self.STATE_TO_ABBR[compact]

        # Basic address/text normalization
        s = re.sub(r'\s+', ' ', s).strip()
        
        # Stable matching (remove punctuation) unless it's a narrative field
        if not any(x in key for x in ["summary", "footnotes", "remarks"]):
            s = re.sub(r'[^\w\s]', '', s).strip()
            
        return s

    def values_match(self, key: str, gt_val: Any, llm_val: Any) -> bool:
        if gt_val == llm_val:
            return True
        
        # Name fields: allow substring match if both are present
        if 'name' in key and gt_val and llm_val:
            if isinstance(gt_val, str) and isinstance(llm_val, str):
                return llm_val in gt_val or gt_val in llm_val
        
        return False

    def flatten(self, d: Any, prefix: str = "") -> Dict[str, str]:
        items = {}
        if isinstance(d, dict):
            for k, v in d.items():
                new_key = f"{prefix}_{k}" if prefix else k
                items.update(self.flatten(v, new_key))
        elif isinstance(d, list):
            for i, v in enumerate(d):
                new_key = f"{prefix}_{i}" if prefix else str(i)
                items.update(self.flatten(v, new_key))
        else:
            items[prefix] = self.normalize_value(prefix, d)
        return items

    def evaluate(self):
        gt_files = set(f.name for f in self.gt_dir.glob("*.json"))
        llm_files = set(f.name for f in self.llm_dir.glob("*.json"))
        common = gt_files.intersection(llm_files)
        
        print(f"Comparing {len(common)} filings...")
        
        for fname in common:
            with open(self.gt_dir / fname, 'r') as f:
                gt_data = self.flatten(json.load(f))
            with open(self.llm_dir / fname, 'r') as f:
                llm_data = self.flatten(json.load(f))
                
            all_keys = set(gt_data.keys()).union(llm_data.keys())
            for k in all_keys:
                # Group keys by field type
                metric_key = re.sub(r'_\d+_', '_', k)
                
                # IGNORE LIST: Fields that don't exist in GT or are internal
                if any(x in metric_key for x in ["summary", "footnotes", "schema_version", "remarks"]):
                    continue
                
                gt_val = gt_data.get(k)
                llm_val = llm_data.get(k)
                
                if gt_val is not None and llm_val is not None:
                    if self.values_match(metric_key, gt_val, llm_val):
                        self.metrics[metric_key]["tp"] += 1
                    else:
                        self.metrics[metric_key]["fn"] += 1
                        self.metrics[metric_key]["fp"] += 1
                elif gt_val is not None and llm_val is None:
                    self.metrics[metric_key]["fn"] += 1
                elif gt_val is None and llm_val is not None:
                    self.metrics[metric_key]["fp"] += 1

    def report(self):
        print(f"{'Field':<50} | {'Prec':<6} | {'Rec':<6} | {'F1':<6}")
        print("-" * 75)
        for k, v in sorted(self.metrics.items()):
            tp, fp, fn = v["tp"], v["fp"], v["fn"]
            prec = tp / (tp + fp) if (tp + fp) > 0 else 0
            rec = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * (prec * rec) / (prec + rec) if (prec + rec) > 0 else 0
            if tp + fn > 50: # Only show fields with significant presence
                print(f"{k:<50} | {prec:.3f} | {rec:.3f} | {f1:.3f}")

if __name__ == "__main__":
    import sys
    evaluator = JSONEvaluator(sys.argv[1], sys.argv[2])
    evaluator.evaluate()
    evaluator.report()

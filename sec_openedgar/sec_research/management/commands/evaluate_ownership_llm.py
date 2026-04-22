import json
import pathlib
import re
import collections
from decimal import Decimal, InvalidOperation
import requests
import pyzstd
from django.core.management.base import BaseCommand
from openedgar.models import OwnershipSubmission, Filing
from sec_research.experiments.ownership_extraction.synthesizers.ownership import OwnershipMarkdownSynthesizer
from sec_research.utils.prompts import get_ownership_extraction_prompt
from tqdm import tqdm
from typing import Dict, Any, List

try:
    from rouge_score import rouge_scorer
    ROUGE_AVAILABLE = True
except ImportError:
    ROUGE_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer, util
    ST_AVAILABLE = True
except ImportError:
    ST_AVAILABLE = False

class Command(BaseCommand):
    help = "Evaluate SEC Ownership extraction against the database ground truth."

    def add_arguments(self, parser):
        parser.add_argument("--holdout", type=str, help="Path to accession numbers list.")
        parser.add_argument("--model", type=str, default="qwen2.5-35b-instruct")
        parser.add_argument("--url", type=str, default="http://localhost:1234/v1/chat/completions")
        parser.add_argument("--cache-dir", type=str, default="scratch/hp_cache_final")
        parser.add_argument("--summary-out", type=str, help="JSON path for metrics summary.")
        parser.add_argument("--limit", type=int, default=500)

    STATE_TO_ABBR = {
        "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
        "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de",
        "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id",
        "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks",
        "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
        "massachusetts": "ma", "michigan": "mi", "minnesota": "mn", "mississippi": "ms",
        "missouri": "mo", "montana": "mt", "nebraska": "ne", "nevada": "nv",
        "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm", "new york": "ny",
        "north carolina": "nc", "north dakota": "nd", "ohio": "oh", "oklahoma": "ok",
        "oregon": "or", "pennsylvania": "pa", "rhode island": "ri", "south carolina": "sc",
        "south dakota": "sd", "tennessee": "tn", "texas": "tx", "utah": "ut",
        "vermont": "vt", "virginia": "va", "washington": "wa", "west virginia": "wv",
        "wisconsin": "wi", "wyoming": "wy", "district of columbia": "dc"
    }

    NUMERIC_FIELDS = {
        "shares",
        "price",
        "transaction_total_value",
        "shares_owned_following_transaction",
        "value_owned_following_transaction",
        "conversion_or_exercise_price",
        "underlying_security_shares",
        "underlying_security_value",
    }

    TIMELINESS_ALLOWED = {"", "e", "l"}

    NULLISH_STRINGS = {
        "",
        "none",
        "null",
        "[none]",
        "[null]",
        "[]",
        "{}",
        "_none_",
        "_none._",
        "_none",
        "none_",
        "n/a",
        "na",
    }

    def is_numeric_like(self, value: str) -> bool:
        s = value.strip().replace(',', '')
        return bool(re.fullmatch(r'[-+]?\d+(?:\.\d+)?', s))

    def normalize_number(self, value: str) -> str:
        s = value.strip().replace(',', '')
        try:
            d = Decimal(s)
        except InvalidOperation:
            return value.strip().lower()
        if d == d.to_integral_value():
            return str(d.to_integral_value())
        n = format(d.normalize(), 'f')
        n = n.rstrip('0').rstrip('.') if '.' in n else n
        return n or '0'

    def normalize_value(self, key: str, value: Any) -> str:
        s = str(value).strip()
        # Do NOT normalize CIKs as numbers (preserves leading zeros)
        if "_cik" in key.lower():
            return s.zfill(10).lower()
        if self.is_numeric_like(s):
            return self.normalize_number(s)

        s = s.lower()
        s = re.sub(r'[\s\n\r\t]+', ' ', s).strip()

        # Preserve punctuation in footnotes and remarks for faithful textual matching.
        if key.startswith('footnotes_') or key == 'remarks':
            return s
            
        # Strip common signature prefixes like /s/
        if "signature_name" in key:
            s = re.sub(r'^/s/\s*', '', s)
            return s.strip()

        # Normalize state names to two-letter abbreviations for state fields.
        if ('_state' in key or key.endswith('state')) and 'state_description' not in key:
            compact = re.sub(r'[^a-z\s]', '', s).strip()
            if compact in self.STATE_TO_ABBR:
                s = self.STATE_TO_ABBR[compact]

        # Keep alphanumerics/spaces for stable matching.
        s = re.sub(r'[^\w\s]', '', s)
        return s.strip()

    def values_match(self, field: str, truth_val: str, llm_val: str, null_sentinel: str) -> bool:
        if truth_val == llm_val:
            return True

        # Name fields: allow llm value to be substring of ground truth.
        if 'name' in field and truth_val != null_sentinel and llm_val != null_sentinel:
            return llm_val in truth_val

        return False

    def sanitize_numeric_string(self, value: str) -> Any:
        """Normalize numeric strings (e.g. 1,234.50 -> 1234.5)."""
        s = value.strip().replace(',', '')
        if not re.fullmatch(r'[-+]?\d+(?:\.\d+)?', s):
            return value.strip()
        try:
            return float(Decimal(s))
        except InvalidOperation:
            return value.strip()

    def sanitize_llm_output(self, data: Any) -> Any:
        """Sanitize extracted JSON prior to cache/emit.

        - Trims surrounding whitespace from all strings.
        - Coerces numeric schema fields from comma-formatted strings to numbers.
        """
        if isinstance(data, dict):
            out = {}
            for k, v in data.items():
                if isinstance(v, str):
                    v = v.strip()
                if k in self.NUMERIC_FIELDS and isinstance(v, str):
                    out[k] = self.sanitize_numeric_string(v)
                else:
                    out[k] = self.sanitize_llm_output(v)
            return out

        if isinstance(data, list):
            return [self.sanitize_llm_output(v) for v in data]

        if isinstance(data, str):
            return data.strip()

        return data

    def repair_extraction(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """No-op by design: evaluate raw model extraction without corrective remapping."""
        return data

    def add_combined_street_keys(self, flat_dict: Dict[str, str]) -> Dict[str, str]:
        """Add owner-level combined street keys to avoid street1/street2 split penalties."""
        out = dict(flat_dict)
        idxs = set()
        for k in flat_dict.keys():
            m = re.match(r'^reporting_owners_(\d+)_street[12]$', k)
            if m:
                idxs.add(m.group(1))

        for idx in idxs:
            s1 = str(flat_dict.get(f"reporting_owners_{idx}_street1", "")).strip()
            s2 = str(flat_dict.get(f"reporting_owners_{idx}_street2", "")).strip()
            parts = [p for p in [s1, s2] if p]
            out[f"reporting_owners_{idx}_street_combined"] = "|".join(sorted(parts)) if parts else ""
        return out

    def parse_fn_list(self, fn_str: str) -> List[str]:
        if not fn_str: return []
        try:
            # Handle JSON arrays or comma-separated strings
            if fn_str.startswith('['):
                return [str(x).upper().replace('F', '') for x in json.loads(fn_str)]
            return [x.strip().upper().replace('F', '') for x in fn_str.split(',') if x.strip()]
        except:
            return []

    def generate_rationale(self, transaction, footnotes):
        """Synthetic Auditor: explain the transaction based on data and footnotes."""
        desc = transaction.security_title or "Security"
        code = transaction.transaction_code or "?"
        shares = getattr(transaction, 'transaction_shares', getattr(transaction, 'underlying_security_shares', 0))
        price = getattr(transaction, 'transaction_price_per_share', getattr(transaction, 'conversion_or_exercise_price', 0))
        ad = getattr(transaction, 'transaction_acquired_disposed_code', '')
        
        action = "acquired" if ad == 'A' else "disposed" if ad == 'D' else "held"
        summary = f"The reporting owner {action} {shares} shares of {desc} at ${price} (Code {code})."
        
        # Append relevant footnote text
        fn_ids = self.parse_fn_list(getattr(transaction, 'security_title_fn', '')) + \
                 self.parse_fn_list(getattr(transaction, 'transaction_shares_fn', '')) + \
                 self.parse_fn_list(getattr(transaction, 'transaction_price_per_share_fn', ''))
        
        added_fns = []
        for fid in fn_ids:
            if fid in footnotes and fid not in added_fns:
                summary += f" [Note {fid}: {footnotes[fid]}]"
                added_fns.append(fid)
        return summary

    def parse_fn_references(self, fn_str: str) -> list:
        """Convert comma-separated footnote references to array. E.g., '1,2,3' -> ['1', '2', '3']"""
        if not fn_str or not isinstance(fn_str, str):
            return []
        return [f.strip() for f in fn_str.split(',') if f.strip()]

    def serialize_ground_truth(self, submission: OwnershipSubmission) -> dict:
        """Exhaustive ground truth serialization matching the PostgreSQL schema."""
        footnotes = {fn.footnote_id.upper().replace('F', ''): fn.footnote_text for fn in submission.footnotes.all()}
        
        issuer = {
            "issuer_cik": str(submission.issuer_cik_id).zfill(10),
            "issuer_name": submission.issuer_name,
            "issuer_trading_symbol": submission.issuer_trading_symbol or "",
            "issuer_foreign_trading_symbol": submission.issuer_foreign_trading_symbol or "",
            "schema_version": submission.schema_version or ""
        }
        
        owners = []
        for o in submission.reporting_owners.all():
            owners.append({
                "rptowner_cik": str(o.rptowner_cik).zfill(10),
                "rptowner_name": o.rptowner_name,
                "rptowner_ccc": o.rptowner_ccc or "",
                "is_director": o.is_director,
                "is_officer": o.is_officer,
                "is_10pctowner": o.is_10pctowner,
                "is_other": o.is_other,
                "is_director_nominee": o.is_director_nominee,
                "officer_title": o.officer_title or "",
                "other_text": o.other_text or "",
                "rptowner_street1": o.rptowner_street1 or "",
                "rptowner_street2": o.rptowner_street2 or "",
                "rptowner_city": o.rptowner_city or "",
                "rptowner_state": o.rptowner_state or "",
                "rptowner_zip": o.rptowner_zip or "",
                "rptowner_non_us_address_flag": o.rptowner_non_us_address_flag,
                "rptowner_non_us_state_territory": o.rptowner_non_us_state_territory or "",
                "rptowner_country": o.rptowner_country or "",
                "rptowner_state_description": o.rptowner_state_description or "",
                "rptowner_good_address": o.rptowner_good_address
            })
            
        non_deriv = []
        for t in submission.non_deriv_transactions.all():
            non_deriv.append({
                "security_title": t.security_title or "",
                "security_title_fn": self.parse_fn_references(t.security_title_fn),
                "transaction_date": str(t.transaction_date) if t.transaction_date else "",
                "transaction_date_fn": self.parse_fn_references(t.transaction_date_fn),
                "deemed_execution_date": str(t.deemed_execution_date) if t.deemed_execution_date else "",
                "deemed_execution_date_fn": self.parse_fn_references(t.deemed_execution_date_fn),
                "transaction_code": t.transaction_code or "",
                "transaction_code_fn": self.parse_fn_references(t.transaction_code_fn),
                "transaction_timeliness": t.transaction_timeliness or "",
                "transaction_timeliness_fn": self.parse_fn_references(t.transaction_timeliness_fn),
                "transaction_form_type": t.transaction_form_type or "",
                "equity_swap_involved": t.equity_swap_involved,
                "shares": t.transaction_shares or 0.0,
                "shares_fn": self.parse_fn_references(t.transaction_shares_fn),
                "price": t.transaction_price_per_share or 0.0,
                "price_fn": self.parse_fn_references(t.transaction_price_per_share_fn),
                "transaction_total_value": t.transaction_total_value or 0.0,
                "transaction_total_value_fn": self.parse_fn_references(t.transaction_total_value_fn),
                "transaction_acquired_disposed_code": t.transaction_acquired_disposed_code or "",
                "transaction_acquired_disposed_code_fn": self.parse_fn_references(t.transaction_acquired_disposed_code_fn),
                "shares_owned_following_transaction": t.shares_owned_following_transaction or 0.0,
                "shares_owned_following_transaction_fn": self.parse_fn_references(t.shares_owned_following_transaction_fn),
                "value_owned_following_transaction": t.value_owned_following_transaction or 0.0,
                "value_owned_following_transaction_fn": self.parse_fn_references(t.value_owned_following_transaction_fn),
                "direct_or_indirect_ownership": t.direct_or_indirect_ownership or "",
                "direct_or_indirect_ownership_fn": self.parse_fn_references(t.direct_or_indirect_ownership_fn),
                "nature_of_ownership": t.nature_of_ownership or "",
                "nature_of_ownership_fn": self.parse_fn_references(t.nature_of_ownership_fn),
                "transaction_summary": self.generate_rationale(t, footnotes)
            })
        
        deriv = []
        for d in submission.deriv_transactions.all():
            deriv.append({
                "security_title": d.security_title or "",
                "security_title_fn": self.parse_fn_references(d.security_title_fn),
                "conversion_or_exercise_price": d.conversion_or_exercise_price or 0.0,
                "conversion_or_exercise_price_fn": self.parse_fn_references(d.conversion_or_exercise_price_fn),
                "transaction_date": str(d.transaction_date) if d.transaction_date else "",
                "transaction_date_fn": self.parse_fn_references(d.transaction_date_fn),
                "deemed_execution_date": str(d.deemed_execution_date) if d.deemed_execution_date else "",
                "deemed_execution_date_fn": self.parse_fn_references(d.deemed_execution_date_fn),
                "transaction_code": d.transaction_code or "",
                "transaction_code_fn": self.parse_fn_references(d.transaction_code_fn),
                "transaction_timeliness": d.transaction_timeliness or "",
                "transaction_timeliness_fn": self.parse_fn_references(d.transaction_timeliness_fn),
                "transaction_form_type": d.transaction_form_type or "",
                "equity_swap_involved": d.equity_swap_involved,
                "transaction_acquired_disposed_code": d.transaction_acquired_disposed_code or "",
                "transaction_acquired_disposed_code_fn": self.parse_fn_references(d.transaction_acquired_disposed_code_fn),
                "exercise_date": str(d.exercise_date) if d.exercise_date else "",
                "exercise_date_fn": self.parse_fn_references(d.exercise_date_fn),
                "expiration_date": str(d.expiration_date) if d.expiration_date else "",
                "expiration_date_fn": self.parse_fn_references(d.expiration_date_fn),
                "underlying_security_title": d.underlying_security_title or "",
                "underlying_security_title_fn": self.parse_fn_references(d.underlying_security_title_fn),
                "underlying_security_shares": d.underlying_security_shares or 0.0,
                "underlying_security_shares_fn": self.parse_fn_references(d.underlying_security_shares_fn),
                "underlying_security_value": d.underlying_security_value or 0.0,
                "underlying_security_value_fn": self.parse_fn_references(d.underlying_security_value_fn),
                "shares_owned_following_transaction": d.shares_owned_following_transaction or 0.0,
                "shares_owned_following_transaction_fn": self.parse_fn_references(d.shares_owned_following_transaction_fn),
                "value_owned_following_transaction": d.value_owned_following_transaction or 0.0,
                "value_owned_following_transaction_fn": self.parse_fn_references(d.value_owned_following_transaction_fn),
                "direct_or_indirect_ownership": d.direct_or_indirect_ownership or "",
                "direct_or_indirect_ownership_fn": self.parse_fn_references(d.direct_or_indirect_ownership_fn),
                "nature_of_ownership": d.nature_of_ownership or "",
                "nature_of_ownership_fn": self.parse_fn_references(d.nature_of_ownership_fn),
                "transaction_summary": self.generate_rationale(d, footnotes)
            })

        non_deriv_holdings = []
        for h in submission.non_deriv_holdings.all():
            non_deriv_holdings.append({
                "security_title": h.security_title or "",
                "security_title_fn": self.parse_fn_references(h.security_title_fn),
                "shares_owned_following_transaction": h.shares_owned_following_transaction or 0.0,
                "shares_owned_following_transaction_fn": self.parse_fn_references(h.shares_owned_following_transaction_fn),
                "value_owned_following_transaction": h.value_owned_following_transaction or 0.0,
                "value_owned_following_transaction_fn": self.parse_fn_references(h.value_owned_following_transaction_fn),
                "direct_or_indirect_ownership": h.direct_or_indirect_ownership or "",
                "direct_or_indirect_ownership_fn": self.parse_fn_references(h.direct_or_indirect_ownership_fn),
                "nature_of_ownership": h.nature_of_ownership or "",
                "nature_of_ownership_fn": self.parse_fn_references(h.nature_of_ownership_fn),
                "transaction_form_type": h.transaction_form_type or ""
            })

        deriv_holdings = []
        for h in submission.deriv_holdings.all():
            deriv_holdings.append({
                "security_title": h.security_title or "",
                "security_title_fn": self.parse_fn_references(h.security_title_fn),
                "conversion_or_exercise_price": h.conversion_or_exercise_price or 0.0,
                "conversion_or_exercise_price_fn": self.parse_fn_references(h.conversion_or_exercise_price_fn),
                "exercise_date": str(h.exercise_date) if h.exercise_date else "",
                "exercise_date_fn": self.parse_fn_references(h.exercise_date_fn),
                "expiration_date": str(h.expiration_date) if h.expiration_date else "",
                "expiration_date_fn": self.parse_fn_references(h.expiration_date_fn),
                "underlying_security_title": h.underlying_security_title or "",
                "underlying_security_title_fn": self.parse_fn_references(h.underlying_security_title_fn),
                "underlying_security_shares": h.underlying_security_shares or 0.0,
                "underlying_security_shares_fn": self.parse_fn_references(h.underlying_security_shares_fn),
                "underlying_security_value": h.underlying_security_value or 0.0,
                "underlying_security_value_fn": self.parse_fn_references(h.underlying_security_value_fn),
                "shares_owned_following_transaction": h.shares_owned_following_transaction or 0.0,
                "shares_owned_following_transaction_fn": self.parse_fn_references(h.shares_owned_following_transaction_fn),
                "value_owned_following_transaction": h.value_owned_following_transaction or 0.0,
                "value_owned_following_transaction_fn": self.parse_fn_references(h.value_owned_following_transaction_fn),
                "direct_or_indirect_ownership": h.direct_or_indirect_ownership or "",
                "direct_or_indirect_ownership_fn": self.parse_fn_references(h.direct_or_indirect_ownership_fn),
                "nature_of_ownership": h.nature_of_ownership or "",
                "nature_of_ownership_fn": self.parse_fn_references(h.nature_of_ownership_fn),
                "transaction_form_type": h.transaction_form_type or ""
            })

        signatures = []
        for s in submission.signatures.all():
            signatures.append({
                "signature_name": s.signature_name,
                "signature_date": str(s.signature_date)
            })

        return {
            "issuer": issuer,
            "form_type": submission.form_type,
            "period_of_report": str(submission.period_of_report) if submission.period_of_report else "",
            "date_of_original_submission": str(submission.date_of_original_submission) if submission.date_of_original_submission else "",
            "remarks": submission.remarks or "",
            "not_subject_to_section_16": submission.not_subject_to_section_16,
            "is_rule_10b5_1_plan": submission.is_rule_10b5_1_plan,
            "no_securities_owned": submission.no_securities_owned,
            "reporting_owners": owners,
            "non_derivative_transactions": non_deriv,
            "non_derivative_holdings": non_deriv_holdings,
            "derivative_transactions": deriv,
            "derivative_holdings": deriv_holdings,
            "footnotes": footnotes,
            "signatures": signatures
        }

    def flatten(self, x, n=''):
        """Flatten nested dict/list structure into flattened key-value pairs."""
        res = {}
        def _f(y, p=''):
            if isinstance(y, dict):
                for k in y: _f(y[k], p+k+'_')
            elif isinstance(y, list):
                for i, v in enumerate(y): _f(v, p+str(i)+'_')
            else: res[p[:-1]] = str(y).strip().lower()
        _f(x, n)
        return res

    def normalize_field_names(self, flat_dict):
        """Normalize field names to canonical form for consistent comparison.
        Handles cases like 'issuer_issuer_cik' -> 'issuer_cik', 'reporting_owners_rptowner_name' -> 'reporting_owners_name'
        """
        normalized = {}
        for k, v in flat_dict.items():
            # Strip redundant prefix patterns
            # e.g., issuer_issuer_* -> issuer_*
            clean_k = re.sub(r'^(\w+)_\1_', r'\1_', k)
            # e.g., reporting_owners_rptowner_* -> reporting_owners_*
            clean_k = re.sub(r'_rptowner_', '_', clean_k)
            # e.g., non_derivative_transactions_transaction_* -> non_derivative_transactions_*
            clean_k = re.sub(r'_(\w+)_\1_', r'_', clean_k)
            # e.g., non_derivative_holdings_shares_owned_shares_owned_* -> non_derivative_holdings_shares_owned_*
            clean_k = re.sub(r'(shares_owned)_\1', r'\1', clean_k)
            clean_k = re.sub(r'(nature_of_ownership)_\1', r'\1', clean_k)
            # Final deduplication of any remaining patterns
            clean_k = re.sub(r'([a-z_]+?)_\1(?=_|$)', r'\1', clean_k)
            normalized[clean_k] = v
        return normalized

    def canonicalize_flat_dict(self, flat_dict: Dict[str, str]) -> Dict[str, str]:
        """Canonicalize flattened values before comparison.

        Footnote references are normalized as order-insensitive sets per base field,
        so values from list flattening (e.g. *_fn_0, *_fn_1) and compact strings
        (e.g. [^1][^2] or 1,2) compare consistently.
        """
        fn_buckets: Dict[str, List[str]] = collections.defaultdict(list)
        out: Dict[str, str] = {}

        def _extract_fn_ids(raw: str) -> List[str]:
            if raw is None:
                return []
            s = str(raw).strip()
            if not s:
                return []

            # Handles [^1][^2], [1][2], and mixed inline refs.
            bracketed = re.findall(r'\[\^?\s*([A-Za-z0-9]+)\s*\]', s)
            if bracketed:
                return [x.upper().replace('F', '') for x in bracketed if x]

            # Handles comma-separated refs and scalar refs.
            parts = [p.strip() for p in s.split(',') if p.strip()]
            return [p.upper().replace('F', '') for p in parts]

        for k, v in flat_dict.items():
            idx_match = re.match(r'^(.*_fn)_\d+$', k)
            if idx_match:
                fn_buckets[idx_match.group(1)].extend(_extract_fn_ids(v))
                continue

            if k.endswith('_fn'):
                fn_buckets[k].extend(_extract_fn_ids(v))
                continue

            out[k] = v

        for base_k, ids in fn_buckets.items():
            # Deduplicate while preserving deterministic comparison output.
            uniq_sorted = sorted({x for x in ids if x})
            out[base_k] = '|'.join(uniq_sorted)

        return out

    def call_llm(self, content: str, model: str, url: str, acc: str = None, cache_dir: str = None) -> dict:
        if cache_dir and acc:
            cache_path = pathlib.Path(cache_dir) / f"{acc}.json"
            if cache_path.exists():
                with open(cache_path, 'r') as f:
                    try:
                        data = json.load(f)
                        if data: return data
                    except: pass

        prompt = get_ownership_extraction_prompt()
        
        text = ""
        for attempt in range(3):
            try:
                r = requests.post(url, json={
                    "model": model, 
                    "messages": [{"role": "user", "content": f"{prompt}\n\n{content}"}], 
                    "temperature": 0.0,
                    "max_tokens": 32768
                }, timeout=600)
                if r.status_code != 200: continue
                text = r.json()['choices'][0]['message'].get('content', '')
                
                # 1. Try to extract JSON from markdown code block
                m = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
                raw_json = m.group(1).strip() if m else None
                
                # 2. If no code block, find the first '{' and last '}'
                if not raw_json:
                    start = text.find('{')
                    end = text.rfind('}')
                    if start >= 0 and end > start:
                        raw_json = text[start:end+1].strip()
                
                if not raw_json or len(raw_json) < 10:
                    continue
                
                # Clean up the JSON string for common LLM artifacts
                try:
                    # Strip reasoning block if present (<thought>...</thought>)
                    raw_json = re.sub(r'<thought>.*?</thought>', '', raw_json, flags=re.DOTALL)
                    
                    # Strip hash comments: e.g., "value": 100, # Comment -> "value": 100,
                    raw_json = re.sub(r'(?m)\s*#.*$', '', raw_json)
                    
                    # Fix numbers with commas: e.g., 1,000.0 -> 1000.0
                    # This regex finds commas between digits that are NOT inside quotes
                    # We do this by finding patterns like : 1,234.56
                    raw_json = re.sub(r'(:\s*)(-?\d+(?:,\d+)+(?:\.\d+)?)', lambda m: m.group(1) + m.group(2).replace(',', ''), raw_json)
                    
                    data = json.loads(raw_json, strict=False)
                except (json.JSONDecodeError, ValueError):
                    # Attempt to fix unescaped newlines inside strings
                    parts = raw_json.split('"')
                    for i in range(1, len(parts), 2):  # odd indices are inside strings
                        parts[i] = parts[i].replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                    fixed_json = '"'.join(parts)
                    try:
                        data = json.loads(fixed_json, strict=False)
                    except:
                        continue
                
                data = self.sanitize_llm_output(data)

                if data and cache_dir and acc:
                    pathlib.Path(cache_dir).mkdir(parents=True, exist_ok=True)
                    with open(pathlib.Path(cache_dir) / f"{acc}.json", 'w') as f:
                        json.dump(data, f, indent=2)
                return data
            except Exception as e:
                import sys
                print(f"DEBUG: Failed to parse JSON for {acc}: {str(e)[:300]}", file=sys.stderr)
                # Save the raw failed response for debugging
                if cache_dir and acc:
                    fail_dir = pathlib.Path(cache_dir).parent / "failed_json"
                    fail_dir.mkdir(parents=True, exist_ok=True)
                    with open(fail_dir / f"{acc}_failed.txt", 'w') as f:
                        f.write(text)
        return {}

    def handle(self, *args, **options):
        base_dir = pathlib.Path("/home/ralbright/data/openedgar/edgar")
        queryset = OwnershipSubmission.objects.all()
        if options['holdout']:
            with open(options['holdout'], 'r') as f:
                accs = [l.strip() for l in f if l.strip()]
            queryset = queryset.filter(accession_number_id__in=accs)
        
        limit = options.get('limit')
        if limit:
            pks = list(queryset[:limit].values_list('pk', flat=True))
            queryset = OwnershipSubmission.objects.filter(pk__in=pks).select_related('accession_number').prefetch_related('reporting_owners', 'non_deriv_transactions', 'non_deriv_holdings', 'deriv_transactions', 'deriv_holdings', 'footnotes', 'signatures')
        else:
            queryset = queryset.select_related('accession_number').prefetch_related('reporting_owners', 'non_deriv_transactions', 'non_deriv_holdings', 'deriv_transactions', 'deriv_holdings', 'footnotes', 'signatures')
        
        metrics = collections.defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})

        processed = 0
        scorer = rouge_scorer.RougeScorer(['rougeL'], use_stemmer=True) if ROUGE_AVAILABLE else None
        rationale_scores = []
        bert_pairs = []

        for sub in queryset:
            acc = sub.accession_number_id
            md_path = base_dir / pathlib.Path(sub.accession_number.path).parent / f"{acc}.out.md.zst"
            if not md_path.exists():
                self.stdout.write(f"DEBUG: Skipping {acc} - Markdown NOT FOUND at {md_path}")
                continue
            
            self.stdout.write(f"DEBUG: Processing {acc}...")
            with open(md_path, "rb") as f: md = pyzstd.decompress(f.read()).decode('utf-8')
            
            truth = self.serialize_ground_truth(sub)
            llm = self.call_llm(md, options['model'], options['url'], acc=acc, cache_dir=options['cache_dir'])
            processed += 1
            self.stdout.write(f"DEBUG: Finished {acc}. Total processed: {processed}")
            
            def is_nullish(v):
                if v is None:
                    return True
                if isinstance(v, (list, dict, tuple, set)):
                    return len(v) == 0
                sv = str(v).strip().lower()
                return sv in self.NULLISH_STRINGS

            t_flat_raw = self.canonicalize_flat_dict(self.flatten(truth))
            l_flat_raw = self.canonicalize_flat_dict(self.flatten(llm))
            t_flat_raw = self.add_combined_street_keys(t_flat_raw)
            l_flat_raw = self.add_combined_street_keys(l_flat_raw)
            t_flat = {}
            l_flat = {}

            NULL_SENTINEL = "__NULL__"

            t_all = {
                k: (NULL_SENTINEL if is_nullish(v) else self.normalize_value(k, v))
                for k, v in t_flat_raw.items()
            }
            l_all = {
                k: (NULL_SENTINEL if is_nullish(v) else self.normalize_value(k, v))
                for k, v in l_flat_raw.items()
            }

            for k, v in t_flat_raw.items():
                if is_nullish(v):
                    continue
                t_flat[k] = self.normalize_value(k, v)
            
            for k, v in l_flat_raw.items():
                if is_nullish(v):
                    continue
                l_flat[k] = self.normalize_value(k, v)
            
            # Normalize field names for both truth and LLM to canonical form
            t_flat = self.normalize_field_names(t_flat)
            l_flat = self.normalize_field_names(l_flat)
            t_all = self.normalize_field_names(t_all)
            l_all = self.normalize_field_names(l_all)
            
            # Debug: check issuer fields
            if processed == 1:
                issuer_truth = {k: v for k, v in t_flat.items() if 'issuer' in k}
                issuer_llm = {k: v for k, v in l_flat.items() if 'issuer' in k}
                self.stdout.write(f"DEBUG: Ground truth issuer fields: {issuer_truth}")
                self.stdout.write(f"DEBUG: LLM issuer fields: {issuer_llm}")
                
                # Find fields with F1=0 and diagnose
                all_truth_fields = set(t_flat.keys())
                all_llm_fields = set(l_flat.keys())
                missing_from_llm = all_truth_fields - all_llm_fields
                extra_in_llm = all_llm_fields - all_truth_fields
                mismatched = {k: (t_flat[k], l_flat.get(k)) for k in all_truth_fields & all_llm_fields if t_flat[k] != l_flat[k]}
                
                if missing_from_llm:
                    self.stdout.write(f"\nDEBUG: Missing from LLM (extraction failures):")
                    for field in sorted(missing_from_llm)[:10]:
                        self.stdout.write(f"  - {field}: {t_flat[field]}")
                if extra_in_llm:
                    self.stdout.write(f"\nDEBUG: Extra in LLM (false positives):")
                    for field in sorted(extra_in_llm)[:10]:
                        self.stdout.write(f"  + {field}: {l_flat[field]}")
                if mismatched:
                    self.stdout.write(f"\nDEBUG: Value mismatches:")
                    for field, (truth_val, llm_val) in sorted(mismatched.items())[:10]:
                        self.stdout.write(f"  ~ {field}: '{truth_val}' vs '{llm_val}'")

            all_metric_keys = set(t_all.keys()) | set(l_all.keys())
            for k in all_metric_keys:
                if "summary" in k:
                    t_val = t_all.get(k, "")
                    l_val = l_all.get(k, "")
                    if t_val != NULL_SENTINEL and l_val != NULL_SENTINEL:
                        if scorer:
                            score = scorer.score(t_val, l_val)['rougeL'].fmeasure
                            rationale_scores.append(score)
                        if ST_AVAILABLE:
                            bert_pairs.append((t_val, l_val))
                    continue
                if re.match(r'^reporting_owners_\d+_street[12]$', k):
                    # Avoid double-penalizing address line split differences.
                    continue
                field = re.sub(r'_\d+_', '_', k)
                t_val = t_all.get(k, NULL_SENTINEL)
                l_val = l_all.get(k, NULL_SENTINEL)

                # Nullish (or missing) on both sides is treated as a match.
                if t_val == NULL_SENTINEL and l_val == NULL_SENTINEL:
                    metrics[field]["tp"] += 1
                    continue

                if self.values_match(k, t_val, l_val, NULL_SENTINEL):
                    metrics[field]["tp"] += 1
                else:
                    if l_val != NULL_SENTINEL:
                        metrics[field]["fp"] += 1
                    if t_val != NULL_SENTINEL:
                        metrics[field]["fn"] += 1

        self.stdout.write(f"\n--- EXHAUSTIVE F1 SUMMARY ({processed} Filings) ---")
        summary = {
            "processed": processed,
            "fields": {}
        }
        for field in sorted(metrics.keys()):
            m = metrics[field]
            p = m["tp"]/(m["tp"]+m["fp"]) if m["tp"]+m["fp"]>0 else 0
            r = m["tp"]/(m["tp"]+m["fn"]) if m["tp"]+m["fn"]>0 else 0
            f1 = 2*p*r/(p+r) if p+r>0 else 0
            summary["fields"][field] = {
                "f1": round(f1, 6),
                "precision": round(p, 6),
                "recall": round(r, 6),
                "tp": m["tp"],
                "fp": m["fp"],
                "fn": m["fn"],
            }
            self.stdout.write(f"[{field:45}] F1: {f1:.3f} | P: {p:.3f} | R: {r:.3f}")

        if rationale_scores:
            avg_rouge = sum(rationale_scores) / len(rationale_scores)
            summary["avg_rougeL"] = round(avg_rouge, 6)
            self.stdout.write(f"\nAverage ROUGE-L Score: {avg_rouge:.4f}")

        if bert_pairs and ST_AVAILABLE:
            self.stdout.write("Calculating Semantic Similarity (Sentence-Transformers)...")
            cands = [p[1] for p in bert_pairs]
            refs = [p[0] for p in bert_pairs]
            model = SentenceTransformer('all-MiniLM-L6-v2')
            cand_embs = model.encode(cands, convert_to_tensor=True)
            ref_embs = model.encode(refs, convert_to_tensor=True)
            cosine_scores = util.cos_sim(cand_embs, ref_embs)
            similarities = cosine_scores.diag()
            avg_sim = similarities.mean().item()
            summary["avg_semantic_sim"] = round(avg_sim, 6)
            self.stdout.write(f"Average Semantic Similarity (Cosine): {avg_sim:.4f}")

        if options.get('summary_out'):
            summary_path = pathlib.Path(options['summary_out'])
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2)
            self.stdout.write(self.style.SUCCESS(f"Summary written to {summary_path}"))

        self.stdout.write(self.style.SUCCESS(f"\nExhaustive Baseline Complete!"))

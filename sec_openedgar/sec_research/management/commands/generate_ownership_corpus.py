import os
import json
import re
import pathlib
import pyzstd
from django.core.management.base import BaseCommand
from django.conf import settings
from openedgar.models import Filing, OwnershipSubmission
from sec_research.experiments.ownership_extraction.synthesizers.ownership import OwnershipMarkdownSynthesizer
from sec_research.utils.prompts import get_ownership_extraction_prompt
from tqdm import tqdm

class Command(BaseCommand):
    help = 'Exports a perfectly aligned dataset of (Markdown, Golden JSON) for LLM fine-tuning.'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=1500, help='Number of records to export')
        parser.add_argument('--out', type=str, default='scratch/sec_ownership_finetune_v3.jsonl', help='Output JSONL filename')
        parser.add_argument('--year', type=int, help='Filter by filing year')
        parser.add_argument('--random', action='store_true', help='Select filings randomly')
        parser.add_argument('--seed', type=int, default=3836, help='Seed for random selection')
        parser.add_argument('--train-list', type=str, help='Path to train_accessions.txt')

    def parse_fn_list(self, fn_str):
        if not fn_str: return []
        try:
            if isinstance(fn_str, str) and fn_str.startswith('['):
                return [str(x).upper().replace('F', '') for x in json.loads(fn_str)]
            ids = re.findall(r'\d+', str(fn_str))
            return sorted(list(set(ids)))
        except: return []

    def generate_rationale(self, transaction, footnotes):
        """Synthetic Auditor: explain the transaction based on data and footnotes."""
        desc = transaction.security_title or "Security"
        code = transaction.transaction_code or "?"
        shares = getattr(transaction, 'transaction_shares', getattr(transaction, 'underlying_security_shares', 0))
        price = getattr(transaction, 'transaction_price_per_share', getattr(transaction, 'conversion_or_exercise_price', 0))
        ad = getattr(transaction, 'transaction_acquired_disposed_code', '')
        
        action = "acquired" if ad == 'A' else "disposed" if ad == 'D' else "held"
        summary = f"The reporting owner {action} {shares} shares of {desc} at ${price} (Code {code})."
        
        fn_ids = self.parse_fn_list(getattr(transaction, 'security_title_fn', '')) + \
                 self.parse_fn_list(getattr(transaction, 'transaction_shares_fn', '')) + \
                 self.parse_fn_list(getattr(transaction, 'transaction_price_per_share_fn', ''))
        
        added_fns = []
        for fid in fn_ids:
            txt = footnotes.get(fid) or footnotes.get(f"F{fid}")
            if txt and fid not in added_fns:
                summary += f" [Note {fid}: {txt}]"
                added_fns.append(fid)
        return summary

    def serialize_ground_truth(self, submission: OwnershipSubmission) -> dict:
        """Serializes the Django Ground Truth into the Expert Target JSON Schema."""
        footnotes = {fn.footnote_id.upper().replace('F', ''): fn.footnote_text for fn in submission.footnotes.all()}
        
        issuer = {
            "issuer_cik": str(submission.issuer_cik_id).zfill(10),
            "issuer_name": submission.issuer_name,
            "issuer_trading_symbol": submission.issuer_trading_symbol or ""
        }
        
        owners = []
        for o in submission.reporting_owners.all():
            owners.append({
                "rptowner_cik": str(o.rptowner_cik).zfill(10),
                "rptowner_name": o.rptowner_name,
                "is_director": o.is_director,
                "is_officer": o.is_officer,
                "is_10pctowner": o.is_10pctowner,
                "officer_title": o.officer_title or "",
                "other_text": o.other_text or ""
            })
            
        non_deriv = []
        for t in submission.non_deriv_transactions.all():
            non_deriv.append({
                "security_title": t.security_title or "",
                "security_title_fn": self.parse_fn_list(t.security_title_fn),
                "transaction_date": str(t.transaction_date) if t.transaction_date else "",
                "deemed_execution_date": str(t.deemed_execution_date) if t.deemed_execution_date else "",
                "transaction_code": t.transaction_code or "",
                "transaction_acquired_disposed_code": t.transaction_acquired_disposed_code or "",
                "shares": t.transaction_shares or 0.0,
                "shares_fn": self.parse_fn_list(t.transaction_shares_fn),
                "price": t.transaction_price_per_share or 0.0,
                "price_fn": self.parse_fn_list(t.transaction_price_per_share_fn),
                "shares_owned_following_transaction": t.shares_owned_following_transaction or 0.0,
                "direct_or_indirect_ownership": t.direct_or_indirect_ownership or "",
                "nature_of_ownership": getattr(t, 'nature_of_ownership_fn', ""),
                "transaction_summary": self.generate_rationale(t, footnotes)
            })

        deriv = []
        for d in submission.deriv_transactions.all():
            deriv.append({
                "security_title": d.security_title or "",
                "security_title_fn": self.parse_fn_list(d.security_title_fn),
                "conversion_or_exercise_price": d.conversion_or_exercise_price or 0.0,
                "conversion_or_exercise_price_fn": self.parse_fn_list(d.conversion_or_exercise_price_fn),
                "transaction_date": str(d.transaction_date) if d.transaction_date else "",
                "deemed_execution_date": str(d.deemed_execution_date) if d.deemed_execution_date else "",
                "transaction_code": d.transaction_code or "",
                "transaction_acquired_disposed_code": d.transaction_acquired_disposed_code or "",
                "exercise_date": str(d.exercise_date) if d.exercise_date else "",
                "expiration_date": str(d.expiration_date) if d.expiration_date else "",
                "underlying_security_title": d.underlying_security_title or "",
                "underlying_security_shares": d.underlying_security_shares or 0.0,
                "shares_owned_following_transaction": d.shares_owned_following_transaction or 0.0,
                "direct_or_indirect_ownership": d.direct_or_indirect_ownership or "",
                "transaction_summary": self.generate_rationale(d, footnotes)
            })
        
        return {
            "issuer": issuer,
            "remarks": submission.remarks or "",
            "not_subject_to_section_16": submission.not_subject_to_section_16,
            "is_rule_10b5_1_plan": submission.is_rule_10b5_1_plan,
            "no_securities_owned": submission.no_securities_owned,
            "reporting_owners": owners,
            "non_derivative_transactions": non_deriv,
            "derivative_transactions": deriv,
            "footnotes": footnotes,
            "signatures": [{"signature_name": s.signature_name, "signature_date": str(s.signature_date)} for s in submission.signatures.all()]
        }

    def handle(self, *args, **options):
        base_dir_str = os.getenv("EDGAR_LOCAL_DATA_DIR", getattr(settings, "EDGAR_LOCAL_DATA_DIR", "/media/data"))
        base_dir = pathlib.Path(base_dir_str)
        out_file = options['out']
        
        system_prompt = get_ownership_extraction_prompt()
        
        queryset = OwnershipSubmission.objects.all().prefetch_related('reporting_owners', 'non_deriv_transactions', 'deriv_transactions', 'footnotes', 'signatures')
        
        if options['train_list']:
            with open(options['train_list'], 'r') as f:
                train_accs = [line.strip() for line in f if line.strip()]
            queryset = queryset.filter(accession_number_id__in=train_accs)
            
        exported = 0
        limit = options['limit']
        
        with open(out_file, 'w', encoding='utf-8') as f:
            for sub in tqdm(queryset, desc="Exporting"):
                if exported >= limit: break
                
                # High-Fidelity Synthesis (Exhaustive & Structural Alignment)
                synthesizer = OwnershipMarkdownSynthesizer()
                try:
                    full_path = base_dir / sub.accession_number.path
                    raw_sgml_path = full_path.parent / f"{sub.accession_number_id}.sgml.zst"
                    if raw_sgml_path.exists():
                        with open(raw_sgml_path, "rb") as zf:
                            raw_sgml = pyzstd.decompress(zf.read()).decode('utf-8', errors='ignore')
                            markdown = synthesizer.synthesize(raw_sgml, accession=sub.accession_number_id)
                    else: continue
                except: continue
                    
                record = {
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"Extract and audit this filing:\n\n{markdown}"},
                        {"role": "assistant", "content": f"```json\n{json.dumps(self.serialize_ground_truth(sub))}\n```"}
                    ]
                }
                f.write(json.dumps(record) + "\n")
                exported += 1
                
        self.stdout.write(self.style.SUCCESS(f"Successfully exported {exported} records to {out_file}"))

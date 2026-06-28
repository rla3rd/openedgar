import pathlib
import pyzstd
import re
from django.core.management.base import BaseCommand
from openedgar.models import (
    OwnershipSubmission, OwnershipSignature, OwnershipReportingOwner, 
    OwnershipNonDerivTransaction, OwnershipNonDerivHolding, 
    OwnershipDerivTransaction, OwnershipDerivHolding, OwnershipFootnote, Filing
)
from lxml import etree

class Command(BaseCommand):
    help = 'High-fidelity repair of Ownership filings in the test set including footnotes.'

    def add_arguments(self, parser):
        parser.add_argument('--holdout', type=str, help='Path to test set accession list.')

    def get_tag_text(self, element, tag, default=''):
        if element is None: return default
        found = element.find(tag)
        if found is None: return default
        val_node = found.find('value')
        if val_node is not None and val_node.text:
            return val_node.text.strip()
        return found.text.strip() if found.text else default

    def resolve_fn(self, element):
        if element is None: return ""
        ids = []
        for fn in element.findall('.//footnoteId'):
            fid = fn.get('id', '')
            clean_id = fid.upper().replace('F', '')
            if clean_id: ids.append(f"{clean_id}")
        return ",".join(ids)

    def handle(self, *args, **options):
        if not options['holdout']:
            self.stderr.write("Please provide --holdout")
            return

        with open(options['holdout'], 'r') as f:
            accs = [l.strip() for l in f if l.strip()]

        submissions = OwnershipSubmission.objects.filter(accession_number_id__in=accs)
        total = submissions.count()
        self.stdout.write(f"Repairing {total} filings with high-fidelity footnote alignment.")

        processed = 0
        fixed_count = 0
        
        for sub in submissions:
            acc = sub.accession_number_id
            filing = sub.accession_number
            sgml_path = filing.resolved_sgml_path
            
            if not sgml_path: continue

            p = pathlib.Path(sgml_path)
            parent = p.parent
            doc_files = list(parent.glob(f"{acc}.*01.zst")) + list(parent.glob(f"{acc}.[345].zst"))
            doc_files = sorted([f for f in doc_files if ".md" not in f.name and ".sgml" not in f.name])
            
            xml_content = ""
            if doc_files:
                try:
                    with open(doc_files[0], "rb") as zf:
                        xml_content = pyzstd.decompress(zf.read()).decode('utf-8', errors='ignore')
                except: pass
            
            if not xml_content:
                try:
                    with open(p, "rb") as zf:
                        raw_sgml = pyzstd.decompress(zf.read()).decode('utf-8', errors='ignore')
                        xml_match = re.search(r'<XML>(.*?)</XML>', raw_sgml, re.DOTALL | re.IGNORECASE)
                        xml_content = xml_match.group(1).strip() if xml_match else raw_sgml
                except: continue

            try:
                match = re.search(r'(<ownershipDocument.*?</ownershipDocument>)', xml_content, re.DOTALL | re.IGNORECASE)
                if not match: continue
                
                xml_data = match.group(1).strip()
                parser = etree.XMLParser(recover=True, encoding='utf-8')
                root = etree.fromstring(xml_data.encode('utf-8'), parser=parser)
                for el in root.iter():
                    if isinstance(el.tag, str) and '}' in el.tag:
                        el.tag = el.tag.split('}', 1)[1]
                
                # DELETE AND RE-INSERT ALL
                sub.signatures.all().delete()
                sub.footnotes.all().delete()
                sub.non_deriv_transactions.all().delete()
                sub.non_deriv_holdings.all().delete()
                sub.deriv_transactions.all().delete()
                sub.deriv_holdings.all().delete()

                # Signatures
                for sig in root.findall('ownerSignature'):
                    OwnershipSignature.objects.create(
                        submission=sub,
                        signature_name=self.get_tag_text(sig, 'signatureName'),
                        signature_date=self.get_tag_text(sig, 'signatureDate')
                    )

                # Footnotes
                fn_node = root.find('footnotes')
                if fn_node is not None:
                    for fn in fn_node.findall('footnote'):
                        OwnershipFootnote.objects.create(
                            submission=sub,
                            footnote_id=fn.get('id', ''),
                            footnote_text=fn.text or ""
                        )

                # Table I
                nd_table = root.find('nonDerivativeTable')
                if nd_table is not None:
                    for i, item in enumerate(nd_table):
                        post = item.find('postTransactionAmounts')
                        nature = item.find('ownershipNature')
                        if item.tag == 'nonDerivativeTransaction':
                            amt = item.find('transactionAmounts')
                            coding = item.find('transactionCoding')
                            OwnershipNonDerivTransaction.objects.create(
                                submission=sub,
                                sequence_key=i,
                                rptowner_cik=sub.reporting_owners.first().rptowner_cik if sub.reporting_owners.exists() else 0,
                                security_title=self.get_tag_text(item, 'securityTitle'),
                                security_title_fn=self.resolve_fn(item.find('securityTitle')),
                                transaction_date=self.get_tag_text(item, 'transactionDate') or None,
                                transaction_date_fn=self.resolve_fn(item.find('transactionDate')),
                                transaction_code=self.get_tag_text(coding, 'transactionCode'),
                                transaction_code_fn=self.resolve_fn(coding.find('transactionCode') if coding is not None else None),
                                transaction_shares=float(self.get_tag_text(amt, 'transactionShares') or 0),
                                transaction_shares_fn=self.resolve_fn(amt.find('transactionShares') if amt is not None else None),
                                transaction_price_per_share=float(self.get_tag_text(amt, 'transactionPricePerShare') or 0),
                                transaction_price_per_share_fn=self.resolve_fn(amt.find('transactionPricePerShare') if amt is not None else None),
                                transaction_acquired_disposed_code=self.get_tag_text(amt, 'transactionAcquiredDisposedCode'),
                                transaction_acquired_disposed_code_fn=self.resolve_fn(amt.find('transactionAcquiredDisposedCode') if amt is not None else None),
                                shares_owned_following_transaction=float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction') or 0),
                                shares_owned_following_transaction_fn=self.resolve_fn(post.find('sharesOwnedFollowingTransaction') if post is not None else None),
                                direct_or_indirect_ownership=self.get_tag_text(nature, 'directOrIndirectOwnership'),
                                direct_or_indirect_ownership_fn=self.resolve_fn(nature.find('directOrIndirectOwnership') if nature is not None else None),
                                nature_of_ownership=self.get_tag_text(nature, 'natureOfOwnership'),
                                nature_of_ownership_fn=self.resolve_fn(nature.find('natureOfOwnership') if nature is not None else None)
                            )
                        else:
                            OwnershipNonDerivHolding.objects.create(
                                submission=sub,
                                sequence_key=i,
                                rptowner_cik=sub.reporting_owners.first().rptowner_cik if sub.reporting_owners.exists() else 0,
                                security_title=self.get_tag_text(item, 'securityTitle'),
                                security_title_fn=self.resolve_fn(item.find('securityTitle')),
                                shares_owned_following_transaction=float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction') or 0),
                                shares_owned_following_transaction_fn=self.resolve_fn(post.find('sharesOwnedFollowingTransaction') if post is not None else None),
                                direct_or_indirect_ownership=self.get_tag_text(nature, 'directOrIndirectOwnership'),
                                direct_or_indirect_ownership_fn=self.resolve_fn(nature.find('directOrIndirectOwnership') if nature is not None else None),
                                nature_of_ownership=self.get_tag_text(nature, 'natureOfOwnership'),
                                nature_of_ownership_fn=self.resolve_fn(nature.find('natureOfOwnership') if nature is not None else None)
                            )

                # Table II
                d_table = root.find('derivativeTable')
                if d_table is not None:
                    for i, item in enumerate(d_table):
                        post = item.find('postTransactionAmounts')
                        nature = item.find('ownershipNature')
                        under = item.find('underlyingSecurity')
                        if item.tag == 'derivativeTransaction':
                            amt = item.find('transactionAmounts')
                            coding = item.find('transactionCoding')
                            OwnershipDerivTransaction.objects.create(
                                submission=sub,
                                sequence_key=i,
                                security_title=self.get_tag_text(item, 'securityTitle'),
                                security_title_fn=self.resolve_fn(item.find('securityTitle')),
                                conversion_or_exercise_price=float(self.get_tag_text(item, 'conversionOrExercisePrice') or 0),
                                conversion_or_exercise_price_fn=self.resolve_fn(item.find('conversionOrExercisePrice')),
                                transaction_date=self.get_tag_text(item, 'transactionDate') or None,
                                transaction_date_fn=self.resolve_fn(item.find('transactionDate')),
                                transaction_code=self.get_tag_text(coding, 'transactionCode'),
                                exercise_date=self.get_tag_text(item, 'exerciseDate') or None,
                                exercise_date_fn=self.resolve_fn(item.find('exerciseDate')),
                                expiration_date=self.get_tag_text(item, 'expirationDate') or None,
                                expiration_date_fn=self.resolve_fn(item.find('expirationDate')),
                                underlying_security_title=self.get_tag_text(under, 'underlyingSecurityTitle'),
                                underlying_security_title_fn=self.resolve_fn(under.find('underlyingSecurityTitle') if under is not None else None),
                                underlying_security_shares=float(self.get_tag_text(under, 'underlyingSecurityShares') or 0),
                                underlying_security_shares_fn=self.resolve_fn(under.find('underlyingSecurityShares') if under is not None else None),
                                transaction_acquired_disposed_code=self.get_tag_text(amt, 'transactionAcquiredDisposedCode'),
                                shares_owned_following_transaction=float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction') or 0),
                                shares_owned_following_transaction_fn=self.resolve_fn(post.find('sharesOwnedFollowingTransaction') if post is not None else None),
                                direct_or_indirect_ownership=self.get_tag_text(nature, 'directOrIndirectOwnership'),
                                direct_or_indirect_ownership_fn=self.resolve_fn(nature.find('directOrIndirectOwnership') if nature is not None else None),
                                nature_of_ownership=self.get_tag_text(nature, 'natureOfOwnership'),
                                nature_of_ownership_fn=self.resolve_fn(nature.find('natureOfOwnership') if nature is not None else None)
                            )
                        else:
                            OwnershipDerivHolding.objects.create(
                                submission=sub,
                                sequence_key=i,
                                security_title=self.get_tag_text(item, 'securityTitle'),
                                security_title_fn=self.resolve_fn(item.find('securityTitle')),
                                conversion_or_exercise_price=float(self.get_tag_text(item, 'conversionOrExercisePrice') or 0),
                                conversion_or_exercise_price_fn=self.resolve_fn(item.find('conversionOrExercisePrice')),
                                exercise_date=self.get_tag_text(item, 'exerciseDate') or None,
                                exercise_date_fn=self.resolve_fn(item.find('exerciseDate')),
                                expiration_date=self.get_tag_text(item, 'expirationDate') or None,
                                expiration_date_fn=self.resolve_fn(item.find('expirationDate')),
                                underlying_security_title=self.get_tag_text(under, 'underlyingSecurityTitle'),
                                underlying_security_title_fn=self.resolve_fn(under.find('underlyingSecurityTitle') if under is not None else None),
                                underlying_security_shares=float(self.get_tag_text(under, 'underlyingSecurityShares') or 0),
                                underlying_security_shares_fn=self.resolve_fn(under.find('underlyingSecurityShares') if under is not None else None),
                                shares_owned_following_transaction=float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction') or 0),
                                shares_owned_following_transaction_fn=self.resolve_fn(post.find('sharesOwnedFollowingTransaction') if post is not None else None),
                                direct_or_indirect_ownership=self.get_tag_text(nature, 'directOrIndirectOwnership'),
                                direct_or_indirect_ownership_fn=self.resolve_fn(nature.find('directOrIndirectOwnership') if nature is not None else None),
                                nature_of_ownership=self.get_tag_text(nature, 'natureOfOwnership'),
                                nature_of_ownership_fn=self.resolve_fn(nature.find('natureOfOwnership') if nature is not None else None)
                            )

                fixed_count += 1
            except Exception as e:
                self.stderr.write(f"Error processing {acc}: {e}")

            processed += 1
            if processed % 100 == 0:
                self.stdout.write(f"Processed {processed}/{total}...")

        self.stdout.write(f"Finished. Repaired {fixed_count} filings with high-fidelity footnotes.")

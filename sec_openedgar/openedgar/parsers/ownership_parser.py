import logging
from datetime import date
import re
from lxml import etree
from openedgar.models import (
    OwnershipSubmission,
    OwnershipReportingOwner,
    OwnershipNonDerivTransaction,
    OwnershipNonDerivHolding,
    OwnershipDerivTransaction,
    OwnershipDerivHolding,
    OwnershipFootnote,
    OwnershipSignature,
)
from openedgar.parsers.ownership import OwnershipParser as CanonicalOwnershipParser

logger = logging.getLogger(__name__)

class OwnershipParser(CanonicalOwnershipParser):
    """
    High-fidelity Parser for SEC Ownership Forms (3, 4, 5 and Amendments).
    Adheres to technical specification v5.5.
    Responsible for normalizing XML data into the Django database.
    """

    def _parse_xml_root(self, xml_content):
        """Parse ownership XML with namespace stripping and recovery."""
        match = re.search(r'(<ownershipDocument.*?</ownershipDocument>)', xml_content, re.DOTALL | re.IGNORECASE)
        if match:
            xml_data = match.group(1).strip()
        else:
            match = re.search(r'(<\?xml.*)', xml_content, re.DOTALL | re.IGNORECASE)
            xml_data = match.group(1).strip() if match else xml_content

        parser = etree.XMLParser(recover=True, encoding='utf-8')
        root = etree.fromstring(xml_data.encode('utf-8'), parser=parser)
        for el in root.iter():
            if isinstance(el.tag, str) and '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]
        return root

    def _to_bool(self, value: str) -> bool:
        return str(value).strip().lower() in ('1', 'true', 'yes', 'y')

    def _to_date(self, value: str):
        """Return a date object for YYYY-MM-DD values, else None."""
        v = str(value or '').strip()
        if not v:
            return None
        try:
            return date.fromisoformat(v)
        except ValueError:
            return None

    def _to_float(self, value):
        v = str(value or '').strip().replace(',', '')
        if not v:
            return None
        try:
            return float(v)
        except ValueError:
            return None

    def _to_int(self, value):
        v = str(value or '').strip()
        if not v:
            return None
        try:
            return int(v)
        except ValueError:
            return None

    def _fn_refs(self, element):
        if element is None:
            return ''
        ids = []
        for fn in element.findall('.//footnoteId'):
            fid = (fn.get('id') or '').upper().replace('F', '').strip()
            if fid and fid not in ids:
                ids.append(fid)
        return ','.join(ids)

    def find_ownership_xml(self, path):
        """Core utility to find the primary XML document in a filing archive."""
        import tarfile, io, pyzstd
        if str(path).endswith(".zst"):
            with open(path, "rb") as f:
                decompressed = pyzstd.decompress(f.read())
                if b"<ownershipDocument" in decompressed:
                    return decompressed.decode('utf-8', errors='ignore'), str(path)
                try:
                    with tarfile.open(fileobj=io.BytesIO(decompressed), mode="r") as tar:
                        for member in tar.getmembers():
                            if member.name.endswith(".xml"):
                                f = tar.extractfile(member)
                                content = f.read().decode('utf-8', errors='ignore')
                                if "<ownershipDocument" in content:
                                    return content, member.name
                except: pass
        return None, None

    def normalize(self, xml_content, filing_obj):
        """Normalizes the raw XML into the database models."""
        try:
            root = self._parse_xml_root(xml_content)
        except Exception as e:
            logger.error(f"Error parsing XML for {filing_obj}: {e}")
            return None

        issuer_node = root.find('issuer')
        issuer_cik = self.get_tag_text(issuer_node, 'issuerCik')
        issuer_cik = issuer_cik.zfill(10) if issuer_cik else getattr(filing_obj, 'cik_id', None)

        # Create the primary submission record
        sub, created = OwnershipSubmission.objects.update_or_create(
            accession_number=filing_obj,
            defaults={
                'filing_date': getattr(filing_obj, 'date_filed', None),
                'issuer_cik_id': issuer_cik,
                'issuer_name': self.get_tag_text(issuer_node, 'issuerName'),
                'issuer_trading_symbol': self.get_tag_text(issuer_node, 'issuerTradingSymbol'),
                'issuer_foreign_trading_symbol': self.get_tag_text(issuer_node, 'issuerForeignTradingSymbol'),
                'form_type': self.get_tag_text(root, 'documentType') or getattr(filing_obj.form_type, 'form_type', ''),
                'schema_version': self.get_tag_text(root, 'schemaVersion'),
                'period_of_report': self._to_date(self.get_tag_text(root, 'periodOfReport')),
                'remarks': self.get_tag_text(root, 'remarks'),
                'date_of_original_submission': self._to_date(self.get_tag_text(root, 'dateOfOriginalSubmission')),
                'not_subject_to_section_16': self._to_bool(self.get_tag_text(root, 'notSubjectToSection16')),
                'is_rule_10b5_1_plan': self._to_bool(self.get_tag_text(root, 'aff10b5One')),
                'no_securities_owned': self._to_bool(self.get_tag_text(root, 'noSecuritiesOwned')),
            }
        )

        # Replace child records on each normalize to keep rows consistent with source XML.
        sub.reporting_owners.all().delete()
        sub.non_deriv_transactions.all().delete()
        sub.non_deriv_holdings.all().delete()
        sub.deriv_transactions.all().delete()
        sub.deriv_holdings.all().delete()
        sub.footnotes.all().delete()
        sub.signatures.all().delete()

        owner_ciks = []
        for owner in root.findall('reportingOwner'):
            id_info = owner.find('reportingOwnerId')
            rel = owner.find('reportingOwnerRelationship')
            addr = owner.find('reportingOwnerAddress')
            cik = self._to_int(self.get_tag_text(id_info, 'rptOwnerCik'))
            if cik:
                owner_ciks.append(cik)
            OwnershipReportingOwner.objects.create(
                submission=sub,
                rptowner_cik=cik,
                rptowner_ccc=self.get_tag_text(id_info, 'rptOwnerCcc') or None,
                rptowner_name=self.get_tag_text(id_info, 'rptOwnerName'),
                is_director=self._to_bool(self.get_tag_text(rel, 'isDirector')),
                is_officer=self._to_bool(self.get_tag_text(rel, 'isOfficer')),
                is_10pctowner=self._to_bool(self.get_tag_text(rel, 'isTenPercentOwner')),
                is_other=self._to_bool(self.get_tag_text(rel, 'isOther')),
                officer_title=self.get_tag_text(rel, 'officerTitle') or None,
                rptowner_street1=self.get_tag_text(addr, 'rptOwnerStreet1') or None,
                rptowner_street2=self.get_tag_text(addr, 'rptOwnerStreet2') or None,
                rptowner_city=self.get_tag_text(addr, 'rptOwnerCity') or None,
                rptowner_state=self.get_tag_text(addr, 'rptOwnerState') or None,
                rptowner_zip=self.get_tag_text(addr, 'rptOwnerZipCode') or None,
                rptowner_non_us_address_flag=self._to_bool(self.get_tag_text(addr, 'rptOwnerNonUSAddressFlag')),
                rptowner_non_us_state_territory=self.get_tag_text(addr, 'rptOwnerNonUSStateTerritory') or None,
                rptowner_country=self.get_tag_text(addr, 'rptOwnerCountry') or None,
                rptowner_state_description=self.get_tag_text(addr, 'rptOwnerStateDescription') or None,
                rptowner_good_address=self._to_bool(self.get_tag_text(addr, 'rptOwnerGoodAddress')),
                is_director_nominee=self._to_bool(self.get_tag_text(rel, 'isDirectorNominee')),
                other_text=self.get_tag_text(rel, 'otherText') or None,
            )

        default_rpt_cik = owner_ciks[0] if owner_ciks else 0

        non_deriv_table = root.find('nonDerivativeTable')
        if non_deriv_table is not None:
            seq = 1
            for item in list(non_deriv_table):
                if item.tag == 'nonDerivativeTransaction':
                    coding = item.find('transactionCoding')
                    amounts = item.find('transactionAmounts')
                    post = item.find('postTransactionAmounts')
                    own = item.find('ownershipNature')
                    OwnershipNonDerivTransaction.objects.create(
                        submission=sub,
                        sequence_key=seq,
                        rptowner_cik=default_rpt_cik,
                        security_title=self.get_tag_text(item, 'securityTitle'),
                        transaction_date=self._to_date(self.get_tag_text(item, 'transactionDate')),
                        transaction_code=self.get_tag_text(coding, 'transactionCode') or None,
                        transaction_shares=self._to_float(self.get_tag_text(amounts, 'transactionShares')),
                        transaction_price_per_share=self._to_float(self.get_tag_text(amounts, 'transactionPricePerShare')),
                        transaction_total_value=self._to_float(self.get_tag_text(amounts, 'transactionTotalValue')),
                        transaction_acquired_disposed_code=self.get_tag_text(amounts, 'transactionAcquiredDisposedCode') or None,
                        shares_owned_following_transaction=self._to_float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction')),
                        value_owned_following_transaction=self._to_float(self.get_tag_text(post, 'valueOwnedFollowingTransaction')),
                        direct_or_indirect_ownership=self.get_tag_text(own, 'directOrIndirectOwnership') or None,
                        nature_of_ownership=self.get_tag_text(own, 'natureOfOwnership') or None,
                        deemed_execution_date=self._to_date(self.get_tag_text(item, 'deemedExecutionDate')),
                        transaction_form_type=self.get_tag_text(coding, 'transactionFormType') or None,
                        equity_swap_involved=self._to_bool(self.get_tag_text(coding, 'equitySwapInvolved')),
                        transaction_timeliness=self.get_tag_text(coding, 'transactionTimeliness') or None,
                        security_title_fn=self._fn_refs(item.find('securityTitle')),
                        transaction_date_fn=self._fn_refs(item.find('transactionDate')),
                        deemed_execution_date_fn=self._fn_refs(item.find('deemedExecutionDate')),
                        transaction_code_fn=self._fn_refs(coding.find('transactionCode') if coding is not None else None),
                        transaction_timeliness_fn=self._fn_refs(coding.find('transactionTimeliness') if coding is not None else None),
                        transaction_shares_fn=self._fn_refs(amounts.find('transactionShares') if amounts is not None else None),
                        transaction_price_per_share_fn=self._fn_refs(amounts.find('transactionPricePerShare') if amounts is not None else None),
                        transaction_total_value_fn=self._fn_refs(amounts.find('transactionTotalValue') if amounts is not None else None),
                        transaction_acquired_disposed_code_fn=self._fn_refs(amounts.find('transactionAcquiredDisposedCode') if amounts is not None else None),
                        shares_owned_following_transaction_fn=self._fn_refs(post.find('sharesOwnedFollowingTransaction') if post is not None else None),
                        value_owned_following_transaction_fn=self._fn_refs(post.find('valueOwnedFollowingTransaction') if post is not None else None),
                        value_owned_following_fn=self._fn_refs(post.find('valueOwnedFollowingTransaction') if post is not None else None),
                        direct_or_indirect_ownership_fn=self._fn_refs(own.find('directOrIndirectOwnership') if own is not None else None),
                        nature_of_ownership_fn=self._fn_refs(own.find('natureOfOwnership') if own is not None else None),
                    )
                    seq += 1
                elif item.tag == 'nonDerivativeHolding':
                    post = item.find('postTransactionAmounts')
                    own = item.find('ownershipNature')
                    coding = item.find('transactionCoding')
                    OwnershipNonDerivHolding.objects.create(
                        submission=sub,
                        sequence_key=seq,
                        rptowner_cik=default_rpt_cik,
                        security_title=self.get_tag_text(item, 'securityTitle'),
                        shares_owned_following_transaction=self._to_float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction')),
                        value_owned_following_transaction=self._to_float(self.get_tag_text(post, 'valueOwnedFollowingTransaction')),
                        direct_or_indirect_ownership=self.get_tag_text(own, 'directOrIndirectOwnership') or None,
                        nature_of_ownership=self.get_tag_text(own, 'natureOfOwnership') or None,
                        transaction_form_type=self.get_tag_text(coding, 'transactionFormType') or None,
                        security_title_fn=self._fn_refs(item.find('securityTitle')),
                        shares_owned_following_transaction_fn=self._fn_refs(post.find('sharesOwnedFollowingTransaction') if post is not None else None),
                        value_owned_following_transaction_fn=self._fn_refs(post.find('valueOwnedFollowingTransaction') if post is not None else None),
                        value_owned_following_fn=self._fn_refs(post.find('valueOwnedFollowingTransaction') if post is not None else None),
                        direct_or_indirect_ownership_fn=self._fn_refs(own.find('directOrIndirectOwnership') if own is not None else None),
                        nature_of_ownership_fn=self._fn_refs(own.find('natureOfOwnership') if own is not None else None),
                    )
                    seq += 1

        deriv_table = root.find('derivativeTable')
        if deriv_table is not None:
            seq = 1
            for item in list(deriv_table):
                if item.tag == 'derivativeTransaction':
                    coding = item.find('transactionCoding')
                    amounts = item.find('transactionAmounts')
                    post = item.find('postTransactionAmounts')
                    own = item.find('ownershipNature')
                    under = item.find('underlyingSecurity')
                    OwnershipDerivTransaction.objects.create(
                        submission=sub,
                        sequence_key=seq,
                        security_title=self.get_tag_text(item, 'securityTitle'),
                        conversion_or_exercise_price=self._to_float(self.get_tag_text(item, 'conversionOrExercisePrice')),
                        transaction_date=self._to_date(self.get_tag_text(item, 'transactionDate')),
                        transaction_code=self.get_tag_text(coding, 'transactionCode') or None,
                        exercise_date=self._to_date(self.get_tag_text(item, 'exerciseDate')),
                        expiration_date=self._to_date(self.get_tag_text(item, 'expirationDate')),
                        underlying_security_title=self.get_tag_text(under, 'underlyingSecurityTitle') or None,
                        underlying_security_shares=self._to_float(self.get_tag_text(under, 'underlyingSecurityShares')),
                        underlying_security_value=self._to_float(self.get_tag_text(under, 'underlyingSecurityValue')),
                        transaction_acquired_disposed_code=self.get_tag_text(amounts, 'transactionAcquiredDisposedCode') or None,
                        shares_owned_following_transaction=self._to_float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction')),
                        value_owned_following_transaction=self._to_float(self.get_tag_text(post, 'valueOwnedFollowingTransaction')),
                        direct_or_indirect_ownership=self.get_tag_text(own, 'directOrIndirectOwnership') or None,
                        nature_of_ownership=self.get_tag_text(own, 'natureOfOwnership') or None,
                        deemed_execution_date=self._to_date(self.get_tag_text(item, 'deemedExecutionDate')),
                        transaction_form_type=self.get_tag_text(coding, 'transactionFormType') or None,
                        equity_swap_involved=self._to_bool(self.get_tag_text(coding, 'equitySwapInvolved')),
                        transaction_timeliness=self.get_tag_text(coding, 'transactionTimeliness') or None,
                        security_title_fn=self._fn_refs(item.find('securityTitle')),
                        conversion_or_exercise_price_fn=self._fn_refs(item.find('conversionOrExercisePrice')),
                        transaction_date_fn=self._fn_refs(item.find('transactionDate')),
                        deemed_execution_date_fn=self._fn_refs(item.find('deemedExecutionDate')),
                        transaction_code_fn=self._fn_refs(coding.find('transactionCode') if coding is not None else None),
                        transaction_timeliness_fn=self._fn_refs(coding.find('transactionTimeliness') if coding is not None else None),
                        exercise_date_fn=self._fn_refs(item.find('exerciseDate')),
                        expiration_date_fn=self._fn_refs(item.find('expirationDate')),
                        underlying_security_title_fn=self._fn_refs(under.find('underlyingSecurityTitle') if under is not None else None),
                        underlying_security_shares_fn=self._fn_refs(under.find('underlyingSecurityShares') if under is not None else None),
                        underlying_security_value_fn=self._fn_refs(under.find('underlyingSecurityValue') if under is not None else None),
                        transaction_acquired_disposed_code_fn=self._fn_refs(amounts.find('transactionAcquiredDisposedCode') if amounts is not None else None),
                        shares_owned_following_transaction_fn=self._fn_refs(post.find('sharesOwnedFollowingTransaction') if post is not None else None),
                        value_owned_following_transaction_fn=self._fn_refs(post.find('valueOwnedFollowingTransaction') if post is not None else None),
                        value_owned_following_fn=self._fn_refs(post.find('valueOwnedFollowingTransaction') if post is not None else None),
                        direct_or_indirect_ownership_fn=self._fn_refs(own.find('directOrIndirectOwnership') if own is not None else None),
                        nature_of_ownership_fn=self._fn_refs(own.find('natureOfOwnership') if own is not None else None),
                    )
                    seq += 1
                elif item.tag == 'derivativeHolding':
                    post = item.find('postTransactionAmounts')
                    own = item.find('ownershipNature')
                    under = item.find('underlyingSecurity')
                    coding = item.find('transactionCoding')
                    OwnershipDerivHolding.objects.create(
                        submission=sub,
                        sequence_key=seq,
                        security_title=self.get_tag_text(item, 'securityTitle'),
                        conversion_or_exercise_price=self._to_float(self.get_tag_text(item, 'conversionOrExercisePrice')),
                        exercise_date=self._to_date(self.get_tag_text(item, 'exerciseDate')),
                        expiration_date=self._to_date(self.get_tag_text(item, 'expirationDate')),
                        underlying_security_title=self.get_tag_text(under, 'underlyingSecurityTitle') or None,
                        underlying_security_shares=self._to_float(self.get_tag_text(under, 'underlyingSecurityShares')),
                        underlying_security_value=self._to_float(self.get_tag_text(under, 'underlyingSecurityValue')),
                        shares_owned_following_transaction=self._to_float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction')),
                        value_owned_following_transaction=self._to_float(self.get_tag_text(post, 'valueOwnedFollowingTransaction')),
                        direct_or_indirect_ownership=self.get_tag_text(own, 'directOrIndirectOwnership') or None,
                        nature_of_ownership=self.get_tag_text(own, 'natureOfOwnership') or None,
                        transaction_form_type=self.get_tag_text(coding, 'transactionFormType') or None,
                        security_title_fn=self._fn_refs(item.find('securityTitle')),
                        conversion_or_exercise_price_fn=self._fn_refs(item.find('conversionOrExercisePrice')),
                        exercise_date_fn=self._fn_refs(item.find('exerciseDate')),
                        expiration_date_fn=self._fn_refs(item.find('expirationDate')),
                        underlying_security_title_fn=self._fn_refs(under.find('underlyingSecurityTitle') if under is not None else None),
                        underlying_security_shares_fn=self._fn_refs(under.find('underlyingSecurityShares') if under is not None else None),
                        underlying_security_value_fn=self._fn_refs(under.find('underlyingSecurityValue') if under is not None else None),
                        shares_owned_following_transaction_fn=self._fn_refs(post.find('sharesOwnedFollowingTransaction') if post is not None else None),
                        value_owned_following_transaction_fn=self._fn_refs(post.find('valueOwnedFollowingTransaction') if post is not None else None),
                        value_owned_following_fn=self._fn_refs(post.find('valueOwnedFollowingTransaction') if post is not None else None),
                        direct_or_indirect_ownership_fn=self._fn_refs(own.find('directOrIndirectOwnership') if own is not None else None),
                        nature_of_ownership_fn=self._fn_refs(own.find('natureOfOwnership') if own is not None else None),
                    )
                    seq += 1

        fn_node = root.find('footnotes')
        if fn_node is not None:
            for fn in fn_node.findall('footnote'):
                raw_id = (fn.get('id') or '').strip()
                clean_id = raw_id.upper().replace('F', '')
                if clean_id:
                    OwnershipFootnote.objects.create(
                        submission=sub,
                        footnote_id=clean_id,
                        footnote_text=(fn.text or '').strip(),
                    )

        for sig in root.findall('ownerSignature'):
            sig_name = self.get_tag_text(sig, 'signatureName')
            sig_date = self._to_date(self.get_tag_text(sig, 'signatureDate'))
            if sig_name and sig_date:
                OwnershipSignature.objects.create(
                    submission=sub,
                    signature_name=sig_name,
                    signature_date=sig_date,
                )

        return sub

import pathlib
from typing import Dict, Any, List, Union
from .base import BaseFormParser
import logging
import re
from lxml import etree

logger = logging.getLogger(__name__)
NULL_TOKEN = "[NULL]"

class OwnershipParser(BaseFormParser):
    """
    Canonical parser for SEC Ownership Forms (3, 4, 5).
    Single source of truth for synthesis — used by training, evaluation, and production.
    DO NOT maintain a parallel implementation elsewhere (see steering.md).
    """

    @property
    def form_types(self) -> List[str]:
        return ['3', '4', '5', '3/A', '4/A', '5/A']

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
            if clean_id: ids.append(f"[^{clean_id}]")
        return "".join(ids)

    def to_markdown(self, buffer: Union[bytes, str]) -> str:
        content = buffer.decode('utf-8', errors='ignore') if isinstance(buffer, bytes) else buffer
        return self.synthesize(content)

    def synthesize(self, xml_content: str, accession: str = None) -> str:
        """Canonical synthesis engine for SEC Ownership filings. Full DB field parity."""
        try:
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
        except Exception as e:
            return f"Error parsing XML: {e}"

        md = []
        doc_type = self.get_tag_text(root, 'documentType')
        schema_version = self.get_tag_text(root, 'schemaVersion')
        period = self.get_tag_text(root, 'periodOfReport')
        issuer_node = root.find('issuer')
        cik = self.get_tag_text(issuer_node, 'issuerCik')
        name = self.get_tag_text(issuer_node, 'issuerName')
        symbol = self.get_tag_text(issuer_node, 'issuerTradingSymbol')
        foreign_symbol = self.get_tag_text(issuer_node, 'issuerForeignTradingSymbol')
        remarks = self.get_tag_text(root, 'remarks')
        not_subject_to_16 = self.get_tag_text(root, 'notSubjectToSection16') in ('1', 'true')
        is_10b5_plan = self.get_tag_text(root, 'aff10b5One') in ('1', 'true')
        no_securities = self.get_tag_text(root, 'noSecuritiesOwned') in ('1', 'true')
        date_of_orig_sub = self.get_tag_text(root, 'dateOfOriginalSubmission')

        # YAML Frontmatter
        md.append("---")
        md.append(f"title: SEC {doc_type} Filing - {name}")
        md.append(f"accession_number: {accession or 'N/A'}")
        md.append(f"form_type: {doc_type}")
        if schema_version:
            md.append(f"schema_version: {schema_version}")
        md.append(f"issuer_cik: {cik}")
        md.append(f"issuer_name: {name}")
        md.append(f"issuer_trading_symbol: {symbol}")
        if foreign_symbol:
            md.append(f"issuer_foreign_trading_symbol: {foreign_symbol}")
        md.append(f"period_of_report: {period}")
        md.append(f"not_subject_to_section_16: {str(not_subject_to_16).lower()}")
        md.append(f"rule_10b5_1_plan: {str(is_10b5_plan).lower()}")
        md.append(f"no_securities_owned: {str(no_securities).lower()}")
        if date_of_orig_sub:
            md.append(f"date_of_original_submission: {date_of_orig_sub}")
        md.append("---")
        md.append(f"\n# SEC {doc_type} Filing - {name}")
        if date_of_orig_sub:
            md.append(f"* **Date of Original Submission:** {date_of_orig_sub}")

        md.append("\n## Reporting Owners")
        reporting_owners = root.findall('reportingOwner')
        if not reporting_owners:
            md.append(NULL_TOKEN)
        for owner in reporting_owners:
            id_info = owner.find('reportingOwnerId')
            rel_info = owner.find('reportingOwnerRelationship')
            addr_info = owner.find('reportingOwnerAddress')
            r_name, r_name_fn = self.get_tag_text(id_info, 'rptOwnerName'), self.resolve_fn(id_info.find('rptOwnerName') if id_info is not None else None)
            md.append(f"### Reporting Person: {r_name} {r_name_fn}".strip())
            md.append(f"* **CIK:** {self.get_tag_text(id_info, 'rptOwnerCik')}")
            rpt_ccc = self.get_tag_text(id_info, 'rptOwnerCcc')
            if rpt_ccc:
                md.append(f"* **CCC:** {rpt_ccc}")

            addr_lines = []
            street1 = self.get_tag_text(addr_info, 'rptOwnerStreet1')
            street2 = self.get_tag_text(addr_info, 'rptOwnerStreet2')
            city = self.get_tag_text(addr_info, 'rptOwnerCity')
            state = self.get_tag_text(addr_info, 'rptOwnerState')
            zip_code = self.get_tag_text(addr_info, 'rptOwnerZipCode')
            country = self.get_tag_text(addr_info, 'rptOwnerCountry')
            non_us_terr = self.get_tag_text(addr_info, 'rptOwnerNonUSStateTerritory')
            if street1:
                addr_lines.append(street1)
            if street2:
                addr_lines.append(street2)
            city_state = ", ".join(filter(None, [city, state or non_us_terr, zip_code]))
            if city_state:
                addr_lines.append(city_state)
            if country:
                addr_lines.append(country)
            if addr_lines:
                md.append("* **Address:**")
                for line in addr_lines:
                    md.append(f"  - {line}")

            roles = []
            if self.get_tag_text(rel_info, 'isDirector') in ('1', 'true'):
                roles.append(f"Director {self.resolve_fn(rel_info.find('isDirector') if rel_info is not None else None)}".strip())
            if self.get_tag_text(rel_info, 'isOfficer') in ('1', 'true'):
                title, title_fn = self.get_tag_text(rel_info, 'officerTitle'), self.resolve_fn(rel_info.find('officerTitle') if rel_info is not None else None)
                roles.append(f"Officer: {title} {title_fn}".strip())
            if self.get_tag_text(rel_info, 'isTenPercentOwner') in ('1', 'true'):
                roles.append(f"10% Owner {self.resolve_fn(rel_info.find('isTenPercentOwner') if rel_info is not None else None)}".strip())
            if self.get_tag_text(rel_info, 'isOther') in ('1', 'true'):
                text, text_fn = self.get_tag_text(rel_info, 'otherText'), self.resolve_fn(rel_info.find('otherText') if rel_info is not None else None)
                roles.append(f"Other: {text} {text_fn}".strip())
            md.append(f"* **Position(s):** {', '.join(roles) if roles else 'No roles specified'}")

        # Table I — split into explicit transaction/holding tables.
        non_deriv_table = root.find('nonDerivativeTable')
        non_deriv_items = list(non_deriv_table) if non_deriv_table is not None else []
        non_deriv_tx = [item for item in non_deriv_items if item.tag == 'nonDerivativeTransaction']
        non_deriv_hold = [item for item in non_deriv_items if item.tag != 'nonDerivativeTransaction']

        if non_deriv_tx or non_deriv_hold:
            md.append("\n## Table I - Non-Derivative Securities")
            if non_deriv_tx:
                md.append("### Transactions")
                md.append("| Security | Date | Deemed Exec | Code | Form Type | A/D | Equity Swap | Timeliness | Shares | Price | Total Value | Owned After | Value After | Nature |")
                md.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
                for item in non_deriv_tx:
                    title, title_fn = self.get_tag_text(item, 'securityTitle'), self.resolve_fn(item.find('securityTitle'))
                    post_node = item.find('postTransactionAmounts')
                    owned, owned_fn = self.get_tag_text(post_node, 'sharesOwnedFollowingTransaction'), self.resolve_fn(post_node.find('sharesOwnedFollowingTransaction') if post_node is not None else None)
                    value_owned, value_owned_fn = self.get_tag_text(post_node, 'valueOwnedFollowingTransaction'), self.resolve_fn(post_node.find('valueOwnedFollowingTransaction') if post_node is not None else None)
                    own_nature = item.find('ownershipNature')
                    direct, direct_fn = self.get_tag_text(own_nature, 'directOrIndirectOwnership'), self.resolve_fn(own_nature.find('directOrIndirectOwnership') if own_nature is not None else None)
                    nature_text, nature_fn = self.get_tag_text(own_nature, 'natureOfOwnership'), self.resolve_fn(own_nature.find('natureOfOwnership') if own_nature is not None else None)
                    nature_cell = f"{direct} ({nature_text}) {direct_fn} {nature_fn}".strip().replace(" ()", "")
                    date, date_fn = self.get_tag_text(item, 'transactionDate'), self.resolve_fn(item.find('transactionDate'))
                    deemed, deemed_fn = self.get_tag_text(item, 'deemedExecutionDate'), self.resolve_fn(item.find('deemedExecutionDate'))
                    coding = item.find('transactionCoding')
                    code, code_fn = self.get_tag_text(coding, 'transactionCode'), self.resolve_fn(coding.find('transactionCode') if coding is not None else None)
                    form_type_val = self.get_tag_text(coding, 'transactionFormType') if coding is not None else ""
                    equity_swap = self.get_tag_text(coding, 'equitySwapInvolved') if coding is not None else ""
                    timeliness = self.get_tag_text(coding, 'transactionTimeliness') if coding is not None else ""
                    amt_node = item.find('transactionAmounts')
                    shares, shares_fn = self.get_tag_text(amt_node, 'transactionShares'), self.resolve_fn(amt_node.find('transactionShares') if amt_node is not None else None)
                    price, price_fn = self.get_tag_text(amt_node, 'transactionPricePerShare'), self.resolve_fn(amt_node.find('transactionPricePerShare') if amt_node is not None else None)
                    total_value, total_value_fn = self.get_tag_text(amt_node, 'transactionTotalValue'), self.resolve_fn(amt_node.find('transactionTotalValue') if amt_node is not None else None)
                    ad_node = amt_node.find('transactionAcquiredDisposedCode') if amt_node is not None else None
                    ad, ad_fn = self.get_tag_text(amt_node, 'transactionAcquiredDisposedCode'), self.resolve_fn(ad_node)
                    md.append(f"| {title} {title_fn}".strip() + f" | {date} {date_fn}".strip() + f" | {deemed} {deemed_fn}".strip() + f" | {code} {code_fn}".strip() + f" | {form_type_val}" + f" | {ad} {ad_fn}".strip() + f" | {equity_swap}" + f" | {timeliness}" + f" | {shares} {shares_fn}".strip() + f" | {price} {price_fn}".strip() + f" | {total_value} {total_value_fn}".strip() + f" | {owned} {owned_fn}".strip() + f" | {value_owned} {value_owned_fn}".strip() + f" | {nature_cell} |")

            if non_deriv_hold:
                md.append("### Holdings")
                md.append("| Security | Owned After | Value After | Nature | Summary |")
                md.append("| :--- | :--- | :--- | :--- | :--- |")
                for item in non_deriv_hold:
                    title, title_fn = self.get_tag_text(item, 'securityTitle'), self.resolve_fn(item.find('securityTitle'))
                    post_node = item.find('postTransactionAmounts')
                    owned, owned_fn = self.get_tag_text(post_node, 'sharesOwnedFollowingTransaction'), self.resolve_fn(post_node.find('sharesOwnedFollowingTransaction') if post_node is not None else None)
                    value_owned, value_owned_fn = self.get_tag_text(post_node, 'valueOwnedFollowingTransaction'), self.resolve_fn(post_node.find('valueOwnedFollowingTransaction') if post_node is not None else None)
                    own_nature = item.find('ownershipNature')
                    direct, direct_fn = self.get_tag_text(own_nature, 'directOrIndirectOwnership'), self.resolve_fn(own_nature.find('directOrIndirectOwnership') if own_nature is not None else None)
                    nature_text, nature_fn = self.get_tag_text(own_nature, 'natureOfOwnership'), self.resolve_fn(own_nature.find('natureOfOwnership') if own_nature is not None else None)
                    nature_cell = f"{direct} ({nature_text}) {direct_fn} {nature_fn}".strip().replace(" ()", "")
                    md.append(f"| {title} {title_fn}".strip() + f" | {owned} {owned_fn}".strip() + f" | {value_owned} {value_owned_fn}".strip() + f" | {nature_cell} | [NONE] |")

        # Table II — split into explicit transaction/holding tables.
        deriv_table = root.find('derivativeTable')
        deriv_items = list(deriv_table) if deriv_table is not None else []
        deriv_tx = [item for item in deriv_items if item.tag == 'derivativeTransaction']
        deriv_hold = [item for item in deriv_items if item.tag != 'derivativeTransaction']

        if deriv_tx or deriv_hold:
            md.append("\n## Table II - Derivative Securities")
            if deriv_tx:
                md.append("### Transactions")
                md.append("| Security | Conv/Exer Price | Date | Code | A/D | Shares | Exercise Date | Expiry Date | Underlying Title | Underlying Shares | Underlying Value | Owned After | Value After | Nature | Summary |")
                md.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | : :--- | :--- | :--- | :--- | :--- | :--- |")
                for item in deriv_tx:
                    title, title_fn = self.get_tag_text(item, 'securityTitle'), self.resolve_fn(item.find('securityTitle'))
                    post_node = item.find('postTransactionAmounts')
                    owned, owned_fn = self.get_tag_text(post_node, 'sharesOwnedFollowingTransaction'), self.resolve_fn(post_node.find('sharesOwnedFollowingTransaction') if post_node is not None else None)
                    value_owned, value_owned_fn = self.get_tag_text(post_node, 'valueOwnedFollowingTransaction'), self.resolve_fn(post_node.find('valueOwnedFollowingTransaction') if post_node is not None else None)
                    own_nature = item.find('ownershipNature')
                    direct, direct_fn = self.get_tag_text(own_nature, 'directOrIndirectOwnership'), self.resolve_fn(own_nature.find('directOrIndirectOwnership') if own_nature is not None else None)
                    nature_text, nature_fn = self.get_tag_text(own_nature, 'natureOfOwnership'), self.resolve_fn(own_nature.find('natureOfOwnership') if own_nature is not None else None)
                    nature_cell = f"{direct} ({nature_text}) {direct_fn} {nature_fn}".strip().replace(" ()", "")
                    price, price_fn = self.get_tag_text(item, 'conversionOrExercisePrice'), self.resolve_fn(item.find('conversionOrExercisePrice'))
                    exer, exer_fn = self.get_tag_text(item, 'exerciseDate'), self.resolve_fn(item.find('exerciseDate'))
                    expr, expr_fn = self.get_tag_text(item, 'expirationDate'), self.resolve_fn(item.find('expirationDate'))
                    under_node = item.find('underlyingSecurity')
                    u_title, u_title_fn = self.get_tag_text(under_node, 'underlyingSecurityTitle'), self.resolve_fn(under_node.find('underlyingSecurityTitle') if under_node is not None else None)
                    u_shares, u_shares_fn = self.get_tag_text(under_node, 'underlyingSecurityShares'), self.resolve_fn(under_node.find('underlyingSecurityShares') if under_node is not None else None)
                    u_value, u_value_fn = self.get_tag_text(under_node, 'underlyingSecurityValue'), self.resolve_fn(under_node.find('underlyingSecurityValue') if under_node is not None else None)
                    date, date_fn = self.get_tag_text(item, 'transactionDate'), self.resolve_fn(item.find('transactionDate'))
                    coding = item.find('transactionCoding')
                    code, code_fn = self.get_tag_text(coding, 'transactionCode'), self.resolve_fn(coding.find('transactionCode') if coding is not None else None)
                    amt_node = item.find('transactionAmounts')
                    shares, shares_fn = self.get_tag_text(amt_node, 'transactionShares'), self.resolve_fn(amt_node.find('transactionShares') if amt_node is not None else None)
                    total_val, total_val_fn = self.get_tag_text(amt_node, 'transactionTotalValue'), self.resolve_fn(amt_node.find('transactionTotalValue') if amt_node is not None else None)
                    shares_cell = f"{shares or total_val} {shares_fn or total_val_fn}".strip()
                    ad_node = amt_node.find('transactionAcquiredDisposedCode') if amt_node is not None else None
                    ad, ad_fn = self.get_tag_text(amt_node, 'transactionAcquiredDisposedCode'), self.resolve_fn(ad_node)
                    md.append(f"| {title} {title_fn}".strip() + f" | {price} {price_fn}".strip() + f" | {date} {date_fn}".strip() + f" | {code} {code_fn}".strip() + f" | {ad} {ad_fn}".strip() + f" | {shares_cell}" + f" | {exer} {exer_fn}".strip() + f" | {expr} {expr_fn}".strip() + f" | {u_title} {u_title_fn}".strip() + f" | {u_shares} {u_shares_fn}".strip() + f" | {u_value} {u_value_fn}".strip() + f" | {owned} {owned_fn}".strip() + f" | {value_owned} {value_owned_fn}".strip() + f" | {nature_cell} | [NONE] |")

            if deriv_hold:
                md.append("### Holdings")
                md.append("| Security | Conv/Exer Price | Exercise Date | Expiry Date | Underlying Title | Underlying Shares | Underlying Value | Owned After | Value After | Nature | Summary |")
                md.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
                for item in deriv_hold:
                    title, title_fn = self.get_tag_text(item, 'securityTitle'), self.resolve_fn(item.find('securityTitle'))
                    post_node = item.find('postTransactionAmounts')
                    owned, owned_fn = self.get_tag_text(post_node, 'sharesOwnedFollowingTransaction'), self.resolve_fn(post_node.find('sharesOwnedFollowingTransaction') if post_node is not None else None)
                    value_owned, value_owned_fn = self.get_tag_text(post_node, 'valueOwnedFollowingTransaction'), self.resolve_fn(post_node.find('valueOwnedFollowingTransaction') if post_node is not None else None)
                    own_nature = item.find('ownershipNature')
                    direct, direct_fn = self.get_tag_text(own_nature, 'directOrIndirectOwnership'), self.resolve_fn(own_nature.find('directOrIndirectOwnership') if own_nature is not None else None)
                    nature_text, nature_fn = self.get_tag_text(own_nature, 'natureOfOwnership'), self.resolve_fn(own_nature.find('natureOfOwnership') if own_nature is not None else None)
                    nature_cell = f"{direct} ({nature_text}) {direct_fn} {nature_fn}".strip().replace(" ()", "")
                    price, price_fn = self.get_tag_text(item, 'conversionOrExercisePrice'), self.resolve_fn(item.find('conversionOrExercisePrice'))
                    exer, exer_fn = self.get_tag_text(item, 'exerciseDate'), self.resolve_fn(item.find('exerciseDate'))
                    expr, expr_fn = self.get_tag_text(item, 'expirationDate'), self.resolve_fn(item.find('expirationDate'))
                    under_node = item.find('underlyingSecurity')
                    u_title, u_title_fn = self.get_tag_text(under_node, 'underlyingSecurityTitle'), self.resolve_fn(under_node.find('underlyingSecurityTitle') if under_node is not None else None)
                    u_shares, u_shares_fn = self.get_tag_text(under_node, 'underlyingSecurityShares'), self.resolve_fn(under_node.find('underlyingSecurityShares') if under_node is not None else None)
                    u_value, u_value_fn = self.get_tag_text(under_node, 'underlyingSecurityValue'), self.resolve_fn(under_node.find('underlyingSecurityValue') if under_node is not None else None)
                    md.append(f"| {title} {title_fn}".strip() + f" | {price} {price_fn}".strip() + f" | {exer} {exer_fn}".strip() + f" | {expr} {expr_fn}".strip() + f" | {u_title} {u_title_fn}".strip() + f" | {u_shares} {u_shares_fn}".strip() + f" | {u_value} {u_value_fn}".strip() + f" | {owned} {owned_fn}".strip() + f" | {value_owned} {value_owned_fn}".strip() + f" | {nature_cell} | [NONE] |")

        # Remarks (SEC XSL order: remarks before footnotes)
        md.append("\n## Filing Remarks")
        md.append(remarks if remarks else NULL_TOKEN)

        # Footnotes — always emitted
        fn_node = root.find('footnotes')
        footnotes = fn_node.findall('footnote') if fn_node is not None else []
        md.append("\n## Footnotes")
        if footnotes:
            for fn in footnotes:
                fid = fn.get('id', '').upper().replace('F', '')
                md.append(f"[^{fid}]: {fn.text}")
        else:
            md.append(NULL_TOKEN)

        md.append("\n## Signatures\n| Signature Name | Date |\n| :--- | :--- |")
        for sig in root.findall('ownerSignature'):
            md.append(f"| {self.get_tag_text(sig, 'signatureName')} | {self.get_tag_text(sig, 'signatureDate')} |")

        return "\n".join(md)

    def extract_ground_truth(self, buffer: Union[bytes, str]) -> Dict[str, Any]:
        """Extract structured ground truth data from SEC Ownership XML."""
        content = buffer.decode('utf-8', errors='ignore') if isinstance(buffer, bytes) else buffer
        try:
            match = re.search(r'(<ownershipDocument.*?</ownershipDocument>)', content, re.DOTALL | re.IGNORECASE)
            if not match: return {}
            
            xml_data = match.group(1).strip()
            parser = etree.XMLParser(recover=True, encoding='utf-8')
            root = etree.fromstring(xml_data.encode('utf-8'), parser=parser)
            for el in root.iter():
                if isinstance(el.tag, str) and '}' in el.tag:
                    el.tag = el.tag.split('}', 1)[1]
        except:
            return {}

        data = {
            "issuer": {
                "issuer_cik": self.get_tag_text(root.find('issuer'), 'issuerCik'),
                "issuer_name": self.get_tag_text(root.find('issuer'), 'issuerName'),
                "issuer_trading_symbol": self.get_tag_text(root.find('issuer'), 'issuerTradingSymbol'),
                "issuer_foreign_trading_symbol": self.get_tag_text(root.find('issuer'), 'issuerForeignTradingSymbol'),
            },
            "form_type": self.get_tag_text(root, 'documentType'),
            "period_of_report": self.get_tag_text(root, 'periodOfReport'),
            "date_of_original_submission": self.get_tag_text(root, 'dateOfOriginalSubmission'),
            "remarks": self.get_tag_text(root, 'remarks'),
            "not_subject_to_section_16": self.get_tag_text(root, 'notSubjectToSection16') in ('1', 'true'),
            "is_rule_10b5_1_plan": self.get_tag_text(root, 'aff10b5One') in ('1', 'true'),
            "no_securities_owned": self.get_tag_text(root, 'noSecuritiesOwned') in ('1', 'true'),
            "reporting_owners": [],
            "non_derivative_transactions": [],
            "non_derivative_holdings": [],
            "derivative_transactions": [],
            "derivative_holdings": [],
            "signatures": [],
            "footnotes": {}
        }

        # Signatures
        for sig in root.findall('ownerSignature'):
            data["signatures"].append({
                "signature_name": self.get_tag_text(sig, 'signatureName'),
                "signature_date": self.get_tag_text(sig, 'signatureDate')
            })

        # Footnotes
        fn_node = root.find('footnotes')
        if fn_node is not None:
            for fn in fn_node.findall('footnote'):
                fid = fn.get('id', '').upper().replace('F', '')
                data["footnotes"][fid] = fn.text

        # Reporting Owners
        for owner in root.findall('reportingOwner'):
            id_info = owner.find('reportingOwnerId')
            rel_info = owner.find('reportingOwnerRelationship')
            addr_info = owner.find('reportingOwnerAddress')
            data["reporting_owners"].append({
                "rptowner_cik": self.get_tag_text(id_info, 'rptOwnerCik'),
                "rptowner_name": self.get_tag_text(id_info, 'rptOwnerName'),
                "rptowner_ccc": self.get_tag_text(id_info, 'rptOwnerCcc'),
                "is_director": self.get_tag_text(rel_info, 'isDirector') in ('1', 'true'),
                "is_officer": self.get_tag_text(rel_info, 'isOfficer') in ('1', 'true'),
                "is_10pctowner": self.get_tag_text(rel_info, 'isTenPercentOwner') in ('1', 'true'),
                "is_other": self.get_tag_text(rel_info, 'isOther') in ('1', 'true'),
                "officer_title": self.get_tag_text(rel_info, 'officerTitle'),
                "other_text": self.get_tag_text(rel_info, 'otherText'),
                "rptowner_street1": self.get_tag_text(addr_info, 'rptOwnerStreet1'),
                "rptowner_street2": self.get_tag_text(addr_info, 'rptOwnerStreet2'),
                "rptowner_city": self.get_tag_text(addr_info, 'rptOwnerCity'),
                "rptowner_state": self.get_tag_text(addr_info, 'rptOwnerState'),
                "rptowner_zip": self.get_tag_text(addr_info, 'rptOwnerZipCode'),
                "rptowner_country": self.get_tag_text(addr_info, 'rptOwnerCountry'),
                "rptowner_non_us_address_flag": self.get_tag_text(addr_info, 'rptOwnerNonUSStateTerritory') != "",
                "rptowner_non_us_state_territory": self.get_tag_text(addr_info, 'rptOwnerNonUSStateTerritory'),
            })

        # Table I
        nd_table = root.find('nonDerivativeTable')
        if nd_table is not None:
            for item in nd_table:
                if item.tag == 'nonDerivativeTransaction':
                    post = item.find('postTransactionAmounts')
                    nature = item.find('ownershipNature')
                    amt = item.find('transactionAmounts')
                    coding = item.find('transactionCoding')
                    data["non_derivative_transactions"].append({
                        "security_title": self.get_tag_text(item, 'securityTitle'),
                        "transaction_date": self.get_tag_text(item, 'transactionDate'),
                        "transaction_code": self.get_tag_text(coding, 'transactionCode'),
                        "transaction_shares": float(self.get_tag_text(amt, 'transactionShares') or 0),
                        "price": float(self.get_tag_text(amt, 'transactionPricePerShare') or 0),
                        "transaction_acquired_disposed_code": self.get_tag_text(amt, 'transactionAcquiredDisposedCode'),
                        "shares_owned_following_transaction": float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction') or 0),
                        "direct_or_indirect_ownership": self.get_tag_text(nature, 'directOrIndirectOwnership'),
                        "nature_of_ownership": self.get_tag_text(nature, 'natureOfOwnership'),
                    })
                else:
                    post = item.find('postTransactionAmounts')
                    nature = item.find('ownershipNature')
                    data["non_derivative_holdings"].append({
                        "security_title": self.get_tag_text(item, 'securityTitle'),
                        "shares_owned_following_transaction": float(self.get_tag_text(post, 'sharesOwnedFollowingTransaction') or 0),
                        "direct_or_indirect_ownership": self.get_tag_text(nature, 'directOrIndirectOwnership'),
                        "nature_of_ownership": self.get_tag_text(nature, 'natureOfOwnership'),
                    })

        return data

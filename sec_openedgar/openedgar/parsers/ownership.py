import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Union
from .base import BaseFormParser

class OwnershipParser(BaseFormParser):
    """
    Specialized parser for SEC Ownership Forms (3, 4, 5).
    Extracts structured XML data and converts to highly readable Markdown tables.
    """
    
    @property
    def form_types(self) -> List[str]:
        return ['3', '4', '5', '3/A', '4/A', '5/A']

    def to_markdown(self, buffer: Union[bytes, str]) -> str:
        """Generates a clean, tabular Markdown representation of the Form 4 XML."""
        data = self.extract_ground_truth(buffer)
        
        md = []
        md.append(f"# SEC Form {data.get('form_type', 'Ownership')} Filing")
        md.append(f"**Issuer:** {data.get('issuer_name')} (CIK: {data.get('issuer_cik')})")
        md.append(f"**Reporting Person:** {data.get('owner_name')} (CIK: {data.get('owner_cik')})")
        md.append(f"**Period of Report:** {data.get('period_of_report')}\n")
        
        # Table I: Non-Derivative Securities
        non_derivs = data.get('non_derivative_transactions', [])
        if non_derivs:
            md.append("### Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned")
            md.append("| Security Title | Date | Code | Amount | Price | Direct/Indirect |")
            md.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
            for t in non_derivs:
                md.append(f"| {t.get('security_title')} | {t.get('date')} | {t.get('code')} | {t.get('shares')} | {t.get('price')} | {t.get('ownership_form')} |")
            md.append("")

        # Table II: Derivative Securities
        derivs = data.get('derivative_transactions', [])
        if derivs:
            md.append("### Table II - Derivative Securities Acquired, Disposed of, or Beneficially Owned")
            md.append("| Security Title | Conversion/Exercise Price | Date | Code | Amount | Exercisable | Expiration |")
            md.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
            for t in derivs:
                md.append(f"| {t.get('security_title')} | {t.get('price')} | {t.get('date')} | {t.get('code')} | {t.get('shares')} | {t.get('exercisable_date')} | {t.get('expiration_date')} |")
            md.append("")

        return "\n".join(md)

    def extract_ground_truth(self, buffer: Union[bytes, str]) -> Dict[str, Any]:
        """Rules-based extraction from the native SEC XML schema."""
        if isinstance(buffer, str):
            # Form 3/4/5 sometimes contain XML blocks inside SGML tags
            # We look for the <XML> block
            if "<XML>" in buffer:
                buffer = buffer.split("<XML>")[1].split("</XML>")[0]
            buffer = buffer.encode('utf-8')
        
        try:
            root = ET.fromstring(buffer)
        except Exception:
            # Fallback if XML is nested or malformed in the SGML
            return {"error": "Invalid XML"}

        result = {
            "form_type": root.findtext('submissionType', ''),
            "issuer_cik": root.findtext('.//issuerCik', ''),
            "issuer_name": root.findtext('.//issuerName', ''),
            "issuer_ticker": root.findtext('.//issuerTradingSymbol', ''),
            "owner_cik": root.findtext('.//reportingOwnerId/cik', ''),
            "owner_name": root.findtext('.//reportingOwnerName', ''),
            "period_of_report": root.findtext('periodOfReport', ''),
            "non_derivative_transactions": [],
            "derivative_transactions": []
        }

        # Parse Table I
        for trans in root.findall('.//nonDerivativeTransaction'):
            result["non_derivative_transactions"].append({
                "security_title": trans.findtext('.//securityTitle/value', ''),
                "date": trans.findtext('.//transactionDate/value', ''),
                "code": trans.findtext('.//transactionAcquiredDisposedCode/value', ''),
                "shares": trans.findtext('.//transactionShares/value', ''),
                "price": trans.findtext('.//transactionPricePerShare/value', ''),
                "ownership_form": trans.findtext('.//directOrIndirectOwnership/value', '')
            })

        # Parse Table II
        for trans in root.findall('.//derivativeTransaction'):
            result["derivative_transactions"].append({
                "security_title": trans.findtext('.//securityTitle/value', ''),
                "price": trans.findtext('.//conversionOrExercisePrice/value', ''),
                "date": trans.findtext('.//transactionDate/value', ''),
                "code": trans.findtext('.//transactionAcquiredDisposedCode/value', ''),
                "shares": trans.findtext('.//transactionShares/value', ''),
                "exercisable_date": trans.findtext('.//exerciseDate/value', ''),
                "expiration_date": trans.findtext('.//expirationDate/value', '')
            })

        return result

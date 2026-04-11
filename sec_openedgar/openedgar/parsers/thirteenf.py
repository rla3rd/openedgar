import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Union
from .base import BaseFormParser

class ThirteenFParser(BaseFormParser):
    """
    Specialized parser for SEC Form 13F (Institutional Holdings).
    """
    
    @property
    def form_types(self) -> List[str]:
        return ['13F-HR', '13F-NT', '13F-HR/A', '13F-NT/A']

    def to_markdown(self, buffer: Union[bytes, str]) -> str:
        """Generates a clean, tabular Markdown representation of Form 13F holdings."""
        data = self.extract_ground_truth(buffer)
        
        md = []
        md.append(f"# SEC Form {data.get('form_type', '13F')} Filing")
        md.append(f"**Manager:** {data.get('manager_name')} (CIK: {data.get('manager_cik')})")
        md.append(f"**Report Period:** {data.get('period_of_report')}\n")
        
        holdings = data.get('holdings', [])
        if holdings:
            md.append("### Information Table - Institutional Holdings")
            md.append("| Name of Issuer | Title of Class | CUSIP | Value (x$1000) | Shares/Amt | Type | Investment Discretion |")
            md.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
            for h in holdings:
                md.append(f"| {h.get('issuer')} | {h.get('class')} | {h.get('cusip')} | {h.get('value')} | {h.get('shares')} | {h.get('sh_type')} | {h.get('discretion')} |")
        else:
            md.append("*No data found in information table or form is a Notice filing.*")
            
        return "\n".join(md)

    def extract_ground_truth(self, buffer: Union[bytes, str]) -> Dict[str, Any]:
        """Rules-based extraction from the 13F XML schema."""
        if isinstance(buffer, str):
            import re
            # Extract content between <XML> or <xml> tags
            xml_blocks = re.findall(r'<(?:XML|xml)>(.*?)</(?:XML|xml)>', buffer, re.DOTALL)
            if len(xml_blocks) > 1:
                primary_xml = xml_blocks[0]
                table_xml = xml_blocks[1]
            elif len(xml_blocks) > 0:
                primary_xml = xml_blocks[0]
                table_xml = ""
            else:
                primary_xml = buffer
                table_xml = ""
        else:
            primary_xml = buffer
            table_xml = ""

        result = {
            "form_type": "13F",
            "manager_cik": "",
            "manager_name": "",
            "period_of_report": "",
            "holdings": []
        }

        try:
            # Parse Primary
            p_root = ET.fromstring(primary_xml.strip())
            # SEC 13F metadata can be at root or under edgarSubmission
            result["manager_cik"] = p_root.findtext('.//credentials/cik') or p_root.findtext('.//managerCik') or ""
            result["manager_name"] = p_root.findtext('.//filingManager/name') or p_root.findtext('.//managerName') or ""
            result["period_of_report"] = p_root.findtext('.//periodOfReport', '')
            result["form_type"] = p_root.findtext('.//submissionType', '13F')
            
            # Parse Information Table if available
            if table_xml:
                t_root = ET.fromstring(table_xml.strip().encode('utf-8'))
                # Handle namespaces if present (common in 13F)
                ns = {'ns': 'http://www.sec.gov/edgar/document/thirteenf/informationtable'}
                
                for info in t_root.findall('.//ns:infoTable', ns) or t_root.findall('.//infoTable'):
                    result["holdings"].append({
                        "issuer": info.findtext('.//ns:nameOfIssuer', info.findtext('.//nameOfIssuer', ''), namespaces=ns),
                        "class": info.findtext('.//ns:titleOfClass', info.findtext('.//titleOfClass', ''), namespaces=ns),
                        "cusip": info.findtext('.//ns:cusip', info.findtext('.//cusip', ''), namespaces=ns),
                        "value": info.findtext('.//ns:value', info.findtext('.//value', ''), namespaces=ns),
                        "shares": info.findtext('.//ns:shrsOrPrnAmt/ns:sshPrnAmt', info.findtext('.//shrsOrPrnAmt/sshPrnAmt', ''), namespaces=ns),
                        "sh_type": info.findtext('.//ns:shrsOrPrnAmt/ns:sshPrnAmtType', info.findtext('.//shrsOrPrnAmt/sshPrnAmtType', ''), namespaces=ns),
                        "discretion": info.findtext('.//ns:investmentDiscretion', info.findtext('.//investmentDiscretion', ''), namespaces=ns),
                        "ticker": None 
                    })
        except Exception as e:
            result["error"] = str(e)

        return result

import pytest
from openedgar.parsers.thirteenf import ThirteenFParser
from openedgar.parsers.ownership import OwnershipParser

def test_13f_parser_basic():
    parser = ThirteenFParser()
    xml_content = """<XML>
<edgarSubmission>
    <headerData><submissionType>13F-HR</submissionType></headerData>
    <formData>
        <filingManager><name>BERKSHIRE HATHAWAY INC</name></filingManager>
        <periodOfReport>2023-12-31</periodOfReport>
    </formData>
</edgarSubmission>
</XML>
<XML>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
    <infoTable>
        <nameOfIssuer>APPLE INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>037833100</cusip>
        <value>1000000</value>
        <shrsOrPrnAmt><sshPrnAmt>5000000</sshPrnAmt><sshPrnAmtType>SH</sshPrnAmtType></shrsOrPrnAmt>
    </infoTable>
</informationTable>
</XML>"""
    data = parser.extract_ground_truth(xml_content)
    assert data["manager_name"] == "BERKSHIRE HATHAWAY INC"
    assert len(data["holdings"]) == 1
    assert data["holdings"][0]["ticker"] is None # We don't resolve ticker in the parser itself, we do it later
    assert data["holdings"][0]["cusip"] == "037833100"

def test_ownership_parser_basic():
    parser = OwnershipParser()
    # Mock Form 4 XML
    xml_content = """<ownershipDocument>
    <issuer><issuerCik>0000320193</issuerCik><issuerName>Apple Inc.</issuerName><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
    <nonDerivativeTable>
        <nonDerivativeTransaction>
            <securityTitle><value>Common Stock</value></securityTitle>
            <transactionDate><value>2024-01-01</value></transactionDate>
            <transactionAmounts><transactionShares><value>100</value></transactionShares></transactionAmounts>
        </nonDerivativeTransaction>
    </nonDerivativeTable>
</ownershipDocument>"""
    data = parser.extract_ground_truth(xml_content)
    assert data["issuer_ticker"] == "AAPL"
    assert len(data["non_derivative_transactions"]) == 1

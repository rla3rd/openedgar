import pytest
import re
from sec_research.experiments.ownership_extraction.synthesizers.ownership import (
    OwnershipMarkdownSynthesizer,
)
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

def test_ownership_markdown_synthesizer_table_i_column_placement():
    parser = OwnershipParser()
    xml_content = """<ownershipDocument>
    <documentType>4</documentType>
    <schemaVersion>X0306</schemaVersion>
    <periodOfReport>2024-01-01</periodOfReport>
    <issuer><issuerCik>0000320193</issuerCik><issuerName>Apple Inc.</issuerName><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
    <nonDerivativeTable>
        <nonDerivativeTransaction>
            <securityTitle><value>Common Stock</value></securityTitle>
            <transactionDate><value>2024-01-01</value></transactionDate>
            <transactionCoding>
                <transactionCode><value>M</value></transactionCode>
                <transactionFormType><value>4</value></transactionFormType>
            </transactionCoding>
            <transactionAmounts>
                <transactionShares><value>100</value></transactionShares>
                <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
            <postTransactionAmounts>
                <sharesOwnedFollowingTransaction><value>150</value></sharesOwnedFollowingTransaction>
            </postTransactionAmounts>
            <ownershipNature>
                <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
            </ownershipNature>
        </nonDerivativeTransaction>
    </nonDerivativeTable>
</ownershipDocument>"""
    md = parser.to_markdown(xml_content)

    assert "| Security | Date | Deemed Exec | Code | Form Type | A/D | Equity Swap | Timeliness | Shares | Price | Total Value | Owned After | Value After | Nature |" in md

    # Ensure transaction form type and ownership direction land in distinct columns.
    data_line = next(line for line in md.splitlines() if line.startswith("| Common Stock"))
    assert re.search(r"\|\s*4\s*\|\s*A\s*\|", data_line)
    assert data_line.rstrip().endswith("| D |")


def test_ownership_to_markdown_accepts_bytes_and_str_equally():
    parser = OwnershipParser()
    xml_content = """<ownershipDocument>
    <documentType>4</documentType>
    <issuer><issuerCik>0000320193</issuerCik><issuerName>Apple Inc.</issuerName><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
    </ownershipDocument>"""

    md_from_str = parser.to_markdown(xml_content)
    md_from_bytes = parser.to_markdown(xml_content.encode("utf-8"))
    assert md_from_str == md_from_bytes


def test_experiments_synthesizer_is_canonical_alias():
    # Hard guard: experiments must not carry a separate implementation.
    assert OwnershipMarkdownSynthesizer is OwnershipParser

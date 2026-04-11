import docling
from docling.document_converter import DocumentConverter
import tempfile
import os

form4_sample = """
<xml>
<issuer><issuerCik>0001234567</issuerCik><issuerName>MOCK CORP</issuerName></issuer>
<reportingOwner><reportingOwnerId><cik>0000099999</cik></reportingOwnerId></reportingOwner>
<nonDerivativeTable>
<nonDerivativeTransaction>
<securityTitle><value>Common Stock</value></securityTitle>
<transactionDate><value>2024-01-01</value></transactionDate>
<transactionShares><value>1000</value></transactionShares>
<transactionPricePerShare><value>150.00</value></transactionPricePerShare>
<transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
</nonDerivativeTransaction>
</nonDerivativeTable>
</xml>
"""

try:
    print("Testing docling on Form 4 XML...")
    converter = DocumentConverter()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp:
        tmp.write(form4_sample.encode("utf-8"))
        tmp_path = tmp.name
        
    try:
        result = converter.convert(tmp_path)
        md = result.document.export_to_markdown()
        print("\n--- Output Markdown ---")
        print(md)
        print("--- End ---")
    finally:
        os.remove(tmp_path)
except Exception as e:
    print(f"Error: {e}")

import sec2md
import sys

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
    print("Testing sec2md.convert_to_markdown on Form 4 XML...")
    md = sec2md.convert_to_markdown(form4_sample)
    print("\n--- Output Markdown ---")
    print(md)
    print("--- End ---")
except Exception as e:
    print(f"Error: {e}")

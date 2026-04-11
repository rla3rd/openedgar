import re

with open('sec_openedgar/openedgar/tasks.py', 'r') as f:
    text = f.read()

# 1. Imports
text = re.sub(
    r'from edgar\.entities import NoCompanyFactsFound\n.*edgar\.use_local_storage\(\)\n',
    'import openedgar.clients.openedgar\nimport openedgar.parsers.openedgar\nfrom openedgar.models import Company, CompanyFact, CompanyInfo, FactIndex, FilingIndex, Filing, FilingDocument, FormIndex, SearchQuery, SearchQueryTerm, SearchQueryResult\nfrom openedgar.sec_api import sec_api\nimport hyperstreamdb as hs\n',
    text,
    flags=re.DOTALL
)

# 2. process_company()
text = text.replace('df = edgar.get_cik_lookup_data()', 'df = sec_api.get_cik_lookup_data()')

# 3. process_companyinfo_cik()
# Replace `c = edgar.Company(company.cik)` block. I'll use regex to match from `c = edgar.Company` until `investor_website = ...`
text = re.sub(
    r'c = edgar\.Company\(company\.cik\).*?investor_website = getattr\(c, \'investor_website\', None\)',
    '''c_data = sec_api.get_company_submissions(company.cik)
        cik = company.cik
        ci = CompanyInfo()
        ci.cik = company
        ci.name = c_data.get('name')
        ci.is_company = True
        ci.category = c_data.get('category')
        ci.description = c_data.get('description')
        ci.entity_type = c_data.get('entityType')
        ci.ein = c_data.get('ein')
        ci.industry = c_data.get('industry')
        ci.sic = c_data.get('sic')
        ci.sic_description = c_data.get('sicDescription')
        ci.state_of_incorporation = c_data.get('stateOfIncorporation')
        ci.state_of_incorporation_description = c_data.get('stateOfIncorporationDescription')
        ci.fiscal_year_end = c_data.get('fiscalYearEnd')
        ci.mailing_address = c_data.get('addresses', {}).get('mailing', {})
        ci.business_address = c_data.get('addresses', {}).get('business', {})
        ci.phone = c_data.get('phone')
        ci.tickers = c_data.get('tickers', [])
        ci.exchanges = c_data.get('exchanges', [])
        ci.former_names = c_data.get('formerNames', [])
        ci.flags = c_data.get('flags')
        ci.insider_transaction_for_owner_exists = c_data.get('insiderTransactionForOwnerExists', 0)
        ci.insider_transaction_for_issuer_exists = c_data.get('insiderTransactionForIssuerExists', 0)
        ci.website = c_data.get('website', '')
        ci.investor_website = c_data.get('investorWebsite', '')''',
    text,
    flags=re.DOTALL
)

# 4. process_companyfacts_cik()
# Replace from `company = edgar.Company(cik)` to `processed = True`
text = re.sub(
    r'company = edgar\.Company\(cik\).*?update_fields=\[\n.*?\s+\'frame\'\]\)\n\s+processed = True',
    '''from openedgar.sec_api import sec_api
        import hyperstreamdb as hs
        
        # Pull from SEC securely with fast parsed DataFrames
        facts_df, meta_df = sec_api.get_company_facts_pandas(cik)
        
        if facts_df is not None and not facts_df.empty:
            # Postgres Model: Save structural FactIndex metadata for search
            for row in meta_df.itertuples():
                try:
                    fi = FactIndex.objects.get(fact=row.fact)
                except FactIndex.DoesNotExist:
                    fi = FactIndex()
                fi.fact = row.fact
                fi.label = row.label
                fi.description = row.description
                fi.save()
                
            # Formatting the Facts dataframe payload
            facts_df['cik'] = str(cik).zfill(10)
            facts_df['id'] = facts_df['fact'].astype(str) + '_' + facts_df['accn'].astype(str)
            facts_df['val'] = facts_df['val'].fillna(0.0)
            facts_df['fy'] = facts_df['fy'].fillna(0)
            facts_df['fp'] = facts_df['fp'].fillna('')
            facts_df['frame'] = facts_df['frame'].fillna('')

            # HyperStreamDB Persistance Bypassing Django CompanyFact
            data_dir = os.getenv('EDGAR_LOCAL_DATA_DIR', '/tmp')
            hs_uri = os.environ.get("HYPERSTREAM_FACTS_URI", f"file://{data_dir}/hyperstream_facts")
            table = hs.Table(uri=hs_uri)
            
            # Extreme high throughput Vector/Facts engine ingestion
            table.upsert(facts_df, key_column='id')
            processed = True''',
    text,
    flags=re.DOTALL
)

# 5. process_filingindex_year replace edgar
text = re.sub(
    r'filing_index = edgar\.get_filings\(year(.*?)\)\n\s+if filing_index is not None:\n\s+filing_index = filing_index\.to_pandas\(\)',
    r'filing_index = sec_api.get_filings([year])\n    if not filing_index.empty:',
    text,
    flags=re.DOTALL
)

# 6. process_filings replace edgar loop
text = re.sub(
    r'filings = edgar\.get_filings\(year\)',
    'filings_df = sec_api.get_filings([year])\n            filings = [row for row in filings_df.itertuples()]',
    text
)
text = re.sub(
    r'filings = edgar\.get_filings\(range\(min_year, max_year \+ 1\)\)',
    'filings_df = sec_api.get_filings(list(range(min_year, max_year + 1)))\n            filings = [row for row in filings_df.itertuples()]',
    text
)
text = re.sub(
    r'for filing in filings:\n\s+f = Filing\(\)\n\s+try:\n.*?f\.text_url = filing\.text_url',
    '''for filing in filings:
            f = Filing()
            try:
                company = Company.objects.get(cik=filing.cik)
            except Company.DoesNotExist:
                company = Company.objects.create(cik=filing.cik, cik_name=filing.company)
            f.document_count = None
            f.acceptance_datetime = None
            f.accession_number = filing.accession_number
            f.form_type = filing.form
            f.date_filed = filing.filing_date
            f.cik = company
            f.company = filing.company
            f.document_url = filing.document_url
            f.homepage_url = filing.homepage_url
            f.text_url = filing.text_url''',
    text,
    flags=re.DOTALL
)

with open('sec_openedgar/openedgar/tasks.py', 'w') as f:
    f.write(text)

print("edgar mentions left:", text.count("edgar."))

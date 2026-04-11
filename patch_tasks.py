import re

with open('sec_openedgar/openedgar/tasks.py', 'r') as f:
    content = f.read()

# 1. Imports
content = re.sub(
    r'# edgartools\nimport edgar\nfrom edgar import forms as frm\nedgar\.set_identity\(EDGAR_IDENTITY\)\nedgar\.use_local_storage\(\)\n',
    r'# sec_api\nfrom openedgar.sec_api import sec_api\n',
    content
)

# 2. process_company()
content = content.replace('df = edgar.get_cik_lookup_data()', 'df = sec_api.get_cik_lookup_data()')

# 3. process_companyinfo_cik()
old_companyinfo = """        processed = False
        company = Company.objects.get(cik=cik)
        c = edgar.Company(company.cik)
        cik = company.cik
        ci = CompanyInfo()
        ci.cik = company
        ci.name = getattr(c, 'name', None)
        ci.is_company = True
        ci.category = getattr(c, 'category', None)
        ci.description = getattr(c, 'description', None)
        ci.entity_type = getattr(c, 'entity_type', None)
        ci.ein = getattr(c, 'ein', None)
        ci.industry = getattr(c, 'industry', None)
        ci.sic = getattr(c, 'sic', None)
        ci.sic_description = getattr(c, 'sic_description', None)
        ci.state_of_incorporation = getattr(c, 'state_of_incorporation', None)
        ci.state_of_incorporation_description = getattr(c, 'state_of_incorporation_description', None)
        ci.fiscal_year_end = getattr(c, 'fiscal_year_end', None)
        ci.mailing_address = getattr(c, 'mailing_address', None).__dict__ if getattr(c, 'mailing_address', None) else None
        ci.business_address = getattr(c, 'business_address', None).__dict__ if getattr(c, 'business_address', None) else None
        ci.phone = getattr(c, 'phone', None)
        ci.tickers = getattr(c, 'tickers', [])
        ci.exchanges = getattr(c, 'exchanges', [])
        ci.former_names = getattr(c, 'former_names', None)
        ci.flags = getattr(c, 'flags', None)
        ci.insider_transaction_for_owner_exists = getattr(c, 'insider_transaction_for_owner_exists', 0)
        ci.insider_transaction_for_issuer_exists = getattr(c, 'insider_transaction_for_issuer_exists', 0)
        ci.website = getattr(c, 'website', None)
        ci.investor_website = getattr(c, 'investor_website', None)"""

new_companyinfo = """        processed = False
        company = Company.objects.get(cik=cik)
        c_data = sec_api.get_company_submissions(company.cik)
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
        ci.investor_website = c_data.get('investorWebsite', '')"""
content = content.replace(old_companyinfo, new_companyinfo)

# 4. process_companyfacts_cik()
# We need to replace the edgar.Company(cik) part
old_facts = """    try:
        company = edgar.Company(cik)
        if company is not None:
            facts = company.get_facts()
            if facts is not None:
                all_facts = facts.get_all_facts()
                
                # Pre-save unique FactIndex entries
                seen_facts = {}
                for fact in all_facts:
                    if fact.concept not in seen_facts:
                        seen_facts[fact.concept] = {'label': getattr(fact, 'label', ''), 'description': ''}
                
                for concept, data in seen_facts.items():
                    try:
                        fi = FactIndex.objects.get(fact=concept)
                    except FactIndex.DoesNotExist:
                        fi = FactIndex()
                    fi.fact = concept
                    fi.label = data['label']
                    fi.description = data['description']
                    fi.save()

                # Process all facts
                fact_objects = []
                # Keep track of unique fact IDs to avoid bulk_create duplicate entries
                seen_ids = set()
                
                for fact in all_facts:
                    # edgartools 5.x FinancialFact attributes mappings
                    fact_id = f"{fact.concept}_{fact.accession}"
                    if fact_id in seen_ids:
                        continue
                    seen_ids.add(fact_id)

                    try:
                        ft = FormIndex.objects.get(form=fact.form_type)
                    except FormIndex.DoesNotExist:
                        ft = FormIndex.objects.create(form=fact.form_type)
                    try:
                        fact_idx = FactIndex.objects.get(fact=fact.concept)
                    except FactIndex.DoesNotExist:
                        fact_idx = FactIndex.objects.create(fact=fact.concept)
                    try:
                        c = Company.objects.get(cik=cik)
                    except Company.DoesNotExist:
                        c = Company.objects.create(cik=cik)
                    try:
                        fi = FilingIndex.objects.get(accession_number=fact.accession)
                    except FilingIndex.DoesNotExist:
                        fi = FilingIndex.objects.create(
                            accession_number=fact.accession,
                            cik=c)
                    
                    try:
                        cf = CompanyFact.objects.get(id=fact_id)
                    except CompanyFact.DoesNotExist:
                        cf = CompanyFact(id=fact_id, cik=c, fact=fact_idx, formtype=ft, accession_number=fi)

                    cf.namespace = getattr(fact, 'taxonomy', 'us-gaap')
                    cf.value = getattr(fact, 'numeric_value', getattr(fact, 'value', 0.0))
                    if cf.value is None: cf.value = 0.0
                    cf.end_date = getattr(fact, 'period_end', None)
                    cf.datefiled = getattr(fact, 'filing_date', None)
                    cf.fiscal_year = getattr(fact, 'fiscal_year', 0)
                    if cf.fiscal_year is None: cf.fiscal_year = 0
                    cf.fiscal_period = getattr(fact, 'fiscal_period', '')
                    cf.frame = ''

                    fact_objects.append(cf)

                CompanyFact.objects.bulk_create(
                    fact_objects,
                    update_conflicts=True, 
                    unique_fields=['id'],
                    update_fields=[
                        'namespace',
                        'value',
                        'end_date',
                        'datefiled',
                        'fiscal_year',
                        'fiscal_period',
                        'frame'])
                processed = True"""

new_facts = """    try:
        import pandas as pd
        facts_df, meta_df = sec_api.get_company_facts_pandas(cik)
        if facts_df is not None and not facts_df.empty:
            # Pre-save unique FactIndex entries
            for row in meta_df.itertuples():
                try:
                    fi = FactIndex.objects.get(fact=row.fact)
                except FactIndex.DoesNotExist:
                    fi = FactIndex()
                fi.fact = row.fact
                fi.label = row.label
                fi.description = row.description
                fi.save()

            # Process all facts
            fact_objects = []
            seen_ids = set()
            
            for fact in facts_df.itertuples():
                fact_id = f"{fact.fact}_{fact.accn}"
                if fact_id in seen_ids:
                    continue
                seen_ids.add(fact_id)

                try:
                    ft = FormIndex.objects.get(form=fact.form)
                except FormIndex.DoesNotExist:
                    ft = FormIndex.objects.create(form=fact.form)
                try:
                    fact_idx = FactIndex.objects.get(fact=fact.fact)
                except FactIndex.DoesNotExist:
                    fact_idx = FactIndex.objects.create(fact=fact.fact)
                try:
                    c = Company.objects.get(cik=cik)
                except Company.DoesNotExist:
                    c = Company.objects.create(cik=cik)
                try:
                    fi = FilingIndex.objects.get(accession_number=fact.accn)
                except FilingIndex.DoesNotExist:
                    fi = FilingIndex.objects.create(
                        accession_number=fact.accn,
                        cik=c)
                
                try:
                    cf = CompanyFact.objects.get(id=fact_id)
                except CompanyFact.DoesNotExist:
                    cf = CompanyFact(id=fact_id, cik=c, fact=fact_idx, formtype=ft, accession_number=fi)

                cf.namespace = fact.namespace
                cf.value = fact.val if pd.notna(fact.val) else 0.0
                if cf.value is None: cf.value = 0.0
                cf.end_date = fact.end if pd.notna(fact.end) else None
                cf.datefiled = fact.filed if pd.notna(fact.filed) else None
                cf.fiscal_year = fact.fy if pd.notna(fact.fy) else 0
                if cf.fiscal_year is None: cf.fiscal_year = 0
                cf.fiscal_period = fact.fp if pd.notna(fact.fp) else ''
                cf.frame = fact.frame if pd.notna(fact.frame) else ''

                fact_objects.append(cf)

            CompanyFact.objects.bulk_create(
                fact_objects,
                update_conflicts=True, 
                unique_fields=['id'],
                update_fields=[
                    'namespace',
                    'value',
                    'end_date',
                    'datefiled',
                    'fiscal_year',
                    'fiscal_period',
                    'frame'])
            processed = True"""
content = content.replace(old_facts, new_facts)

# 5. process_filingindex_year
old_fidx = """def process_filingindex_year(year:int, batch_size:int=1000, upsert:bool=False, formtypes:Iterable[str]=None):
    filing_index = edgar.get_filings(year)
    if filing_index is not None:
        filing_index = filing_index.to_pandas()"""
        
new_fidx = """def process_filingindex_year(year:int, batch_size:int=1000, upsert:bool=False, formtypes:Iterable[str]=None):
    filing_index = sec_api.get_filings([year])
    if not filing_index.empty:"""
content = content.replace(old_fidx, new_fidx)

# 6. process_filings
old_pf1 = """            filings = edgar.get_filings(year)
            filing_ct: int = len(filings)"""
new_pf1 = """            filings_df = sec_api.get_filings([year])
            filings = [row for row in filings_df.itertuples()]
            filing_ct: int = len(filings)"""
content = content.replace(old_pf1, new_pf1)

old_pf2 = """            filings = edgar.get_filings(range(min_year, max_year + 1))
            filing_ct: int = len(filings)"""
new_pf2 = """            filings_df = sec_api.get_filings(list(range(min_year, max_year + 1)))
            filings = [row for row in filings_df.itertuples()]
            filing_ct: int = len(filings)"""
content = content.replace(old_pf2, new_pf2)

old_pfloop = """        filing_objects = []  
        for filing in filings:
            f = Filing()
            try:
                company = Company.objects.get(cik=filing.cik)
            except Company.DoesNotExist:
                company = Company.objects.create(cik=filing.cik, cik_name=filing.company)
            f.document_count = filing.header.document_count
            f.acceptance_datetime = filing.header.acceptance_datetime
            f.accession_number = filing.accession_number
            f.form_type = filing.form
            f.date_filed = filing.filing_date
            f.cik = company
            f.company = filing.company
            f.document_url = filing.document.url
            f.homepage_url = filing.homepage_url
            f.text_url = filing.text_url"""
new_pfloop = """        filing_objects = []  
        for filing in filings:
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
            f.text_url = filing.text_url"""
content = content.replace(old_pfloop, new_pfloop)

with open('sec_openedgar/openedgar/tasks.py', 'w') as f:
    f.write(content)


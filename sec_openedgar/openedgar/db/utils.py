"""
MIT License
Copyright (c) 2024 Richard Albright
Copyright (c) 2018 ContraxSuite, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import os
import sys
import django
from django.core.management import call_command
from sqlalchemy import inspect

# Add the project path to the sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.local")
django.setup()

from django.db import connection
from django.apps import apps
from openedgar.db.session import engine, Session, Base
from openedgar.db.models import *


def create_tables():
    """Create all tables in the database."""
    Base.metadata.create_all(engine)
    print("SQLAlchemy tables created successfully.")


def drop_tables():
    """Drop all tables in the database."""
    Base.metadata.drop_all(engine)
    print("SQLAlchemy tables dropped successfully.")


def migrate_data():
    """Migrate data from Django ORM to SQLAlchemy."""
    # Import Django models
    from openedgar.models import (
        Company as DjangoCompany,
        CompanyInfo as DjangoCompanyInfo,
        FormIndex as DjangoFormIndex,
        BulkFilingIndex as DjangoBulkFilingIndex,
        FilingIndex as DjangoFilingIndex,
        Filing as DjangoFiling,
        FactIndex as DjangoFactIndex,
        CompanyFact as DjangoCompanyFact,
        FilingDocument as DjangoFilingDocument,
        SearchQuery as DjangoSearchQuery,
        SearchQueryTerm as DjangoSearchQueryTerm,
        SearchQueryResult as DjangoSearchQueryResult,
    )
    
    session = Session()
    
    try:
        # Migrate Company
        print("Migrating Company data...")
        companies = DjangoCompany.objects.all()
        for django_company in companies:
            company = Company(
                cik=django_company.cik,
                cik_name=django_company.cik_name
            )
            session.add(company)
        
        # Migrate FormIndex (needed before other tables with form_type foreign keys)
        print("Migrating FormIndex data...")
        form_indices = DjangoFormIndex.objects.all()
        for django_form_index in form_indices:
            form_index = FormIndex(
                form=django_form_index.form,
                description=django_form_index.description
            )
            session.add(form_index)
        
        # Commit to ensure foreign key references exist
        session.commit()
        
        # Migrate CompanyInfo
        print("Migrating CompanyInfo data...")
        company_infos = DjangoCompanyInfo.objects.all()
        for django_company_info in company_infos:
            company_info = CompanyInfo(
                cik_id=django_company_info.cik_id,
                name=django_company_info.name,
                is_company=django_company_info.is_company,
                category=django_company_info.category,
                description=django_company_info.description,
                entity_type=django_company_info.entity_type,
                ein=django_company_info.ein,
                industry=django_company_info.industry,
                sic=django_company_info.sic,
                sic_description=django_company_info.sic_description,
                state_of_incorporation=django_company_info.state_of_incorporation,
                state_of_incorporation_description=django_company_info.state_of_incorporation_description,
                fiscal_year_end=django_company_info.fiscal_year_end,
                mailing_address=django_company_info.mailing_address,
                business_address=django_company_info.business_address,
                phone=django_company_info.phone,
                tickers=django_company_info.tickers,
                exchanges=django_company_info.exchanges,
                former_names=django_company_info.former_names,
                flags=django_company_info.flags,
                insider_transaction_for_owner_exists=django_company_info.insider_transaction_for_owner_exists,
                insider_transaction_for_issuer_exists=django_company_info.insider_transaction_for_issuer_exists,
                website=django_company_info.website,
                investor_website=django_company_info.investor_website,
                asof=django_company_info.asof
            )
            session.add(company_info)
        
        # Migrate BulkFilingIndex
        print("Migrating BulkFilingIndex data...")
        bulk_filing_indices = DjangoBulkFilingIndex.objects.all()
        for django_bulk_filing_index in bulk_filing_indices:
            bulk_filing_index = BulkFilingIndex(
                filename=django_bulk_filing_index.filename,
                year=django_bulk_filing_index.year,
                quarter=django_bulk_filing_index.quarter,
                processed=django_bulk_filing_index.processed,
                error=django_bulk_filing_index.error,
                ignored=django_bulk_filing_index.ignored
            )
            session.add(bulk_filing_index)
        
        # Migrate FilingIndex
        print("Migrating FilingIndex data...")
        filing_indices = DjangoFilingIndex.objects.all()
        for django_filing_index in filing_indices:
            filing_index = FilingIndex(
                form_id=django_filing_index.form_type_id if django_filing_index.form_type_id else None,
                cik=django_filing_index.cik_id,
                date_filed=django_filing_index.date_filed,
                filename=django_filing_index.filename,
                accession_number=django_filing_index.accession_number
            )
            session.add(filing_index)
        
        # Migrate Filing
        print("Migrating Filing data...")
        filings = DjangoFiling.objects.all()
        for django_filing in filings:
            filing = Filing(
                form_id=django_filing.form_type_id if django_filing.form_type_id else None,
                cik_id=django_filing.cik_id,
                date_filed=django_filing.date_filed,
                accession_number=django_filing.accession_number,
                file_number=django_filing.file_number,
                path=django_filing.path,
                extracted=django_filing.extracted,
                processed=django_filing.processed,
                error=django_filing.error,
                company_name=django_filing.company_name,
                company=django_filing.company,
                filing_html_index=django_filing.filing_html_index,
                homepage_url=django_filing.homepage_url,
                text_url=django_filing.text_url
            )
            session.add(filing)
        
        # Commit to ensure foreign key references exist
        session.commit()
        
        # Migrate FactIndex
        print("Migrating FactIndex data...")
        fact_indices = DjangoFactIndex.objects.all()
        for django_fact_index in fact_indices:
            fact_index = FactIndex(
                fact=django_fact_index.fact,
                label=django_fact_index.label,
                description=django_fact_index.description
            )
            session.add(fact_index)
        
        # Commit to ensure foreign key references exist
        session.commit()
        
        # Migrate CompanyFact
        print("Migrating CompanyFact data...")
        company_facts = DjangoCompanyFact.objects.all()
        for django_company_fact in company_facts:
            company_fact = CompanyFact(
                id=django_company_fact.id,
                cik_id=django_company_fact.cik_id,
                accession_number=django_company_fact.accession_number,
                fact_id=django_company_fact.fact_id,
                namespace=django_company_fact.namespace,
                value=django_company_fact.value,
                end_date=django_company_fact.end_date,
                datefiled=django_company_fact.datefiled,
                fiscal_year=django_company_fact.fiscal_year,
                fiscal_period=django_company_fact.fiscal_period,
                formtype_id=django_company_fact.formtype_id,
                frame=django_company_fact.frame
            )
            session.add(company_fact)
        
        # Migrate FilingDocument
        print("Migrating FilingDocument data...")
        filing_documents = DjangoFilingDocument.objects.all()
        for django_filing_document in filing_documents:
            filing_document = FilingDocument(
                filing_id=django_filing_document.filing_id,
                type=django_filing_document.type,
                sequence=django_filing_document.sequence,
                file_name=django_filing_document.file_name,
                content_type=django_filing_document.content_type,
                description=django_filing_document.description,
                sha1=django_filing_document.sha1,
                start_pos=django_filing_document.start_pos,
                end_pos=django_filing_document.end_pos,
                is_processed=django_filing_document.is_processed,
                is_error=django_filing_document.is_error
            )
            session.add(filing_document)
        
        # Migrate SearchQuery
        print("Migrating SearchQuery data...")
        search_queries = DjangoSearchQuery.objects.all()
        for django_search_query in search_queries:
            search_query = SearchQuery(
                id=django_search_query.id,
                form_type=django_search_query.form_type,
                date_created=django_search_query.date_created,
                date_completed=django_search_query.date_completed
            )
            session.add(search_query)
        
        # Commit to ensure foreign key references exist
        session.commit()
        
        # Migrate SearchQueryTerm
        print("Migrating SearchQueryTerm data...")
        search_query_terms = DjangoSearchQueryTerm.objects.all()
        for django_search_query_term in search_query_terms:
            search_query_term = SearchQueryTerm(
                search_query_id=django_search_query_term.search_query_id,
                term=django_search_query_term.term
            )
            session.add(search_query_term)
        
        # Commit to ensure foreign key references exist
        session.commit()
        
        # Migrate SearchQueryResult
        print("Migrating SearchQueryResult data...")
        search_query_results = DjangoSearchQueryResult.objects.all()
        for django_search_query_result in search_query_results:
            search_query_result = SearchQueryResult(
                search_query_id=django_search_query_result.search_query_id,
                filing_document_id=django_search_query_result.filing_document_id,
                term_id=django_search_query_result.term_id,
                count=django_search_query_result.count
            )
            session.add(search_query_result)
        
        # Final commit
        session.commit()
        print("Data migration completed successfully.")
        
    except Exception as e:
        session.rollback()
        print(f"Error during migration: {e}")
        raise
    finally:
        session.close()


def check_tables():
    """Check if all tables exist in the database."""
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    expected_tables = [
        'openedgar_company',
        'openedgar_companyinfo',
        'openedgar_formindex',
        'openedgar_bulkfilingindex',
        'openedgar_filingindex',
        'openedgar_filing',
        'openedgar_factindex',
        'openedgar_companyfact',
        'openedgar_filingdocument',
        'openedgar_searchquery',
        'openedgar_searchqueryterm',
        'openedgar_searchqueryresult'
    ]
    
    missing_tables = [table for table in expected_tables if table not in tables]
    
    if missing_tables:
        print(f"Missing tables: {', '.join(missing_tables)}")
        return False
    else:
        print("All tables exist.")
        return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='SQLAlchemy database utilities')
    parser.add_argument('--create', action='store_true', help='Create all tables')
    parser.add_argument('--drop', action='store_true', help='Drop all tables')
    parser.add_argument('--migrate', action='store_true', help='Migrate data from Django ORM')
    parser.add_argument('--check', action='store_true', help='Check if all tables exist')
    
    args = parser.parse_args()
    
    if args.drop:
        drop_tables()
    
    if args.create:
        create_tables()
    
    if args.check:
        check_tables()
    
    if args.migrate:
        migrate_data()

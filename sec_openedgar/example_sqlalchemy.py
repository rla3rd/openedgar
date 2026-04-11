#!/usr/bin/env python
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

"""
Example script demonstrating how to use the SQLAlchemy models in the OpenEdgar project.
This script shows common operations and patterns for working with the database.
"""

import os
import sys
import datetime

# Add the project path to the sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import SQLAlchemy components
from openedgar.db import Session
from openedgar.db.models import (
    Company, CompanyInfo, Filing, FormIndex, FactIndex, CompanyFact
)
from openedgar.db.helpers import session_scope, get_or_create, filter_queryset, order_by_queryset


def example_basic_operations():
    """Demonstrate basic CRUD operations with SQLAlchemy."""
    print("\n=== Basic CRUD Operations ===\n")
    
    # Create a session
    session = Session()
    
    try:
        # Create: Add a new company
        print("Creating a new company...")
        company = Company(cik=999999999, cik_name="Example Corp")
        session.add(company)
        session.flush()  # Flush to get the ID without committing
        print(f"Created company: {company}")
        
        # Create: Add company info for the company
        print("\nAdding company info...")
        company_info = CompanyInfo(
            cik_id=company.cik,
            name="Example Corporation",
            is_company=True,
            industry="Technology",
            sic="7370",
            sic_description="Services-Computer Programming, Data Processing, Etc.",
            asof=datetime.date.today()
        )
        session.add(company_info)
        session.flush()
        print(f"Added company info: {company_info}")
        
        # Read: Query the company
        print("\nQuerying the company...")
        queried_company = session.query(Company).filter_by(cik=999999999).first()
        print(f"Queried company: {queried_company}")
        
        # Read: Query with a join to get company info
        print("\nQuerying with a join...")
        result = session.query(Company, CompanyInfo).\
            join(CompanyInfo, Company.cik == CompanyInfo.cik_id).\
            filter(Company.cik == 999999999).first()
        if result:
            company, info = result
            print(f"Company: {company.cik_name}, Industry: {info.industry}")
        
        # Update: Modify the company name
        print("\nUpdating the company...")
        queried_company.cik_name = "Example Corp Updated"
        session.flush()
        print(f"Updated company: {queried_company}")
        
        # Delete: Remove the company and its info (cascade delete)
        print("\nDeleting the company...")
        session.delete(queried_company)  # This will cascade delete company_info
        
        # Rollback changes for this example
        print("\nRolling back changes for this example...")
        session.rollback()
        print("Changes rolled back.")
        
    finally:
        # Always close the session
        session.close()


def example_using_helpers():
    """Demonstrate using the helper functions that mimic Django ORM."""
    print("\n=== Using Helper Functions (Django-like API) ===\n")
    
    with session_scope() as session:
        # Get or create a form type
        print("Using get_or_create...")
        form_index, created = get_or_create(
            session, FormIndex, form="10-K", defaults={"description": "Annual report"}
        )
        if created:
            print(f"Created new form index: {form_index}")
        else:
            print(f"Found existing form index: {form_index}")
        
        # Filter queryset
        print("\nUsing filter_queryset...")
        companies = filter_queryset(session, Company, cik_name__contains="Corp")
        print(f"Found {companies.count()} companies with 'Corp' in their name")
        
        # Order by
        print("\nUsing order_by_queryset...")
        ordered_companies = order_by_queryset(companies, "-cik")
        for company in ordered_companies.limit(5).all():
            print(f"Company: {company.cik} - {company.cik_name}")


def example_relationships():
    """Demonstrate working with relationships in SQLAlchemy."""
    print("\n=== Working with Relationships ===\n")
    
    with session_scope() as session:
        # Query a company with its filings
        companies = session.query(Company).limit(1).all()
        
        if companies:
            company = companies[0]
            print(f"Company: {company.cik} - {company.cik_name}")
            
            # Access related filings
            print(f"\nFilings for company {company.cik}:")
            for filing in company.filings[:5]:  # Limit to first 5
                print(f"  Filing: {filing.accession_number} - {filing.form_id} - {filing.date_filed}")
            
            # Access company info
            if company.company_info:
                info = company.company_info
                print(f"\nCompany Info for {company.cik}:")
                print(f"  Name: {info.name}")
                print(f"  Industry: {info.industry}")
                print(f"  SIC: {info.sic} - {info.sic_description}")


def example_complex_queries():
    """Demonstrate more complex queries with SQLAlchemy."""
    print("\n=== Complex Queries ===\n")
    
    with session_scope() as session:
        # Query companies with recent filings
        print("Companies with recent 10-K filings:")
        one_year_ago = datetime.date.today() - datetime.timedelta(days=365)
        
        recent_filings = session.query(Company, Filing).\
            join(Filing, Company.cik == Filing.cik_id).\
            filter(Filing.form_id == "10-K").\
            filter(Filing.date_filed >= one_year_ago).\
            order_by(Filing.date_filed.desc()).\
            limit(5).all()
        
        for company, filing in recent_filings:
            print(f"Company: {company.cik_name}, Filing Date: {filing.date_filed}")
        
        # Aggregate query - count filings by form type
        print("\nCounting filings by form type:")
        from sqlalchemy import func
        
        filing_counts = session.query(FormIndex.form, func.count(Filing.id).label("count")).\
            join(Filing, FormIndex.form == Filing.form_id).\
            group_by(FormIndex.form).\
            order_by(func.count(Filing.id).desc()).\
            limit(5).all()
        
        for form, count in filing_counts:
            print(f"Form {form}: {count} filings")


def main():
    print("SQLAlchemy Example Script for OpenEdgar")
    print("This script demonstrates how to use the SQLAlchemy models.")
    print("Note: This script only demonstrates the queries without committing changes.")
    
    # Run examples
    example_basic_operations()
    example_using_helpers()
    example_relationships()
    example_complex_queries()
    
    print("\nExamples completed.")


if __name__ == "__main__":
    main()

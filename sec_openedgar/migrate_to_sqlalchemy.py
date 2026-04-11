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

import os
import sys
import argparse

# Add the project path to the sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


def install_dependencies():
    """Install required SQLAlchemy dependencies."""
    import subprocess
    print("Installing SQLAlchemy dependencies...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements_sqlalchemy.txt"])
    print("Dependencies installed successfully.")


def run_migration():
    """Run the migration process from Django ORM to SQLAlchemy."""
    from openedgar.db.utils import create_tables, migrate_data, check_tables
    
    print("Step 1: Creating SQLAlchemy tables...")
    create_tables()
    
    print("\nStep 2: Migrating data from Django ORM to SQLAlchemy...")
    migrate_data()
    
    print("\nStep 3: Verifying migration...")
    check_tables()
    
    print("\nMigration completed successfully!")


def show_example():
    """Show an example of converting Django ORM code to SQLAlchemy."""
    print("\n=== Example: Converting Django ORM to SQLAlchemy ===\n")
    
    print("Django ORM code:")
    print("""
    # Get a company by CIK
    company = Company.objects.get(cik=1234567890)
    
    # Get all filings for a company
    filings = Filing.objects.filter(cik=company.cik).order_by('-date_filed')
    
    # Create a new company
    new_company = Company.objects.create(cik=9876543210, cik_name="New Company Corp")
    
    # Update a company
    company.cik_name = "Updated Name"
    company.save()
    
    # Delete a company
    company.delete()
    """)
    
    print("\nSQLAlchemy equivalent:")
    print("""
    from openedgar.db import Session
    from openedgar.db.models import Company, Filing
    from openedgar.db.helpers import session_scope
    
    # Using context manager for session
    with session_scope() as session:
        # Get a company by CIK
        company = session.query(Company).filter_by(cik=1234567890).first()
        
        # Get all filings for a company
        filings = session.query(Filing).filter_by(cik_id=company.cik).order_by(Filing.date_filed.desc()).all()
        
        # Create a new company
        new_company = Company(cik=9876543210, cik_name="New Company Corp")
        session.add(new_company)
        
        # Update a company
        company.cik_name = "Updated Name"
        
        # Delete a company
        session.delete(company)
        
        # Changes are committed automatically when the context manager exits
    """)
    
    print("\n=== Using Helper Functions (Django-like API) ===\n")
    print("""
    from openedgar.db.helpers import session_scope, filter_queryset, order_by_queryset, get_or_create
    from openedgar.db.models import Company, Filing
    
    with session_scope() as session:
        # Get a company by CIK
        company = filter_queryset(session, Company, cik=1234567890).first()
        
        # Get all filings for a company
        filings_query = filter_queryset(session, Filing, cik_id=company.cik)
        filings = order_by_queryset(filings_query, '-date_filed').all()
        
        # Get or create a company
        company, created = get_or_create(session, Company, cik=9876543210, cik_name="New Company Corp")
        if created:
            print("Created new company")
        else:
            print("Found existing company")
    """)


def main():
    parser = argparse.ArgumentParser(description='Migrate from Django ORM to SQLAlchemy')
    parser.add_argument('--install', action='store_true', help='Install SQLAlchemy dependencies')
    parser.add_argument('--migrate', action='store_true', help='Run the migration process')
    parser.add_argument('--example', action='store_true', help='Show code conversion examples')
    
    args = parser.parse_args()
    
    if args.install:
        install_dependencies()
    
    if args.migrate:
        run_migration()
    
    if args.example:
        show_example()
    
    # If no arguments provided, show help
    if not (args.install or args.migrate or args.example):
        parser.print_help()


if __name__ == "__main__":
    main()

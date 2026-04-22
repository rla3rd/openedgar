import os
import pandas as pd
from django.core.management.base import BaseCommand
from django.conf import settings
from openedgar.sec_api import sec_api
from openedgar.models import Filing, Company, FormIndex
from django.db import transaction

class Command(BaseCommand):
    help = 'Syncs the SEC Master Index into PostgreSQL for precision filing discovery'

    def add_arguments(self, parser):
        parser.add_argument('--year', nargs='+', type=int, required=True, help='Year(s) to sync')
        parser.add_argument('--qtr', nargs='+', type=int, default=[1, 2, 3, 4], help='Quarter(s) to sync (1-4)')
        parser.add_argument('--forms', nargs='+', help='Optional: Filter to specific form types (e.g. 3 4 5)')

    def handle(self, *args, **options):
        years = options['year']
        qtrs = options['qtr']
        for year in years:
            for qtr in qtrs:
                self.process_quarter(year, qtr)

    def process_quarter(self, year, qtr):
        self.stdout.write(self.style.NOTICE(f"\nProcessing SEC Master Index (Full Firehose): {year} QTR{qtr}..."))
        
        df = sec_api.get_filings_for_quarter(year, qtr)
        if df.empty:
            self.stdout.write(self.style.ERROR("No filings found for this period."))
            return

        total_rows = len(df)
        self.stdout.write(f"Processing {total_rows} filings...")

        # Prepare path mapping logic (Vectorized)
        df['year_str'] = df['filing_date'].str[:4]
        df['month_str'] = df['filing_date'].str[5:7]
        df['day_str'] = df['filing_date'].str[8:10]
        
        df['local_path'] = (
            "data/" + df['year_str'] + "/QTR" + str(qtr) + "/" + 
            df['month_str'] + "/" + df['day_str'] + "/" + 
            df['accession_number'] + ".sgml.zst"
        )

        # 1. Ensure Companies exist (Unique CIKs)
        unique_ciks = df[['cik', 'company']].drop_duplicates('cik')
        self.stdout.write(f"Ensuring {len(unique_ciks)} companies exist in DB...")
        
        companies_to_create = []
        existing_ciks = set(Company.objects.filter(cik__in=unique_ciks['cik'].tolist()).values_list('cik', flat=True))
        
        for _, row in unique_ciks.iterrows():
            if row['cik'] not in existing_ciks:
                companies_to_create.append(Company(cik=row['cik'], cik_name=row['company']))
        
        if companies_to_create:
            Company.objects.bulk_create(companies_to_create, ignore_conflicts=True)

        # 2. Ensure Form Indexes exist
        unique_forms = df['form'].unique()
        for ftype in unique_forms:
            FormIndex.objects.get_or_create(form=ftype)

        # 3. Bulk Upsert filings
        self.stdout.write("Performing bulk ingestion into Filing table...")
        
        batch_size = 5000
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            filings = []
            for _, row in batch_df.iterrows():
                filings.append(Filing(
                    accession_number=row['accession_number'],
                    cik_id=row['cik'],
                    form_type_id=row['form'],
                    date_filed=row['filing_date'],
                    company=row['company'],
                    path=row['local_path'],
                    document_url=row['document_url'],
                    text_url=row['text_url'],
                    homepage_url=row['homepage_url'],
                    is_processed=False
                ))
            
            Filing.objects.bulk_create(filings, ignore_conflicts=True)
            self.stdout.write(f"  Processed {min(i + batch_size, total_rows)}/{total_rows}...")

        self.stdout.write(self.style.SUCCESS(f"Successfully synced Master Index for {year} Q{qtr}."))

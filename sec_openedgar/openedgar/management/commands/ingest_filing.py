import logging
from django.core.management.base import BaseCommand
from openedgar.models import Company, CompanyInfo, Filing, FormIndex
from openedgar.processes.rag_pipeline import ModernRAGPipeline

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Ingest a specific SEC filing by Ticker or CIK using edgartools and index it in the HyperstreamDB RAG database."

    def add_arguments(self, parser):
        parser.add_argument('--identifier', type=str, required=True, help="Stock ticker symbol or CIK number (e.g. AAPL or 0000320193)")
        parser.add_argument('--form', type=str, default='10-K', help="Filing form type to ingest (default: 10-K)")
        parser.add_argument('--count', type=int, default=1, help="Number of recent filings of this type to ingest (default: 1)")

    def handle(self, *args, **options):
        identifier = options['identifier'].strip().upper()
        form_type = options['form'].strip().upper()
        count = options['count']

        self.stdout.write(self.style.WARNING(f"Initiating edgartools lookup for: {identifier}..."))

        try:
            from edgar import Company as SECCompany
            sec_company = SECCompany(identifier)
            if not sec_company:
                self.stderr.write(self.style.ERROR(f"Could not find company matching identifier: {identifier}"))
                return

            self.stdout.write(self.style.SUCCESS(f"Found Company: {sec_company.name} (CIK: {sec_company.cik})"))

            # 1. Sync Company Record
            company, created_co = Company.objects.get_or_create(
                cik=sec_company.cik,
                defaults={
                    'cik_name': sec_company.name,
                    'ticker': sec_company.tickers[0] if (hasattr(sec_company, 'tickers') and sec_company.tickers) else (identifier if len(identifier) < 10 else "")
                }
            )
            if created_co:
                self.stdout.write(f"Created local Company record for {company.cik_name}")

            # 2. Sync CompanyInfo Record
            info, created_info = CompanyInfo.objects.get_or_create(
                cik=company,
                defaults={
                    'name': sec_company.name,
                    'is_company': True,
                    'sic': str(sec_company.sic) if sec_company.sic else None,
                    'sic_description': getattr(sec_company, 'sic_description', "") or "",
                    'insider_transaction_for_owner_exists': 0,
                    'insider_transaction_for_issuer_exists': 0,
                }
            )

            # 3. Retrieve Filings from SEC
            self.stdout.write(f"Fetching latest {count} {form_type} filings from SEC...")
            sec_filings = sec_company.get_filings(form=form_type)
            
            if not sec_filings:
                self.stderr.write(self.style.ERROR(f"No filings found for {company.cik_name} of type {form_type}."))
                return

            pipeline = ModernRAGPipeline()
            ingested_count = 0

            # Process up to the requested count of filings
            for i, sec_filing in enumerate(sec_filings[:count]):
                accession = sec_filing.accession_no
                self.stdout.write(self.style.WARNING(f"[{i+1}/{count}] Processing filing {accession}..."))

                # Ensure FormIndex exists
                FormIndex.objects.get_or_create(form=form_type)

                # 4. Sync Filing Record
                filing, created_filing = Filing.objects.get_or_create(
                    accession_number=accession,
                    defaults={
                        'company': company.cik_name,
                        'form_type_id': form_type,
                        'date_filed': sec_filing.filing_date,
                        'cik': company,
                        'path': sec_filing.document or "",
                    }
                )

                # 5. Extract Markdown and Ingest into HyperstreamDB RAG vector space
                if created_filing or not filing.is_processed:
                    try:
                        self.stdout.write(f"Extracting raw filing text using edgartools...")
                        raw_text = sec_filing.markdown()
                        
                        self.stdout.write(f"Indexing fragments into HyperstreamDB vector database...")
                        pipeline.ingest_filing_chunks(
                            cik=company.cik,
                            accession_number=accession,
                            form_type=form_type,
                            date_filed=str(sec_filing.filing_date),
                            markdown=raw_text
                        )
                        
                        filing.is_processed = True
                        filing.is_error = False
                        filing.save()
                        self.stdout.write(self.style.SUCCESS(f"Successfully processed and indexed filing {accession}."))
                        ingested_count += 1
                    except Exception as e:
                        filing.is_error = True
                        filing.save()
                        self.stderr.write(self.style.ERROR(f"Failed to ingest filing {accession}: {str(e)}"))
                else:
                    self.stdout.write(self.style.SUCCESS(f"Filing {accession} has already been processed and indexed."))

            self.stdout.write(self.style.SUCCESS(f"Finished! Ingested/verified {ingested_count} new filings for {company.cik_name}."))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Ingestion run encountered a fatal error: {str(e)}"))
            logger.exception("Fatal error in ingest_filing command")

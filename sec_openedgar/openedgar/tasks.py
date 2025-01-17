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

# Libraries
import sys
import traceback
import datetime
import hashlib
import logging
import tempfile
import os
import pathlib
import tarfile
import io
import zstd
import orjson as json
import pandas as pd
from bs4 import BeautifulSoup
from typing import Iterable, Union, Optional, Dict, List

# Packages
import dateutil.parser
import django.db.utils
from celery import shared_task

# Project
from config.settings.base import S3_DOCUMENT_PATH
from openedgar.clients.s3 import S3Client
from openedgar.clients.local import LocalClient
from edgar.entities import NoCompanyFactsFound
import openedgar.clients.openedgar
import openedgar.parsers.openedgar
from openedgar.models import Company
from openedgar.models import CompanyFact
from openedgar.models import CompanyInfo
from openedgar.models import FactIndex
from openedgar.models import FilingIndex
from openedgar.models import Filing
from openedgar.models import FilingDocument
from openedgar.models import FormIndex
from openedgar.models import SearchQuery
from openedgar.models import SearchQueryTerm
from openedgar.models import SearchQueryResult
 
# edgartools
import edgar
from edgar import forms as frm
edgar.use_local_storage()

# import tabula for formtypes
import tabula

# LexNLP imports
import lexnlp.nlp.en.tokens

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

def download_data():
    edgar.download_edgar_data()
    
def download_bulk_filings(year=None, qtr=None, verbose=False, backfill=False):
    data_dir = pathlib.Path(os.getenv("EDGAR_LOCAL_DATA_DIR"))
    base_url = 'https://www.sec.gov/Archives/edgar/Feed/'
    page = edgar.httprequests.get_with_retry(base_url)
    soup = BeautifulSoup(page, features="lxml")
    table = soup.find("table")
    
    begin_year = 1997
    if backfill:
        if year:
            begin_year = year
        years = []
        for tr in table.find_all('tr'):
            row = tr.find_all('td')
            if len(row) > 0:
                yr = row[0].find('a').attrs['href'].replace("/", "")
                if yr != "":
                    if int(yr) >= begin_year:
                        years.append(int(yr))
    else:
        years = [year]
    years.sort()
    for year in years:
        year_page = edgar.httprequests.get_with_retry(f"{base_url}{year}/")
        year_soup = BeautifulSoup(year_page, features="lxml")
        year_table = year_soup.find("table")
        if qtr:
            quarters = [f"QTR{qtr}"]
        else:
            quarters = []
            for tr in year_table.find_all('tr'):
                row = tr.find_all('td')
                if len(row) > 0:
                    text_qtr = row[0].find('a').attrs['href'].replace("/", "")
                    if text_qtr in ("QTR1", "QTR2", "QTR3", "QTR4"):
                        quarters.append(text_qtr)
        for quarter in quarters:
            quarter_url = f"{base_url}{year}/{quarter}/"
            if verbose:
                print(quarter_url)
            quarter_page = edgar.httprequests.get_with_retry(quarter_url)
            quarter_soup = BeautifulSoup(quarter_page, features="lxml")
            quarter_table = quarter_soup.find("table")
            for tr in quarter_table.find_all('tr'):
                row = tr.find_all('td')
                if len(row) > 0:
                    filename = row[0].find('a').attrs['href']
                    if filename.split(".")[-1] == 'gz':
                        file_url = f"{base_url}{year}/{quarter}/{filename}"
                        if verbose:
                            print(file_url)
                        try:
                            file_data = edgar.httprequests.get_with_retry(file_url)
                            with tarfile.open(fileobj=io.BytesIO(file_data.content)) as tar:
                                for member in tar.getmembers():
                                    if member.isreg():
                                        f = tar.extractfile(member)
                                        file_path = data_dir / "data" / str(year) / quarter / f"{member.name}.zstd"
                                        content = f.read()
                                        compressed_content=zstd.compress(content)
                                        file_path.parent.mkdir(parents=True, exist_ok=True)
                                        file = file_path.open('wb')
                                        file.write(compressed_content)
                                        file.flush()
                                        file.close()
                                tar.close()
                        except Exception:
                            error = sys.exc_info()[0]
                            details = traceback.format_exc()
                            sys.stderr.write(f'{file_url}: {error} - {details}')
                            

def process_formtypes():
    try:
        pdf_path = "https://www.sec.gov/info/edgar/forms/edgform.pdf"
        dfs = tabula.read_pdf(pdf_path, user_agent=os.getenv('EDGAR_IDENTITY'), pages='2-31', lattice=True)
        forms = pd.concat(dfs)
        forms['Submission Type'] = forms['Submission Type'].str.replace('\r', ' ')
        forms.reset_index(inplace=True, drop=True)
        forms = forms[['Submission Type', 'Description']][~pd.isnull(forms['Submission Type'])].copy()
        forms['Submission Type'] = forms['Submission Type'].str.split(', ')
        forms = forms.explode('Submission Type')
        forms.rename(columns={'Submission Type': 'Form'}, inplace=True)
        forms.reset_index(inplace=True, drop=True)
        for form in forms.itertuples():
            try:
                f = FormIndex.objects.get(form=form.Form)
            except:
                f = FormIndex()

            f.form = form.Form
            f.description = form.Description
            f.save()
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')

def process_company():
    """
    populate company table
    """
    try:
        df = edgar.get_cik_lookup_data()
        ciknames = {}
        company_objects = []
        for row in df.itertuples():
            ciknames[row.cik] = row.name
        for cik in ciknames.keys():
            company_objects.append(
                Company(cik=cik, cik_name=ciknames[cik]))
        Company.objects.bulk_create(
            company_objects,
            update_conflicts=True, 
            update_fields=['cik_name'],
            unique_fields=['cik'])
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')
        
def process_companyinfo_cik(cik:int):
    try:
        processed = False
        company = Company.objects.get(cik=cik)
        c = edgar.Company(company.cik)
        cik = company.cik
        ci = CompanyInfo()
        ci.cik = company
        ci.name = c.name
        ci.is_company = c.is_company
        ci.category = c.category
        ci.description = c.description
        ci.entity_type = c.entity_type
        ci.ein = c.ein
        ci.industry = c.industry
        ci.sic = c.sic
        ci.sic_description = c.sic_description
        ci.state_of_incorporation = c.state_of_incorporation
        ci.state_of_incorporation_description = c.state_of_incorporation_description
        ci.fiscal_year_end = c.fiscal_year_end
        ci.mailing_address = c.mailing_address.__dict__
        ci.business_addres = c.business_address.__dict__
        ci.phone = c.phone
        ci.tickers = c.tickers
        ci.exchanges = c.exchanges
        ci.former_names = c.former_names
        ci.flags = c.flags
        ci.insider_transaction_for_owner_exists = c.insider_transaction_for_owner_exists
        ci.insider_transaction_for_issuer_exists = c.insider_transaction_for_issuer_exists
        ci.website = c.website
        ci.investor_website = c.investor_website
        try:
            oci = CompanyInfo.objects.get(cik=c.cik)
        except CompanyInfo.DoesNotExist:
            oci = None
        if oci != ci:
            ci.processed = True
            ci.save()
        return processed
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{cik}: {error} - {details}')

@shared_task
def process_companyinfo(cik:int=0, multiple:bool=False, upsert:bool=False):
    try:
        print('Getting Company Objects')
        company = None
        if cik == 0 or multiple:
            if not upsert:
                # ciks in company not in companyinfo
                company_ciks = set(
                    Company.objects \
                        .all() \
                        .filter(cik__gte=cik) \
                        .order_by('cik') \
                        .values_list('cik', flat=True))
                companyinfo_ciks = set(
                    CompanyInfo.objects \
                        .all() \
                        .filter(cik__gte=cik) \
                        .order_by('cik') \
                        .values_list('cik', flat=True))
                get_ciks = company_ciks.union(companyinfo_ciks) \
                    - company_ciks.intersection(companyinfo_ciks)
                companies = Company.objects.all().filter(cik__in=get_ciks).order_by('cik')
            else:
                # ciks >= passed in cik
                companies = Company.objects.all().filter(cik__gte=cik).order_by('cik')
        else:
            # single cik
            companies = [Company.objects.get(cik=cik)]
        print(f'Got {len(companies)} Company Objects')
        i = 0
        total = len(companies)
        for company in companies:
            i += 1
            processed = process_companyinfo_cik(company.cik)
            if processed:
                action = 'processed'
            else:
                action = 'skipped'
            print(f"{action} {company.cik}: {i} of {total}")
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        if company is None:
            err_cik = cik
        else:
            err_cik = company.cik
        sys.stderr.write(f'{err_cik}: {error} - {details}')
        
def process_companyfacts_bulk():
    data_path = pathlib.Path(os.getenv('EDGAR_LOCAL_DATA_DIR'))
    facts_path = data_path / 'companyfacts'
    facts_files = facts_path.glob('*.json')
    
    results = []
    for filenm in facts_files:
        cik = int(filenm.stem[3:])
        res = process_companyfacts_cik.s(cik).apply_async(serializer='json')
        results.append(res)
    for res in results:
        res.get()
    
@shared_task
def process_companyfacts_cik(cik:int):
    processed = False
    try:
        company = edgar.Company(cik)
        if company is not None:
            facts = company.get_facts()
            if facts is not None:
                if hasattr(facts, 'fact_meta'):
                    for fact_index in facts.fact_meta.itertuples():
                        try:
                            fi = FactIndex.objects.get(fact=fact_index.fact)
                        except FactIndex.DoesNotExist:
                            fi = FactIndex()
                        fi.fact = fact_index.fact
                        fi.label = fact_index.label
                        fi.description = fact_index.description
                        fi.save()
                    facts = facts.to_pandas()
                    facts['id'] = facts.fact + '_' + facts.accn
                    facts.drop_duplicates(subset=['id'], keep='last', inplace=True)
                    fact_objects = []
                    for fact in facts.itertuples():
                        try:
                            ft = FormIndex.objects.get(form=fact.form)
                        except FormIndex.DoesNotExist:
                            ft = FormIndex.objects.create(form=fact.form)
                        try:
                            fact_idx = FactIndex.objects.get(fact=fact.fact)
                        except FactIndex.DoesNotExist:
                            fact_idx = FactIndex().objects.create(fact=fact.fact)
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
                            cf = CompanyFact.objects.get(id=fact.id)
                        except CompanyFact.DoesNotExist:
                            cf = CompanyFact(
                                id=fact.id,
                                cik=c,
                                fact=fact_idx,
                                formtype=ft,
                                namespace=fact.namespace,
                                value=fact.val,
                                end_date=fact.end,
                                datefiled=fact.filed,
                                fiscal_year=fact.fy,
                                fiscal_period=fact.fp,
                                frame=fact.frame,
                                accession_number=fi)
                        
                        cf.cik = c
                        cf.accession_number = fi
                        cf.fact = fact_idx
                        cf.formtype = ft
                        cf.id = fact.id
                        cf.namespace = fact.namespace
                        cf.value = fact.val
                        cf.end_date = fact.end
                        cf.datefiled = fact.filed
                        cf.fiscal_year = fact.fy
                        cf.fiscal_period = fact.fp
                        cf.frame = fact.frame
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
                    processed = True
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')
        processed = False
    finally:
        print(cik, processed)
    return processed
        
@shared_task
def process_companyfacts(cik:int=0, multiple:bool=False, upsert:bool=False):
    try:
        print('Getting CompanyInfo Objects')
        company = None
        if cik == 0 or multiple:
            if not upsert:
                # ciks in company not in companyinfo
                company_ciks = set(
                    CompanyInfo.objects \
                        .all() \
                        .filter(cik__gte=cik) \
                        .filter(is_company=True) \
                        .order_by('cik') \
                        .values_list('cik', flat=True))
                companyfact_ciks = set(
                    CompanyFact.objects \
                        .all() \
                        .filter(cik__gte=cik) \
                        .order_by('cik') \
                        .values_list('cik', flat=True))
                ciks = company_ciks.union(companyfact_ciks) \
                    - company_ciks.intersection(companyfact_ciks)
              
            else:
                # ciks >= passed in cik 
                ciks = CompanyInfo.objects \
                    .all() \
                    .filter(cik__gte=cik) \
                    .filter(is_company=True) \
                    .order_by('cik') \
                    .values_list('cik', flat=True)
        else:
            # single cik
            ciks = CompanyInfo.objects \
                .get(cik=cik) \
                .filter(is_company=True) \
                .values_list('cik', flat=True)
        print(f'Got {len(ciks)} CIKs')
        i = 0
        total = len(ciks)
        for cik in ciks:
            i += 1
            processed = process_companyfacts_cik(cik)
            if processed:
                action = 'processed'
            else:
                action = 'skipped'
            print(f"{action} {cik}: {i} of {total}")
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        if company is None:
            err_cik = cik
        else:
            err_cik = company.cik
        sys.stderr.write(f'{err_cik}: {error} - {details}')

def process_filingindex_year(year:int, batch_size:int=1000, upsert:bool=False, formtypes:Iterable[str]=None):
    filing_index = edgar.get_filings(year, form=formtypes)
    if filing_index is not None:
        filing_index = filing_index.to_pandas()
        if formtypes is not None:
            filing_index = filing_index[filing_index['form'].isin(formtypes)]
        if not upsert:
            accnos = FilingIndex.objects.all() \
                .filter(date_filed__gte=datetime.date(year, 1, 1)) \
                .filter(date_filed__lte=datetime.date(year, 12, 31)) \
                .order_by('accession_number') \
                .values_list('accession_number', flat=True)
            filing_index = filing_index[~(filing_index['accession_number'].isin(accnos))]
        filing_ct: int = filing_index.shape[0]
        for start in range(0, filing_ct, batch_size):
            end = min(start + batch_size, filing_ct)
            filing_objects = []
            filing_index.drop_duplicates(subset=['accession_number'], keep='last', inplace=True)
            for filing in filing_index.iloc[start:end].itertuples():
                f = FilingIndex()
                try:
                    company = Company.objects.get(cik=filing.cik)
                except Company.DoesNotExist:
                    company = Company.objects.create(cik=filing.cik, cik_name=filing.company)
                f.accession_number = filing.accession_number
                try:
                    frm = FormIndex.objects.get(form=filing.form)
                except FormIndex.DoesNotExist:
                    frm = FormIndex.objects.create(form=filing.form)
                f.form_type = frm
                f.date_filed = filing.filing_date
                f.cik = company
                f.company = filing.company
                try:
                    of = FilingIndex.objects.get(accession_number=f.accession_number)
                except FilingIndex.DoesNotExist:
                    of = None
                if f != of:
                    filing_objects.append(f)
            FilingIndex.objects.bulk_create(
                filing_objects,
                update_conflicts=True, 
                unique_fields=['accession_number'],
                update_fields=['cik', 'company', 'form_type', 'date_filed']
                )
            print(f"FilingIndex Batch {year}: {start} - {end}")
            
@shared_task  
def process_filingindex(backfill:bool=False, upsert:bool=False, formtypes:Iterable[str]=None):
    try:
        if not backfill:
            """
            Get list of index files for a given year.
            :param year: filing year to retrieve
            :return:
            """
            year = datetime.date.today().year
            
            # Log entrance
            logger.info("Locating form index list for {0}".format(year))

            # Form index dataframe
            process_filingindex_year(year, upsert=upsert)
          
        else:
            min_year: int = 1950
            max_year: int = 2050
            # Log entrance
            logger.info("Retrieving form index list")

            # Retrieve dataframe
            for year in range(min_year, max_year + 1):
                process_filingindex_year(year, upsert=upsert)
        
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')
        
def process_filings(year:int=None, upsert=False, backfill:bool=False):
    try:
        if not backfill:
            """
            Get list of index files for a given year.
            :param year: filing year to retrieve
            :return:
            """
            if year is None:
                year = datetime.date.today().year
            
            # Log entrance
            logger.info("Locating form index list for {0}".format(year))

            # Form index dataframe
            filings = edgar.get_filings(year)
            filing_ct: int = len(filings)
            
            # Log exit
            logger.info("Successfully located {0} form index files for {1}".format(filing_ct, year))
        else:
            min_year: int = 1950
            max_year: int = 2050
            # Log entrance
            logger.info("Retrieving form index list")

            # Retrieve dataframe
            filings = edgar.get_filings(range(min_year, max_year + 1))
            filing_ct: int = len(filings)
            
            # Log exit
            logger.info("Successfully located {0} form index files from {1} to {2}".format(filing_ct, min_year, max_year))
        
        filing_objects = []  
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
            f.text_url = filing.text_url
            try:
                of = Filing.objects.get(accession_number=f.accession_number)
            except Filing.DoesNotExist:
                of = None
            if f != of:
                filing_objects.append(f)
        Filing.objects.bulk_create(
            filing_objects,
            update_conflicts=True, 
            unique_fields=['accession_number'])
    except Exception:
        error = sys.exc_info()[0]
        details = traceback.format_exc()
        sys.stderr.write(f'{error} - {details}')

# this really really looks like its the full text document being used, due to start_pos and end_pos                
def create_filing_documents(client, documents, filing, store_raw: bool = True, store_text: bool = True):
    """
    Create filing document records given a list of documents
    and a filing record.
    :param documents: list of documents from parse_filing
    :param filing: Filing record
    :param store_raw: whether to store raw contents
    :param store_text: whether to store text contents
    :return:
    """
    # Get client if we're using S3


    # Iterate through documents
    document_records = []
    for document in documents:
        # Create DB object
        filing_doc = FilingDocument()
        filing_doc.filing = filing
        filing_doc.type = document["type"]
        filing_doc.sequence = document["sequence"]
        filing_doc.file_name = document["file_name"]
        filing_doc.content_type = document["content_type"]
        filing_doc.description = document["description"]
        filing_doc.sha1 = document["sha1"]
        filing_doc.start_pos = document["start_pos"]
        filing_doc.end_pos = document["end_pos"]
        filing_doc.is_processed = True
        filing_doc.is_error = len(document["content"]) > 0
        document_records.append(filing_doc)

        # Upload raw if requested
        if store_raw and len(document["content"]) > 0:
            raw_path = pathlib.Path(S3_DOCUMENT_PATH, "raw", document["sha1"]).as_posix()
            if not client.path_exists(raw_path):
                client.put_buffer(raw_path, document["content"])
                logger.info("Uploaded raw file for filing={0}, sequence={1}, sha1={2}"
                            .format(filing, document["sequence"], document["sha1"]))
            else:
                logger.info("Raw file for filing={0}, sequence={1}, sha1={2} already exists on S3"
                            .format(filing, document["sequence"], document["sha1"]))

        # Upload text to S3 if requested
        if store_text and document["content_text"] is not None:
            raw_path = pathlib.Path(S3_DOCUMENT_PATH, "text", document["sha1"]).as_posix()
            if not client.path_exists(raw_path):
                client.put_buffer(raw_path, document["content_text"], write_bytes=False)
                logger.info("Uploaded text contents for filing={0}, sequence={1}, sha1={2}"
                            .format(filing, document["sequence"], document["sha1"]))
            else:
                logger.info("Text contents for filing={0}, sequence={1}, sha1={2} already exists on S3"
                            .format(filing, document["sequence"], document["sha1"]))

    # Create in bulk
    FilingDocument.objects.bulk_create(document_records)
    return len(document_records)


def create_filing_error(row, filing_path: str):
    """
    Create a Filing error record from an index row.
    :param row:
    :param filing_path:
    :return:
    """
    # Get vars
    cik = row["CIK"]
    company_name = row["Company Name"]
    form_type = row["Form Type"]

    try:
        date_filed = dateutil.parser.parse(str(row["Date Filed"])).date()
    except ValueError:
        date_filed = None
    except IndexError:
        date_filed = None

    # Create empty error filing record
    filing = CompanyFiling()
    filing.form_type = form_type
    filing.date_filed = date_filed
    filing.path = filing_path
    filing.is_error = True
    filing.is_processed = False

    # Get company info
    try:
        company = Company.objects.get(cik=cik)

        try:
            _ = CompanyInfo.objects.get(company=company, date=date_filed)
        except CompanyInfo.DoesNotExist:
            # Create company info record
            company_info = CompanyInfo()
            company_info.company = company
            company_info.name = company_name
            company_info.sic = None
            company_info.state_incorporation = None
            company_info.state_location = None
            company_info.date = date_filed
            company_info.save()
    except Company.DoesNotExist:
        # Create company
        company = Company()
        company.cik = cik
        company.cik_name = company_name

        try:
            company.save()
        except django.db.utils.IntegrityError:
            return create_filing_error(row, filing_path)

        # Create company info record
        company_info = CompanyInfo()
        company_info.company = company
        company_info.name = company_name
        company_info.sic = None
        company_info.state_incorporation = None
        company_info.state_location = None
        company_info.date = date_filed
        company_info.save()

    # Finally update company and save
    filing.company = company
    filing.save()
    return True


@shared_task
def process_filing_index(client_type: str, file_path: str, filing_index_buffer: Union[str, bytes] = None,
                         form_type_list: Iterable[str] = None, store_raw: bool = False, store_text: bool = False):
    """
    Process a filing index from an S3 path or buffer.
    :param file_path: S3 or local path to process; if filing_index_buffer is none, retrieved from here
    :param filing_index_buffer: buffer; if not present, s3_path must be set
    :param form_type_list: optional list of form type to process
    :param store_raw:
    :param store_text:
    :return:
    """
    # Log entry
    logger.info("Processing filing index {0}...".format(file_path))

    if client_type == "S3":
        client = S3Client()
    else:
        client = LocalClient()

    # Retrieve buffer if not passed
    if filing_index_buffer is None:
        logger.info("Retrieving filing index buffer for: {}...".format(file_path))
        filing_index_buffer = client.get_buffer(file_path)

    # Write to disk to handle headaches
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(filing_index_buffer)
    temp_file.close()

    # Get main filing data structure
    filing_index_data = openedgar.clients.openedgar.list_index()
    logger.info("Parsed {0} records from index".format(filing_index_data.shape[0]))

    # Iterate through rows
    bad_record_count = 0
    for _, row in filing_index_data.iterrows():
        # Check for form type whitelist
        if form_type_list is not None:
            if row["Form Type"] not in form_type_list:
                logger.info("Skipping filing {0} with form type {1}...".format(row["File Name"], row["Form Type"]))
                continue

        # Cleanup path
        if row["File Name"].lower().startswith("data/"):
            filing_path = "edgar/{0}".format(row["File Name"])
        elif row["File Name"].lower().startswith("edgar/"):
            filing_path = row["File Name"]

        # Check if filing record exists
        try:
            filing = Filing.objects.get(path=filing_path)
            logger.info("Filing record already exists: {0}".format(filing))
        except Filing.MultipleObjectsReturned as e:
            # Create new filing record
            logger.error("Multiple Filing records found for s3_path={0}, skipping...".format(filing_path))
            logger.info("Raw exception: {0}".format(e))
            continue
        except Filing.DoesNotExist as f:
            # Create new filing record
            logger.info("No Filing record found for {0}, creating...".format(filing_path))
            logger.info("Raw exception: {0}".format(f))

            # Check if exists; download and upload to S3 if missing
            if not client.path_exists(filing_path):
                # Download
                try:
                    filing_buffer, _ = openedgar.clients.openedgar.get_buffer("/Archives/{0}".format(filing_path))
                except RuntimeError as g:
                    logger.error("Unable to access resource {0} from EDGAR: {1}".format(filing_path, g))
                    bad_record_count += 1
                    create_filing_error(row, filing_path)
                    continue

                # Upload
                client.put_buffer(filing_path, filing_buffer)

                logger.info("Downloaded from EDGAR and uploaded to {}...".format(client_type))
            else:
                # Download
                logger.info("File already stored on {}, retrieving and processing...".format(client_type))
                filing_buffer = client.get_buffer(filing_path)

            # Parse
            filing_result = process_filing(client, filing_path, filing_buffer, store_raw=store_raw, store_text=store_text)
            if filing_result is None:
                logger.error("Unable to process filing.")
                bad_record_count += 1
                create_filing_error(row, filing_path)

    # Create a filing index record
    # this bit of code below is changed from FilingIndex
    # to CompanyFiling, the columns have not been updated yet
    edgar_url = "/Archives/{0}".format(file_path).replace("//", "/")
    try:
        filing_index = Filing.objects.get(edgar_url=edgar_url)
        filing_index.total_record_count = filing_index_data.shape[0]
        filing_index.bad_record_count = bad_record_count
        filing_index.is_processed = True
        filing_index.is_error = False
        filing_index.save()
        logger.info("Updated existing filing index record.")
    except Filing.DoesNotExist:
        filing_index = Filing()
        filing_index.edgar_url = edgar_url
        filing_index.date_published = None
        filing_index.date_downloaded = datetime.date.today()
        filing_index.total_record_count = filing_index_data.shape[0]
        filing_index.bad_record_count = bad_record_count
        filing_index.is_processed = True
        filing_index.is_error = False
        filing_index.save()
        logger.info("Created new filing index record.")

    # Delete file if we make it this far
    os.remove(temp_file.name)


@shared_task
def process_filing(client, file_path: str, filing_buffer: Union[str, bytes] = None, store_raw: bool = False,
                   store_text: bool = False):
    """
    Process a filing from a path or filing buffer.
    :param file_path: path to process; if filing_buffer is none, retrieved from here
    :param filing_buffer: buffer; if not present, s3_path must be set
    :param store_raw:
    :param store_text:
    :return:
    """
    # Log entry
    logger.info("Processing filing {0}...".format(file_path))


    # Check for existing record first
    try:
        filing = Filing.objects.get(s3_path=file_path)
        if filing is not None:
            logger.error("Filing {0} has already been created in record {1}".format(file_path, filing))
            return None
    except CompanyFiling.DoesNotExist:
        logger.info("No existing record found.")
    except CompanyFiling.MultipleObjectsReturned:
        logger.error("Multiple existing record found.")
        return None

    # Get buffer
    if filing_buffer is None:
        logger.info("Retrieving filing buffer from S3...")
        filing_buffer = client.get_buffer(file_path)

    # Get main filing data structure
    filing_data = openedgar.parsers.openedgar.parse_filing(filing_buffer, extract=store_text)
    if filing_data["cik"] is None:
        logger.error("Unable to parse CIK from filing {0}; assuming broken and halting...".format(file_path))
        return None

    try:
        # Get company
        company = Company.objects.get(cik=filing_data["cik"])
        logger.info("Found existing company record.")

        # Check if record exists for date
        try:
            _ = CompanyInfo.objects.get(company=company, date=filing_data["date_filed"])

            logger.info("Found existing company info record.")
        except CompanyInfo.DoesNotExist:
            # Create company info record
            company_info = CompanyInfo()
            company_info.company = company
            company_info.name = filing_data["company_name"]
            company_info.sic = filing_data["sic"]
            company_info.state_incorporation = filing_data["state_incorporation"]
            company_info.state_location = filing_data["state_location"]
            company_info.date = filing_data["date_filed"].date() if isinstance(filing_data["date_filed"],
                                                                               datetime.datetime) else \
                filing_data["date_filed"]
            company_info.save()

            logger.info("Created new company info record.")

    except Company.DoesNotExist:
        # Create company
        company = Company()
        company.cik = filing_data["cik"]

        try:
            # Catch race with another task/thread
            company.save()

            try:
                _ = CompanyInfo.objects.get(company=company, date=filing_data["date_filed"])
            except CompanyInfo.DoesNotExist:
                # Create company info record
                company_info = CompanyInfo()
                company_info.company = company
                company_info.name = filing_data["company_name"]
                company_info.sic = filing_data["sic"]
                company_info.state_incorporation = filing_data["state_incorporation"]
                company_info.state_location = filing_data["state_location"]
                company_info.date = filing_data["date_filed"]
                company_info.save()
        except django.db.utils.IntegrityError:
            company = Company.objects.get(cik=filing_data["cik"])

        logger.info("Created company and company info records.")

    # Now create the filing record
    try:
        filing = Filing()
        filing.form_type = filing_data["form_type"]
        filing.accession_number = filing_data["accession_number"]
        filing.date_filed = filing_data["date_filed"]
        filing.document_count = filing_data["document_count"]
        filing.company = company
        filing.sha1 = hashlib.sha1(filing_buffer).hexdigest()
        filing.s3_path = file_path
        filing.is_processed = False
        filing.is_error = True
        filing.save()
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unable to create filing record: {0}".format(e))
        return None

    # Create filing document records
    try:
        create_filing_documents(client, filing_data["documents"], filing, store_raw=store_raw, store_text=store_text)
        filing.is_processed = True
        filing.is_error = False
        filing.save()
        return filing
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unable to create filing documents for {0}: {1}".format(filing, e))
        return None


@shared_task
def extract_filing(client, file_path: str, filing_buffer: Union[str, bytes] = None):
    """
    Extract the contents of a filing from an S3 path or filing buffer.
    :param file_path: S3 path to process; if filing_buffer is none, retrieved from here
    :param filing_buffer: buffer; if not present, s3_path must be set
    :return:
    """
    # Get buffer



    if filing_buffer is None:
        logger.info("Retrieving filing buffer from S3...")
        filing_buffer = client.get_buffer(file_path)

    # Get main filing data structure
    _ = openedgar.parsers.openedgar.parse_filing(filing_buffer)


@shared_task
def search_filing_document_sha1(client, sha1: str, term_list: Iterable[str], search_query_id: int, document_id: int,
                                case_sensitive: bool = False,
                                token_search: bool = False, stem_search: bool = False):
    """
    Search a filing document by sha1 hash.
    :param stem_search:
    :param token_search:
    :param sha1: sha1 hash of document to search
    :param term_list: list of terms
    :param search_query_id:
    :param document_id:
    :param case_sensitive:
    :return:
    """
    # Get buffer
    logger.info("Retrieving buffer from S3...")
    text_s3_path = pathlib.Path(S3_DOCUMENT_PATH, "text", sha1).as_posix()
    document_buffer = client.get_buffer(text_s3_path).decode("utf-8")

    # Check if case
    if not case_sensitive:
        document_buffer = document_buffer.lower()

    # TODO: Refactor search types
    # TODO: Cleanup flow for reduced recalc
    # TODO: Don't search same SHA1 repeatedly, but need to coordinate with calling process

    # Get contents
    if not token_search and not stem_search:
        document_contents = document_buffer
    elif token_search:
        document_contents = lexnlp.nlp.en.tokens.get_token_list(document_buffer)
    elif stem_search:
        document_contents = lexnlp.nlp.en.tokens.get_stem_list(document_buffer)

    # For term in term list
    counts = {}
    for term in term_list:
        # term_tokens = lexnlp.nlp.en.tokens.get_token_list(term)

        if stem_search:
            term = lexnlp.nlp.en.tokens.DEFAULT_STEMMER.stem(term)

        if case_sensitive:
            counts[term] = document_contents.count(term)
        else:
            counts[term] = document_contents.count(term.lower())

    search_query = None
    results = []
    for term in counts:
        if counts[term] > 0:
            # Get search query if empty
            if search_query is None:
                search_query = SearchQuery.objects.get(id=search_query_id)

            # Get term
            search_term = SearchQueryTerm.objects.get(search_query_id=search_query_id, term=term)

            # Create result
            result = SearchQueryResult()
            result.search_query = search_query
            result.filing_document_id = document_id
            result.term = search_term
            result.count = counts[term]
            results.append(result)

    # Create if any
    if len(results) > 0:
        SearchQueryResult.objects.bulk_create(results)
    logger.info("Found {0} search terms in document sha1={1}".format(len(results), sha1))
    return True


@shared_task
def extract_filing_document_data_sha1(client, sha1: str):
    """
    Extract structured data from a filing document by sha1 hash, e.g.,
    dates, money, noun phrases.
    :param sha1:
    :param document_id:
    :return:
    """
    # Get buffer
    logger.info("Retrieving buffer from S3...")
    text_s3_path = pathlib.Path(S3_DOCUMENT_PATH, "text", sha1).as_posix()
    document_buffer = client.get_buffer(text_s3_path).decode("utf-8")

    # TODO: Build your own database here.
    _ = len(document_buffer)

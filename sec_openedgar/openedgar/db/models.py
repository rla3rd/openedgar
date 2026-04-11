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

# Package imports
import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, Float, ForeignKey, 
    Date, DateTime, Text, UniqueConstraint, SmallInteger, JSON
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .session import Base

class Company(Base):
    """
    Company, which stores a CIK/security company info.
    """
    __tablename__ = 'openedgar_company'

    # Key fields
    cik = Column(BigInteger, primary_key=True, index=True)
    cik_name = Column(String(1024), index=True)
    
    # Relationships
    company_info = relationship("CompanyInfo", back_populates="company", uselist=False)
    filings = relationship("Filing", back_populates="company")
    facts = relationship("CompanyFact", back_populates="company")
    
    def __repr__(self):
        return f"Company cik={self.cik}, cik_name={self.cik_name}"


class CompanyInfo(Base):
    """
    Company info, which stores a name, SIC, and other data associated with
    a CIK/security on a given date.
    """
    __tablename__ = 'openedgar_companyinfo'

    # Fields
    cik_id = Column(BigInteger, ForeignKey('openedgar_company.cik'), primary_key=True, index=True)
    name = Column(String(1024), index=True)
    is_company = Column(Boolean)
    category = Column(String(1024), nullable=True)
    description = Column(String(1024), nullable=True)
    entity_type = Column(String(1024), nullable=True)
    ein = Column(String(1024), nullable=True)
    industry = Column(String(1024), index=True, nullable=True)
    sic = Column(String(4), index=True, nullable=True)
    sic_description = Column(String(1024), index=True, nullable=True)
    state_of_incorporation = Column(String(32), index=True, nullable=True)
    state_of_incorporation_description = Column(String(1024), nullable=True)
    fiscal_year_end = Column(String(1024), nullable=True)
    mailing_address = Column(JSON, nullable=True)
    business_address = Column(JSON, nullable=True)
    phone = Column(String(20), nullable=True)
    tickers = Column(ARRAY(String(14)), nullable=True)
    exchanges = Column(ARRAY(String(1024)), nullable=True)
    former_names = Column(JSON, nullable=True)
    flags = Column(String(1024), nullable=True)
    insider_transaction_for_owner_exists = Column(SmallInteger)
    insider_transaction_for_issuer_exists = Column(SmallInteger)
    website = Column(String(1024), nullable=True)
    investor_website = Column(String(1024), nullable=True)
    asof = Column(Date, default=datetime.datetime.now, index=True)
    
    # Relationships
    company = relationship("Company", back_populates="company_info")
    
    def __repr__(self):
        return f"CompanyInfo cik={self.cik_id}, name={self.name}, asof={self.asof}"


class FormIndex(Base):
    """
    Form index, which stores a list of form types.
    """
    __tablename__ = 'openedgar_formindex'

    form = Column(String(64), primary_key=True)
    description = Column(String(1024), nullable=True)
    
    # Relationships
    filings = relationship("Filing", back_populates="form_type")
    filing_indices = relationship("FilingIndex", back_populates="form_type")
    company_facts = relationship("CompanyFact", back_populates="formtype")
    
    def __repr__(self):
        return f"FormIndex form={self.form}, description={self.description}"


class BulkFilingIndex(Base):
    """
    Bulk Filing Index, listing of all bulk filing files by date
    """
    __tablename__ = 'openedgar_bulkfilingindex'

    filename = Column(String(1024), primary_key=True)
    year = Column(Integer, index=True, nullable=False)
    quarter = Column(Integer, index=True, nullable=False)
    processed = Column(Boolean, default=False, index=True)
    error = Column(Boolean, default=False, index=True)
    ignored = Column(Boolean, default=False, index=True)
    
    def __repr__(self):
        return f"BulkFilingIndex filename={self.filename}, year={self.year}, quarter={self.quarter}"


class FilingIndex(Base):
    """
    Filing Index, listing of all filings by formtype and cik
    """
    __tablename__ = 'openedgar_filingindex'

    form_id = Column(String(64), ForeignKey('openedgar_formindex.form'), index=True, nullable=True)
    cik = Column(BigInteger, ForeignKey('openedgar_company.cik'), index=True)
    date_filed = Column(Date, index=True)
    filename = Column(String(1024))
    accession_number = Column(String(1024), primary_key=True)
    
    # Relationships
    form_type = relationship("FormIndex", back_populates="filing_indices")
    company = relationship("Company")
    
    def __repr__(self):
        return f"FilingIndex accession_number={self.accession_number}, form={self.form_id}, cik={self.cik}"


class Filing(Base):
    """
    Company Filing, which stores a single filing record from an index.
    """
    __tablename__ = 'openedgar_filing'

    id = Column(Integer, primary_key=True)
    form_id = Column(String(64), ForeignKey('openedgar_formindex.form'), index=True, nullable=True)
    cik_id = Column(BigInteger, ForeignKey('openedgar_company.cik'), index=True)
    date_filed = Column(Date, index=True)
    accession_number = Column(String(1024), unique=True, index=True)
    file_number = Column(String(1024), index=True, nullable=True)
    path = Column(String(1024), nullable=True)
    extracted = Column(Boolean, default=False, index=True)
    processed = Column(Boolean, default=False, index=True)
    error = Column(Boolean, default=False, index=True)
    company_name = Column(String(1024), nullable=True)
    company = Column(String(1024), nullable=True)
    filing_html_index = Column(String(1024), nullable=True)
    homepage_url = Column(String(1024), nullable=True)
    text_url = Column(String(1024), nullable=True)
    
    # Relationships
    form_type = relationship("FormIndex", back_populates="filings")
    company_rel = relationship("Company", back_populates="filings", foreign_keys=[cik_id])
    documents = relationship("FilingDocument", back_populates="filing")
    
    def __repr__(self):
        return f"Filing id={self.id}, accession_number={self.accession_number}, form={self.form_id}, cik={self.cik_id}"


class FactIndex(Base):
    """
    Fact index, which stores a list of fact types.
    """
    __tablename__ = 'openedgar_factindex'

    fact = Column(String(1024), primary_key=True)
    label = Column(String(1024), nullable=True)
    description = Column(String(2048), nullable=True)
    
    # Relationships
    company_facts = relationship("CompanyFact", back_populates="fact_rel")
    
    def __repr__(self):
        return f"FactIndex fact={self.fact}, label={self.label}"


class CompanyFact(Base):
    """
    Company Facts, stored by accession number and fact
    """
    __tablename__ = 'openedgar_companyfact'

    id = Column(String(1024), primary_key=True)
    cik_id = Column(BigInteger, ForeignKey('openedgar_company.cik'), index=True)
    accession_number = Column(String(1024), index=True)
    fact_id = Column(String(1024), ForeignKey('openedgar_factindex.fact'), index=True)
    namespace = Column(String(1024), index=True)
    value = Column(Float, index=True)
    end_date = Column(Date, nullable=True)
    datefiled = Column(Date, index=True)
    fiscal_year = Column(Integer, index=True)
    fiscal_period = Column(String(1024), index=True)
    formtype_id = Column(String(64), ForeignKey('openedgar_formindex.form'))
    frame = Column(String(1024), nullable=True)
    
    # Relationships
    company = relationship("Company", back_populates="facts")
    fact_rel = relationship("FactIndex", back_populates="company_facts")
    formtype = relationship("FormIndex", back_populates="company_facts")
    
    def __repr__(self):
        return f"CompanyFact cik={self.cik_id}, accession_number={self.accession_number}, fact={self.fact_id}"


class FilingDocument(Base):
    """
    Filing document, which corresponds to a <DOCUMENT>...</DOCUMENT> section of a <SEC-DOCUMENT>.
    """
    __tablename__ = 'openedgar_filingdocument'

    id = Column(Integer, primary_key=True)
    filing_id = Column(Integer, ForeignKey('openedgar_filing.id'), index=True)
    type = Column(String(1024), index=True, nullable=True)
    sequence = Column(Integer, index=True, default=0)
    file_name = Column(String(1024), nullable=True)
    content_type = Column(String(1024), nullable=True)
    description = Column(String(1024), nullable=True)
    sha1 = Column(String(1024), index=True)
    start_pos = Column(Integer, index=True)
    end_pos = Column(Integer, index=True)
    is_processed = Column(Boolean, default=False, index=True)
    is_error = Column(Boolean, default=False, index=True)
    
    # Relationships
    filing = relationship("Filing", back_populates="documents")
    search_results = relationship("SearchQueryResult", back_populates="filing_document")
    
    # Unique constraint
    __table_args__ = (
        UniqueConstraint('filing_id', 'sequence', name='unique_filing_sequence'),
    )
    
    def __repr__(self):
        return f"FilingDocument id={self.id}, filing_id={self.filing_id}, sequence={self.sequence}"


class SearchQuery(Base):
    """
    Search query object
    """
    __tablename__ = 'openedgar_searchquery'

    id = Column(Integer, primary_key=True)
    form_type = Column(String(64), index=True, nullable=True)
    date_created = Column(DateTime, default=datetime.datetime.now)
    date_completed = Column(DateTime, nullable=True)
    
    # Relationships
    terms = relationship("SearchQueryTerm", back_populates="search_query")
    results = relationship("SearchQueryResult", back_populates="search_query")
    
    def __repr__(self):
        return f"SearchQuery id={self.id}"


class SearchQueryTerm(Base):
    """
    Search term object
    """
    __tablename__ = 'openedgar_searchqueryterm'

    id = Column(Integer, primary_key=True)
    search_query_id = Column(Integer, ForeignKey('openedgar_searchquery.id'), index=True)
    term = Column(String(128))
    
    # Relationships
    search_query = relationship("SearchQuery", back_populates="terms")
    results = relationship("SearchQueryResult", back_populates="term")
    
    # Unique constraint
    __table_args__ = (
        UniqueConstraint('search_query_id', 'term', name='unique_search_query_term'),
    )
    
    def __repr__(self):
        return f"SearchQueryTerm search_query_id={self.search_query_id}, term={self.term}"


class SearchQueryResult(Base):
    """
    Search result object
    """
    __tablename__ = 'openedgar_searchqueryresult'

    id = Column(Integer, primary_key=True)
    search_query_id = Column(Integer, ForeignKey('openedgar_searchquery.id'), index=True)
    filing_document_id = Column(Integer, ForeignKey('openedgar_filingdocument.id'), index=True)
    term_id = Column(Integer, ForeignKey('openedgar_searchqueryterm.id'), index=True)
    count = Column(Integer, default=0)
    
    # Relationships
    search_query = relationship("SearchQuery", back_populates="results")
    filing_document = relationship("FilingDocument", back_populates="search_results")
    term = relationship("SearchQueryTerm", back_populates="results")
    
    def __repr__(self):
        return f"SearchQueryResult search_query_id={self.search_query_id}, term_id={self.term_id}, count={self.count}"

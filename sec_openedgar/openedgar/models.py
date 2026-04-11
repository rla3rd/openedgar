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
import django.db.models
from django.contrib.postgres.fields import ArrayField
from django.utils import timezone

# Wagtail imports
from django.conf import settings
from wagtail.models import Page
from wagtail.fields import StreamField, RichTextField
from wagtail import blocks
from wagtail.admin.panels import FieldPanel
from wagtail.snippets.models import register_snippet



@register_snippet
class Company(django.db.models.Model):
    """
    Company, which stores a CIK/security company info.
    Part of the core Security Master.
    """

    # Key fields
    cik = django.db.models.BigIntegerField(db_index=True, primary_key=True, unique=True)
    cik_name = django.db.models.CharField(max_length=1024, db_index=True)
    
    # Modern Security Master fields
    ticker = django.db.models.CharField(max_length=12, db_index=True, null=True, blank=True)
    exchange = django.db.models.CharField(max_length=32, db_index=True, null=True, blank=True)
    sic_code = django.db.models.CharField(max_length=4, db_index=True, null=True, blank=True)
    
    # Global Identifiers (Symbology)
    figi = django.db.models.CharField(max_length=12, db_index=True, null=True, blank=True, help_text="OpenFIGI Identifier")
    cusip = django.db.models.CharField(max_length=9, db_index=True, null=True, blank=True)
    isin = django.db.models.CharField(max_length=12, db_index=True, null=True, blank=True)
    
    is_active = django.db.models.BooleanField(default=True, db_index=True)

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "Company cik={0}, cik_name={1}" \
            .format(self.cik, self.cik_name) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")


class CompanyInfo(django.db.models.Model):
    """
    Company info, which stores a name, SIC, and other data associated with
    a CIK/security on a given date.
    """
    # Fields
    cik = django.db.models.OneToOneField(Company, db_column='cik', primary_key=True, db_index=True, on_delete=django.db.models.CASCADE)
    name = django.db.models.CharField(max_length=1024, db_index=True)
    is_company = django.db.models.BooleanField()
    category = django.db.models.CharField(max_length=1024, null=True)
    description = django.db.models.CharField(max_length=1024, null=True)
    entity_type = django.db.models.CharField(max_length=1024, null=True)
    ein = django.db.models.CharField(max_length=1024, null=True)
    industry = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    sic = django.db.models.CharField(max_length=4, db_index=True, null=True)
    sic_description = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    state_of_incorporation = django.db.models.CharField(max_length=32, db_index=True, null=True)
    state_of_incorporation_description = django.db.models.CharField(max_length=1024, null=True)
    fiscal_year_end = django.db.models.CharField(max_length=1024, null=True)
    mailing_address = django.db.models.JSONField(null=True)
    business_address = django.db.models.JSONField(null=True)
    phone = django.db.models.CharField(max_length=20, null=True)
    tickers = ArrayField(django.db.models.CharField(max_length=14), null=True)
    exchanges = ArrayField(django.db.models.CharField(max_length=1024), null=True)
    former_names = django.db.models.JSONField(null=True)
    flags = django.db.models.CharField(max_length=1024, null=True)
    insider_transaction_for_owner_exists = django.db.models.SmallIntegerField()
    insider_transaction_for_issuer_exists = django.db.models.SmallIntegerField()
    website = django.db.models.CharField(max_length=1024, null=True)
    investor_website = django.db.models.CharField(max_length=1024, null=True)
    asof = django.db.models.DateField(default=django.utils.timezone.now, db_index=True)
    
    def __str__(self):
        """
        String representation method
        :return:
        """
        return "CompanyInfo cik={0}, name={1}, asof={2}" \
            .format(self.cik, self.name, self.asof) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")



@register_snippet
class FormIndex(django.db.models.Model):
    """
    Form index, which stores a list of form types.
    """
    form = django.db.models.CharField(max_length=64, primary_key=True, null=False)
    description = django.db.models.CharField(max_length=1024, null=True)

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "FormIndex form_type={0}" \
            .format(self.form_type) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")
            
class BulkFilingIndex(django.db.models.Model):
    """
    Bulk Filing Index, listing of all bulk filing files by date
    """
    filename = django.db.models.CharField(max_length=1024, primary_key=True, null=False)
    year = django.db.models.IntegerField(db_index=True, null=False)
    quarter = django.db.models.IntegerField(db_index=True, null=False)
    processed = django.db.models.DateField(db_index=True, null=True, default=False)
    error = django.db.models.BooleanField(default=False, db_index=True)
    ignored = django.db.models.BooleanField(default=False, db_index=True)
    
    def __str__(self):
        """
        String representation method
        :return:
        """
        return "BulkFilingIndex filename={0} year={1} quarter={2} processed={3} error={4} ignored={5}" \
            .format(self.filename, self.year, self.quarter, self.processed, self.error, self.ignored) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")
    
    
class FilingIndex(django.db.models.Model):
    """
    Filing Index, listing of all filings by formtype and cik
    """
    # Fields
    form_type = django.db.models.ForeignKey(FormIndex, db_column='form', db_index=True, on_delete=django.db.models.CASCADE, null=True)
    company = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    cik = django.db.models.ForeignKey(Company, db_column='cik', db_index=True, on_delete=django.db.models.CASCADE, null=False)
    date_filed = django.db.models.DateField(db_index=True, null=True)
    accession_number = django.db.models.CharField(max_length=1024, primary_key=True, null=False)
    
    def __str__(self):
        """
        String representation method
        :return:
        """
        return "accession_number={0} company={1} cik={2}, form_type={3}, date_filed={4}" \
            .format(self.accession_number, self.company, self.cik, self.form_type, self.date_filed) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")
    


@register_snippet
class Filing(django.db.models.Model):
    """
    Company Filing, which stores a single filing record from an index.
    """
    # Fields
    form_type = django.db.models.ForeignKey(FormIndex, db_column='form', max_length=64, db_index=True, on_delete=django.db.models.CASCADE, null=True)
    accession_number = django.db.models.CharField(max_length=1024, primary_key=True, null=False)
    date_filed = django.db.models.DateField(db_index=True, null=True)
    cik = django.db.models.ForeignKey(Company, db_column='cik', db_index=True, on_delete=django.db.models.CASCADE, null=False)
    company = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    sha1 = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    path = django.db.models.CharField(max_length=1024, db_index=True)
    document_count = django.db.models.IntegerField(default=0)
    processed_document_count = django.db.models.IntegerField(default=0)
    is_processed = django.db.models.BooleanField(default=False, db_index=True)
    is_error = django.db.models.BooleanField(default=False, db_index=True)
    acceptance_datetime = django.db.models.DateField(db_index=True, null=True)
    date_downloaded = django.db.models.DateField(db_index=True, null=True)
    document_url = django.db.models.CharField(max_length=1024, null=True)
    homepage_url = django.db.models.CharField(max_length=1024, null=True)
    text_url = django.db.models.CharField(max_length=1024, null=True)
    
    def __str__(self):
        """
        String representation method
        :return:
        """
        return "Filing id={0}, cik={1}, form_type={2}, date_filed={3}" \
            .format(self.id, self.company.cik if self.company else None, self.form_type, self.date_filed) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")


class FactIndex(django.db.models.Model):
    fact = django.db.models.CharField(max_length=1024, primary_key=True,  null=False)
    label = django.db.models.CharField(max_length=1024, null=True)
    description = django.db.models.CharField(max_length=2048, null=True)

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "FactIndex fact={0}" \
            .format(self.fact) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")


class CompanyFact(django.db.models.Model):
    """
    Company Facts, stored by accession number and fact
    """
    # Fields
    id = django.db.models.CharField(max_length=1024, primary_key=True)
    cik = django.db.models.ForeignKey(Company, db_column='cik', db_index=True, on_delete=django.db.models.CASCADE)
    accession_number = django.db.models.ForeignKey(FilingIndex, db_column='accession_number', db_index=True, on_delete=django.db.models.CASCADE)
    fact = django.db.models.ForeignKey(FactIndex, db_column="fact", db_index=True, on_delete=django.db.models.CASCADE)
    namespace = django.db.models.CharField(max_length=1024, db_index=True)
    value = django.db.models.FloatField(db_index=True)
    end_date = django.db.models.DateField(null=True)
    datefiled = django.db.models.DateField(db_index=True)
    fiscal_year = django.db.models.IntegerField(db_index=True)
    fiscal_period = django.db.models.CharField(max_length=1024, db_index=True)
    formtype = django.db.models.ForeignKey(FormIndex, db_column='form', on_delete=django.db.models.CASCADE, max_length=1024)
    frame = django.db.models.CharField(max_length=1024, null=True)
    
    def __str__(self):
        """
        String representation method
        :return:
        """
        return "CompanyFact cik={0}, accession_number={1}, fact={2}" \
            .format(self.cik, self.accession_number, self.fact) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")


class FilingDocument(django.db.models.Model):
    """
    Filing document, which corresponds to a <DOCUMENT>...</DOCUMENT> section of a <SEC-DOCUMENT>.
    """

    # Key fields
    filing = django.db.models.ForeignKey(Filing, db_index=True, on_delete=django.db.models.CASCADE)
    type = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    sequence = django.db.models.IntegerField(db_index=True, default=0)
    file_name = django.db.models.CharField(max_length=1024, null=True)
    content_type = django.db.models.CharField(max_length=1024, null=True)
    description = django.db.models.CharField(max_length=1024, null=True)
    sha1 = django.db.models.CharField(max_length=1024, db_index=True)
    start_pos = django.db.models.IntegerField(db_index=True)
    end_pos = django.db.models.IntegerField(db_index=True)
    is_processed = django.db.models.BooleanField(default=False, db_index=True)
    is_error = django.db.models.BooleanField(default=False, db_index=True)

    class Meta:
        unique_together = ('filing', 'sequence')

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "FilingDocument id={0}, filing={1}, sequence={2}" \
            .format(self.id, self.filing, self.sequence) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")


class SearchQuery(django.db.models.Model):
    """
    Search query object
    """
    form_type = django.db.models.CharField(max_length=64, db_index=True, null=True)
    date_created = django.db.models.DateTimeField(default=datetime.datetime.now)
    date_completed = django.db.models.DateTimeField(null=True)

    def __str__(self):
        """
        String rep
        :return:
        """
        return "SearchQuery id={0}".format(self.id)


class SearchQueryTerm(django.db.models.Model):
    """
    Search term object
    """
    search_query = django.db.models.ForeignKey(SearchQuery, db_index=True, on_delete=django.db.models.CASCADE)
    term = django.db.models.CharField(max_length=128)

    class Meta:
        unique_together = ('search_query', 'term')

    def __str__(self):
        """
        String rep
        :return:
        """
        return "SearchQueryTerm search_query={0}, term={1}".format(self.search_query, self.term)


class SearchQueryResult(django.db.models.Model):
    """
    Search result object
    """
    search_query = django.db.models.ForeignKey(SearchQuery, db_index=True, on_delete=django.db.models.CASCADE)
    filing_document = django.db.models.ForeignKey(FilingDocument, db_index=True, on_delete=django.db.models.CASCADE)
    term = django.db.models.ForeignKey(SearchQueryTerm, db_index=True, on_delete=django.db.models.CASCADE)
    count = django.db.models.IntegerField(default=0)

    def __str__(self):
        """
        String rep
        :return:
        """
        return "SearchQueryTerm search_query={0}, term={1}".format(self.search_query, self.term)

# --- Wagtail CMS Models ---

class SECIndexPage(Page):
    """Root page for SEC Analysis reports."""
    intro = RichTextField(blank=True)

    content_panels = Page.content_panels + [
        FieldPanel('intro')
    ]

    subpage_types = ['openedgar.FilingAnalysisPage']


class FilingAnalysisPage(Page):
    """A detailed AI-generated analysis report for a specific SEC filing."""
    cik = django.db.models.BigIntegerField()
    company_name = django.db.models.CharField(max_length=1024)
    form_type = django.db.models.CharField(max_length=64)
    date_filed = django.db.models.DateField()
    
    analysis_content = StreamField([
        ('heading', blocks.CharBlock(form_classname="title")),
        ('paragraph', blocks.RichTextBlock()),
        ('ai_opinion', blocks.TextBlock(help_text="The LLM's primary conclusion.")),
        ('supporting_fragment', blocks.StructBlock([
            ('section', blocks.CharBlock()),
            ('content', blocks.TextBlock()),
            ('has_table', blocks.BooleanBlock(required=False)),
        ])),
    ], use_json_field=True)

    content_panels = Page.content_panels + [
        FieldPanel('cik'),
        FieldPanel('company_name'),
        FieldPanel('form_type'),
        FieldPanel('date_filed'),
        FieldPanel('analysis_content'),
    ]

    parent_page_types = ['openedgar.SECIndexPage']


class HomePage(Page):
    """Modern Wagtail-based Home Page."""
    body = RichTextField(blank=True)

    content_panels = Page.content_panels + [
        FieldPanel('body'),
    ]

    max_count = 1


class AboutPage(Page):
    """Modern Wagtail-based About Page."""
    body = RichTextField(blank=True)

    content_panels = Page.content_panels + [
        FieldPanel('body'),
    ]

    max_count = 1


class AnalystDashboardPage(Page):
    """The interactive RAG Analyst Dashboard, integrated into Wagtail."""
    intro = RichTextField(blank=True)

    content_panels = Page.content_panels + [
        FieldPanel('intro'),
    ]

    max_count = 1

    def get_template(self, request, *args, **kwargs):
        # We can use our existing premium dashboard template
        return 'pages/rag_dashboard.html'

# --- Analyst & Security Master Layer ---

@register_snippet
class AnalystProfile(django.db.models.Model):
    """
    Extended user profile for financial analysts.
    Manages coverage and permissions for multi-tenant research.
    """
    user = django.db.models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=django.db.models.CASCADE, related_name='analyst_profile')
    job_title = django.db.models.CharField(max_length=255, blank=True)
    coverage_industries = ArrayField(django.db.models.CharField(max_length=4), help_text="List of SIC codes covered by this analyst.", blank=True, default=list)
    
    panels = [
        FieldPanel('user'),
        FieldPanel('job_title'),
        FieldPanel('coverage_industries'),
    ]

    def __str__(self):
        return f"Analyst: {self.user.username} ({self.job_title})"

@register_snippet
class CoverageAssignment(django.db.models.Model):
    """
    Explicit assignment of a specific stock (CIK) to an analyst.
    """
    analyst = django.db.models.ForeignKey(AnalystProfile, on_delete=django.db.models.CASCADE, related_name='assignments')
    company = django.db.models.ForeignKey(Company, on_delete=django.db.models.CASCADE)
    date_assigned = django.db.models.DateField(auto_now_add=True)
    is_primary = django.db.models.BooleanField(default=True, help_text="Is this the lead analyst for this stock?")

    class Meta:
        unique_together = ('analyst', 'company')

    def __str__(self):
        return f"{self.analyst.user.username} -> {self.company.cik_name}"

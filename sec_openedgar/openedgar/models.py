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


class Company(django.db.models.Model):
    """
    Company, which stores a CIK/security company info.
    """

    # Key fields
    cik = django.db.models.BigIntegerField(db_index=True, primary_key=True, unique=True)
    cik_name = django.db.models.CharField(max_length=1024, db_index=True)

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
    cik = django.db.models.ForeignKey(Company, db_column='cik', primary_key=True, unique=True, db_index=True, on_delete=django.db.models.CASCADE)
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


class CompanyFiling(django.db.models.Model):
    """
    Company Filing, which stores a single filing record from an index.
    """

    # Fields
    form_type = django.db.models.CharField(max_length=64, db_index=True, null=True)
    accession_number = django.db.models.CharField(max_length=1024, primary_key=True, null=False)
    date_filed = django.db.models.DateField(db_index=True, null=True)
    cik = django.db.models.ForeignKey(Company, db_column='cik', db_index=True, on_delete=django.db.models.CASCADE, null=False)
    sha1 = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    path = django.db.models.CharField(max_length=1024, db_index=True)
    document_count = django.db.models.IntegerField(default=0)
    processed_document_count = django.db.models.IntegerField(default=0)
    is_processed = django.db.models.BooleanField(default=False, db_index=True)
    is_error = django.db.models.BooleanField(default=False, db_index=True)
    acceptance_datetime = django.db.models.DateField(db_index=True, null=True)
    date_downloaded = django.db.models.DateField(default=django.utils.timezone.now, db_index=True)
    document_url = django.db.models.CharField(max_length=1024)
    homepage_url = django.db.models.CharField(max_length=1024)
    text_url = django.db.models.CharField(max_length=1024)
    
    def __str__(self):
        """
        String representation method
        :return:
        """
        return "Filing id={0}, cik={1}, form_type={2}, date_filed={3}" \
            .format(self.id, self.company.cik if self.company else None, self.form_type, self.date_filed) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")


class CompanyFacts(django.db.models.Model):
    """
    Company Facts, stored by accession number and fact
    """
    # Fields
    id = django.db.models.AutoField(primary_key=True)
    cik = django.db.models.ForeignKey(Company, db_column='cik', db_index=True, on_delete=django.db.models.CASCADE)
    accession_number = django.db.models.ForeignKey(CompanyFiling, db_column='accession_number', db_index=True, on_delete=django.db.models.CASCADE)
    fact = django.db.models.CharField(max_length=1024, db_index=True,  null=False)
    namespace = django.db.models.CharField(max_length=1024, db_index=True)
    value = django.db.models.FloatField(db_index=True)
    start_date = django.db.models.DateField()
    end_date = django.db.models.DateField()
    datefiled = django.db.models.DateField(db_index=True)
    fiscal_year = django.db.models.IntegerField(max_length=1024, db_index=True)
    fiscal_period = django.db.models.CharField(max_length=1024, db_index=True)
    formtype = django.db.models.CharField(max_length=1024)
    frame = django.db.models.CharField(max_length=1024)
    
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
    filing = django.db.models.ForeignKey(CompanyFiling, db_index=True, on_delete=django.db.models.CASCADE)
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

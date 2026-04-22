import os
import django
import sys

# Set up Django environment
sys.path.append('/home/ralbright/projects/openedgar/sec_openedgar')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.base')
django.setup()

from openedgar.models import Filing, Company
from django.db.models import Count
from django.db.models.functions import ExtractYear

# Count filings by year
try:
    counts = Filing.objects.annotate(year=ExtractYear('date_filed')).values('year').annotate(count=Count('accession_number')).order_by('-year')
    print("Filing counts by year:")
    for entry in counts:
        print(f"  Year {entry['year']}: {entry['count']} filings")
except Exception as e:
    print(f"Error counting filings: {e}")

# Count companies
try:
    comp_count = Company.objects.count()
    print(f"\nTotal Companies: {comp_count}")
except Exception as e:
    print(f"Error counting companies: {e}")

# Also check for recent activity
latest = Filing.objects.order_by('date_filed').last()
if latest:
    print(f"\nLatest Filing Accession: {latest.accession_number}, Date: {latest.date_filed}")
else:
    print("\nNo filings in DB.")

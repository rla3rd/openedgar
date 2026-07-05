import json
from unittest.mock import patch, MagicMock
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from openedgar.models import Company, CompanyInfo, Filing, AnalystProfile, CoverageAssignment, SECIndexPage, FilingAnalysisPage, FormIndex

User = get_user_model()

class AnalystFeatureTests(TestCase):
    def setUp(self):
        # Create standard test user
        self.user = User.objects.create_user(
            username='testanalyst',
            password='testpassword123',
            name='Test Analyst'
        )
        self.client.login(username='testanalyst', password='testpassword123')
        
        # Verify analyst profile is automatically created on-demand
        self.profile = self.user.profile
        
        # Setup FormIndex first
        self.form_index = FormIndex.objects.create(form="10-K")
        
        # Setup mock company in DB
        self.company = Company.objects.create(cik=123456, cik_name="TEST CORP", ticker="TST")
        self.company_info = CompanyInfo.objects.create(
            cik=self.company, 
            name="TEST CORP", 
            is_company=True,
            insider_transaction_for_issuer_exists=False,
            insider_transaction_for_owner_exists=False
        )
        
        # Setup mock filing in DB
        self.filing = Filing.objects.create(
            accession_number="000123456-24-000001",
            company=self.company,
            form_type_id="10-K",
            date_filed="2024-01-01",
            cik=self.company,
            path="data/filing.txt"
        )

    def test_profile_edit_view(self):
        """Tests editing user name, job title, and coverage industries (SICs)"""
        url = reverse('users:update')
        data = {
            'name': 'Updated Analyst Name',
            'job_title': 'Lead Biotech Analyst',
            'coverage_industries': '2834, 2836'
        }
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302) # Redirect to detail page
        
        # Verify database fields updated correctly
        self.user.refresh_from_db()
        self.profile.refresh_from_db()
        
        self.assertEqual(self.user.name, 'Updated Analyst Name')
        self.assertEqual(self.profile.job_title, 'Lead Biotech Analyst')
        self.assertEqual(self.profile.coverage_industries, ['2834', '2836'])

    @patch('edgar.Company')
    def test_coverage_assignment_create_view_with_resolver(self, mock_Company):
        """Tests stock coverage assignment including edgartools resolution lookup fallback"""
        # Scenario 1: Assigning existing local stock
        url = reverse('users:add_coverage')
        data = {'identifier': 'TST'}
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)
        
        # Verify local assignment created
        assignment = CoverageAssignment.objects.filter(analyst=self.profile, company=self.company).first()
        self.assertIsNotNone(assignment)
        self.assertTrue(assignment.is_primary)
        
        # Scenario 2: Assigning a new stock resolved via edgartools
        mock_sec_company = MagicMock()
        mock_sec_company.cik = 987654
        mock_sec_company.name = "EDGAR resolved INC"
        mock_sec_company.tickers = ["EDG"]
        mock_sec_company.sic = 7372
        mock_sec_company.sic_description = "Services-Prepackaged Software"
        mock_Company.return_value = mock_sec_company
        
        data_new = {'identifier': 'EDG'}
        response_new = self.client.post(url, data_new)
        self.assertEqual(response_new.status_code, 302)
        
        # Verify company, companyinfo, and coverage assignment were dynamically generated
        new_company = Company.objects.filter(cik=987654).first()
        self.assertIsNotNone(new_company)
        self.assertEqual(new_company.cik_name, "EDGAR resolved INC")
        
        new_assignment = CoverageAssignment.objects.filter(analyst=self.profile, company=new_company).first()
        self.assertIsNotNone(new_assignment)

    @patch('sec_research.utils.inference.InferenceProvider.call')
    def test_ai_profile_bio_view(self, mock_inference_call):
        """Tests mock AI biography generation endpoint via LM Studio"""
        mock_inference_call.return_value = "This is a mock professional biography written by Qwen 27B."
        
        url = reverse('users:api_bio')
        response = self.client.post(url)
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertEqual(data['bio'], "This is a mock professional biography written by Qwen 27B.")
        mock_inference_call.assert_called_once()

    def test_save_qa_entry_in_session(self):
        """Tests saving custom analyst Q&A session entries for reports"""
        url = reverse('save_qa_entry')
        data = {
            'accession': self.filing.accession_number,
            'question': 'What are the main risk factors?',
            'answer': 'Model analysis identifies competition and supply chain risks.',
            'excerpt': 'Competition in the prepackaged software space...'
        }
        
        response = self.client.post(url, json.dumps(data), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        
        # Verify it was added to session dict
        session_key = f"qa_{self.filing.accession_number}"
        saved_entries = self.client.session.get(session_key)
        self.assertEqual(len(saved_entries), 1)
        self.assertEqual(saved_entries[0]['question'], 'What are the main risk factors?')

    @patch('openedgar.processes.analyst_report.AnalystReporter._compile_pdf')
    def test_generate_analyst_report_publishes_to_wagtail(self, mock_compile_pdf):
        """Tests that saving Q&As and posting compiles the report and creates a Wagtail Page node"""
        mock_compile_pdf.return_value = "Success! Mock PDF compiled."
        
        # 1. Seed session with Q&A
        session_key = f"qa_{self.filing.accession_number}"
        session = self.client.session
        session[session_key] = [{
            'question': 'Test Risk Question',
            'answer': 'Test compliance findings answer.',
            'excerpt': 'Test Raw excerpt quote.'
        }]
        session.save()
        
        # 2. Trigger report generation
        url = reverse('generate_report')
        data = {
            'accession': self.filing.accession_number,
            'notes': 'Test additional analyst disclosures'
        }
        
        response = self.client.post(url, data)
        # Should redirect to the newly published Wagtail page
        self.assertEqual(response.status_code, 302)
        
        # 3. Verify page is created under library hierarchy
        report_slug = f"analysis-tst-{self.filing.accession_number.replace('-', '')}"
        page = FilingAnalysisPage.objects.filter(slug=report_slug).first()
        self.assertIsNotNone(page)
        self.assertEqual(page.cik, 123456)
        self.assertEqual(page.company_name, "TEST CORP")

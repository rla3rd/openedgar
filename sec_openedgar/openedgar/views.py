import os
import json
import logging
import asyncio
from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import View
from django.http import JsonResponse, HttpResponse
from django.contrib import messages
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

from .models import Company, CompanyInfo, Filing, FilingDocument, SECIndexPage, FilingAnalysisPage, FormIndex
from .processes.rag_pipeline import ModernRAGPipeline
from .processes.analyst_report import AnalystReporter
from sec_research.utils.inference import InferenceProvider

logger = logging.getLogger(__name__)

class RAGChatView(View):
    """
    Handles the Premium RAG Dashboard.
    GET: Returns the dashboard template.
    POST: Processes a search/chat query and returns semantic fragments + LLM response.
    """
    def get(self, request, *args, **kwargs):
        # Fetch company filings for search guide
        companies = list(Company.objects.all()[:10])
        return render(request, 'pages/rag_dashboard.html', {'companies': companies})

    def post(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            query = data.get('query', '')
            cik = data.get('cik', None)
            
            if not query:
                return JsonResponse({'error': 'No query provided'}, status=400)
            
            pipeline = ModernRAGPipeline()
            
            # Step 1: Perform RAG search
            results_df = pipeline.query(query, k=5, cik=cik)
            
            if results_df is None or results_df.empty:
                # Fallback to direct LLM call if no filings found
                prompt = f"Answer the following financial query: {query}"
                provider = InferenceProvider(
                    provider_type="openai",
                    model=os.getenv("LOCAL_LLM_MODEL", "qwen/qwe3.6-27b"),
                    api_url=os.getenv("LOCAL_LLM_URL", "http://localhost:1234/v1/chat/completions"),
                    api_key="no-key"
                )
                llm_response = provider.call(prompt, "You are a professional financial research assistant.")
                return JsonResponse({
                    'response': llm_response + "\n\n*(Note: No direct filings were found matching this query in the database. This response is generated purely from general model knowledge.)*",
                    'fragments': []
                })
            
            # Step 2: Prepare fragments for frontend
            fragments = results_df.to_dict(orient='records')
            
            # Step 3: Query Local LLM (Qwen 27B via LM Studio)
            context = "\n\n".join([f"[{f['form_type']} {f['date_filed']}]: {f['content']}" for f in fragments])
            prompt = f"Use the following SEC filing fragments to answer the user query.\n\nContext:\n{context}\n\nQuery: {query}"
            
            provider = InferenceProvider(
                provider_type="openai",
                model=os.getenv("LOCAL_LLM_MODEL", "qwen/qwe3.6-27b"),
                api_url=os.getenv("LOCAL_LLM_URL", "http://localhost:1234/v1/chat/completions"),
                api_key="no-key"
            )
            llm_response = provider.call(prompt, "You are a professional financial research assistant.")
            
            return JsonResponse({
                'response': llm_response,
                'fragments': fragments
            })
            
        except Exception as e:
            logger.exception("Error in RAGChatView")
            return JsonResponse({'error': str(e)}, status=500)


class FilingQAView(View):
    """
    Handles interactive raw filing Q&A.
    Allows resolving CIK/Ticker, dynamic filing download via edgartools, 
    on-the-fly RAG indexing, and single-filing chat.
    """
    def get(self, request, *args, **kwargs):
        accession = request.GET.get('accession', '').strip()
        ticker = request.GET.get('ticker', '').strip().upper()
        form_type = request.GET.get('form', '10-K').strip()
        
        filing = None
        
        # 1. Resolve filing dynamically if ticker/form is provided
        if ticker:
            try:
                from edgar import Company as SECCompany
                sec_company = SECCompany(ticker)
                if sec_company:
                    # Sync Company locally
                    company, _ = Company.objects.get_or_create(
                        cik=sec_company.cik,
                        defaults={'cik_name': sec_company.name, 'ticker': sec_company.tickers[0] if (hasattr(sec_company, 'tickers') and sec_company.tickers) else ticker}
                    )
                    # Sync CompanyInfo
                    CompanyInfo.objects.get_or_create(
                        cik=company,
                        defaults={
                            'name': sec_company.name,
                            'is_company': True,
                            'sic': str(sec_company.sic) if sec_company.sic else None,
                            'sic_description': getattr(sec_company, 'sic_description', "") or "",
                            'insider_transaction_for_issuer_exists': False,
                            'insider_transaction_for_owner_exists': False,
                        }
                    )
                    
                    # Fetch target filings
                    sec_filings = sec_company.get_filings(form=form_type)
                    if sec_filings:
                        latest_sec_filing = sec_filings[0]
                        accession = latest_sec_filing.accession_no
                        
                        # Ensure FormIndex exists
                        FormIndex.objects.get_or_create(form=form_type)

                        # Sync Filing locally
                        filing, created = Filing.objects.get_or_create(
                            accession_number=accession,
                            defaults={
                                'company': company.cik_name,
                                'form_type_id': form_type,
                                'date_filed': latest_sec_filing.filing_date,
                                'cik': company,
                                'path': latest_sec_filing.document or "",
                            }
                        )
                        
                        # Index in hyperstreamdb if new
                        if created or not filing.is_processed:
                            logger.info(f"Ingesting filing {accession} dynamically into hyperstreamdb...")
                            raw_text = latest_sec_filing.markdown()
                            pipeline = ModernRAGPipeline()
                            pipeline.ingest_filing_chunks(
                                company.cik, 
                                accession, 
                                form_type, 
                                str(latest_sec_filing.filing_date), 
                                raw_text
                            )
                            filing.is_processed = True
                            filing.is_error = False
                            filing.save()
            except Exception as e:
                logger.exception("Failed to dynamically fetch filing")
                messages.error(request, f"Error resolving filing via edgartools: {str(e)}")
                return redirect(reverse('users:detail', kwargs={'username': request.user.username}))
                
        # 2. Get filing from DB
        if accession:
            filing = Filing.objects.filter(accession_number=accession).select_related('cik').first()
            
        if not filing:
            messages.error(request, "No filing selected or found. Please fetch a filing by ticker/CIK first.")
            return redirect(reverse('users:detail', kwargs={'username': request.user.username}))
            
        # 3. Retrieve raw text
        raw_text = ""
        try:
            # Fallback directly to edgartools if local file is missing
            from edgar import find
            f = find(filing.accession_number)
            raw_text = f.markdown()
        except Exception as e:
            logger.warning(f"Could not load filing text via edgartools: {e}. Checking local DB/files...")
            raw_text = f"Filing {filing.accession_number} loaded. Content is indexed in vector database."
            
        # 4. Load saved Q&A entries for this filing from session
        session_key = f"qa_{filing.accession_number}"
        saved_entries = request.session.get(session_key, [])
        
        context = {
            'filing': filing,
            'company': filing.company,
            'raw_text': raw_text,
            'saved_entries': saved_entries,
        }
        return render(request, 'pages/filing_qa.html', context)

    def post(self, request, *args, **kwargs):
        """
        Submits question against the filing content using ModernRAGPipeline inside a specific accession.
        """
        try:
            data = json.loads(request.body)
            accession = data.get('accession', '')
            question = data.get('question', '')
            
            if not accession or not question:
                return JsonResponse({'error': 'Missing filing accession or question'}, status=400)
                
            filing = Filing.objects.filter(accession_number=accession).select_related('cik').first()
            if not filing:
                return JsonResponse({'error': 'Filing record not found'}, status=404)
                
            # Perform RAG vector search scoped to this specific filing
            pipeline = ModernRAGPipeline()
            results_df = pipeline.query(
                question, 
                accession_number=accession, 
                k=4
            )
            
            if results_df is None or results_df.empty:
                # Slicing raw text fallback
                try:
                    from edgar import get_filing
                    f = get_filing(accession)
                    raw_text = f.markdown()[:40000]
                    context = f"Raw filing excerpt:\n{raw_text}"
                except:
                    context = "Filing text context unavailable."
                fragments = []
            else:
                fragments = results_df.to_dict(orient='records')
                context = "\n\n".join([f"[{f['form_type']} {f['date_filed']}]: {f['content']}" for f in fragments])
                
            prompt = (
                f"You are a senior financial analyst. Answer the user's question regarding "
                f"{filing.cik.cik_name}'s {filing.form_type} filing.\n"
                f"Use the following filing excerpts as context. Quote directly from the text where appropriate.\n\n"
                f"Context:\n{context}\n\n"
                f"Question: {question}"
            )
            
            # Resolve LLM settings from the requesting user's profile
            try:
                profile = request.user.profile
                llm_provider = profile.llm_provider
                llm_api_key = profile.get_llm_api_key()
                llm_api_url = profile.get_llm_api_url()
                llm_model = profile.get_llm_default_model()
            except Exception:
                llm_provider = 'openai'
                llm_api_key = os.getenv('LOCAL_LLM_MODEL', '')
                llm_api_url = os.getenv('LOCAL_LLM_URL', 'https://api.openai.com/v1/chat/completions')
                llm_model = os.getenv('LOCAL_LLM_MODEL', 'gpt-4o-mini')

            if not llm_api_key:
                return JsonResponse({
                    'error': (
                        f'No API key configured for {llm_provider}. '
                        'Please go to Edit Profile and add your API key.'
                    )
                }, status=400)

            provider = InferenceProvider(
                provider_type=llm_provider,
                model=llm_model,
                api_url=llm_api_url,
                api_key=llm_api_key,
            )
            response = provider.call(prompt, "You are a professional financial research assistant.")
            
            return JsonResponse({
                'response': response,
                'fragments': fragments
            })
        except Exception as e:
            logger.exception("Error in filing Q&A post")
            return JsonResponse({'error': str(e)}, status=500)


class SaveQAEntryView(View):
    """
    Saves a specific Q&A pair and optional text selection into the session for report construction.
    """
    def post(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            accession = data.get('accession', '')
            question = data.get('question', '')
            answer = data.get('answer', '')
            excerpt = data.get('excerpt', '')
            
            if not accession or not question or not answer:
                return JsonResponse({'error': 'Missing parameters'}, status=400)
                
            session_key = f"qa_{accession}"
            entries = request.session.get(session_key, [])
            
            # Avoid duplicates
            if not any(e['question'] == question for e in entries):
                entries.append({
                    'question': question,
                    'answer': answer,
                    'excerpt': excerpt
                })
                request.session[session_key] = entries
                request.session.modified = True
                
            return JsonResponse({'status': 'success', 'entries_count': len(entries)})
        except Exception as e:
            logger.exception("Failed to save Q&A entry")
            return JsonResponse({'error': str(e)}, status=500)


class GenerateAnalystReportView(View):
    """
    Compiles the saved Q&A findings into a LaTeX report on disk, compiles to PDF, 
    and automatically registers the report as a FilingAnalysisPage in Wagtail.
    """
    def post(self, request, *args, **kwargs):
        accession = request.POST.get('accession', '').strip()
        additional_notes = request.POST.get('notes', '').strip()
        
        if not accession:
            messages.error(request, "Filing accession number is required.")
            return redirect(reverse('users:detail', kwargs={'username': request.user.username}))
            
        filing = get_object_or_404(Filing, accession_number=accession)
        session_key = f"qa_{accession}"
        qa_entries = request.session.get(session_key, [])
        
        if not qa_entries:
            messages.error(request, "No Q&A entries have been saved for this report. Ask questions and save them first.")
            return redirect(f"/filing/qa/?accession={accession}")
            
        try:
            # 1. Compile LaTeX Document
            # Escaping helpers
            def tex_clean(val):
                return str(val).replace('&', '\\&').replace('%', '\\%').replace('$', '\\$').replace('_', '\\_').replace('#', '\\#')
                
            company_name = filing.cik.cik_name
            topic = f"{filing.form_type} Financial Research & Compliance Study"
            
            # Format findings block
            findings_latex = ""
            for i, entry in enumerate(qa_entries):
                findings_latex += f"\\subsection*{{Query {i+1}: {tex_clean(entry['question'])} Elective Insight}}\n"
                if entry['excerpt']:
                    findings_latex += f"\\begin{{quote}}\n\\textit{{Filing Quote: {tex_clean(entry['excerpt'])} }}\n\\end{{quote}}\n\n"
                findings_latex += f"{tex_clean(entry['answer'])}\n\n"
                
            if additional_notes:
                findings_latex += f"\\section*{{Analyst Notes}}\n{tex_clean(additional_notes)}\n"
                
            # Create LaTeX file
            reporter = AnalystReporter(output_dir="reports")
            
            # Inject standard latex template parameters
            from string import Template
            from .processes.analyst_report import LATEX_TEMPLATE
            
            tex_content = Template(LATEX_TEMPLATE).substitute(
                company_name=company_name,
                topic=topic,
                summary=f"Automated RAG-enabled review of {company_name}'s {filing.form_type} filing.",
                fragments=findings_latex
            )
            
            safe_name = filing.cik.ticker or str(filing.cik.cik)
            tex_path = os.path.join(reporter.output_dir, f"report_{safe_name}_{filing.accession_number}.tex")
            with open(tex_path, "w") as f:
                f.write(tex_content)
                
            # Compile PDF
            pdf_result = reporter._compile_pdf(tex_path)
            messages.info(request, pdf_result)
            
            # 2. Sync to Wagtail FilingAnalysisPage (Seeking Alpha CMS Node)
            # Find parent index page or create it
            index_page = SECIndexPage.objects.first()
            if not index_page:
                from wagtail.models import Page
                # Find the default home page of Wagtail to add under
                root = Page.get_first_root_node()
                home_page = root.get_children().first() or root
                
                index_page = SECIndexPage(
                    title="SEC Filing Analysis Library",
                    intro="Comprehensive financial research articles created by our team of analysts using SEC index systems.",
                    slug="sec-library"
                )
                home_page.add_child(instance=index_page)
                index_page.save_revision().publish()
                
            # Check if this report already exists under index
            report_slug = f"analysis-{safe_name.lower()}-{filing.accession_number.replace('-', '')}"
            analysis_page = FilingAnalysisPage.objects.filter(slug=report_slug).first()
            
            # StreamField stream structure
            analysis_stream = [
                ('heading', "Executive Takeaways"),
                ('ai_opinion', f"Automated compliance review of {company_name} filings. Highlights include focus on risk disclosures and financial indicators."),
            ]
            
            for entry in qa_entries:
                analysis_stream.append(('heading', f"Analysis: {entry['question']}"))
                analysis_stream.append(('paragraph', f"<p>{entry['answer']}</p>"))
                if entry['excerpt']:
                    analysis_stream.append(('supporting_fragment', {
                        'section': "Filing Excerpt",
                        'content': entry['excerpt'],
                        'has_table': False
                    }))
                    
            if additional_notes:
                analysis_stream.append(('heading', "Analyst Recommendations"))
                analysis_stream.append(('paragraph', f"<p>{additional_notes}</p>"))
                
            if not analysis_page:
                analysis_page = FilingAnalysisPage(
                    title=f"{company_name} ({safe_name}) {filing.form_type} Analysis",
                    slug=report_slug,
                    cik=filing.cik.cik,
                    company_name=company_name,
                    form_type=filing.form_type,
                    date_filed=filing.date_filed,
                    analysis_content=analysis_stream
                )
                index_page.add_child(instance=analysis_page)
            else:
                analysis_page.analysis_content = analysis_stream
                
            analysis_page.save_revision().publish()
            messages.success(request, f"Successfully published seeking alpha style HTML report page to public website at {analysis_page.url}")
            
            # Clear Q&A session entries after generating report
            request.session[session_key] = []
            request.session.modified = True
            
            return redirect(analysis_page.url)
            
        except Exception as e:
            logger.exception("Report Generation failed")
            messages.error(request, f"Error compiling report: {str(e)}")
            return redirect(f"/filing/qa/?accession={accession}")


class FilingHTMLView(View):
    """
    Renders raw SEC HTML filing data from edgartools.
    """
    def get(self, request, *args, **kwargs):
        accession = request.GET.get('accession', '').strip()
        if not accession:
            return HttpResponse("Accession number required", status=400)
            
        try:
            from edgar import find
            f = find(accession)
            html_content = f.html()
            return HttpResponse(html_content, content_type='text/html')
        except Exception as e:
            return HttpResponse(f"Error loading HTML filing content: {str(e)}", status=500)


class FilingComparisonView(View):
    """
    Chronological section comparison for 10-K and 10-Q filings.
    Maps different item numbers between 10-K and 10-Q (e.g. MD&A: Item 7 vs Part I Item 2).
    """
    def get(self, request, *args, **kwargs):
        cik = request.GET.get('cik', '').strip()
        section_type = request.GET.get('section', 'risk_factors').strip()
        
        if not cik:
            messages.error(request, "A CIK or Ticker is required for comparison.")
            return redirect(reverse('users:redirect'))
            
        company = get_object_or_404(Company, cik=cik)
        
        # Get all 10-K and 10-Q filings for this company, ordered chronologically
        filings = Filing.objects.filter(
            company=company,
            form_type__form__in=['10-K', '10-Q']
        ).order_by('date_filed')
        
        comparison_data = []
        
        # Map section selection to edgartools keys
        # Format: (10-K key, 10-Q key)
        section_maps = {
            'risk_factors': ('Item 1A', 'Item 1A'),
            'mda': ('Item 7', 'Item 2'),
            'legal': ('Item 3', 'Item 1'),
            'business': ('Item 1', None),
        }
        
        section_labels = {
            'risk_factors': 'Item 1A: Risk Factors',
            'mda': "MD&A (Management's Discussion & Analysis)",
            'legal': 'Item 3 / Part II Item 1: Legal Proceedings',
            'business': 'Item 1: Business Overview',
        }
        
        k_key, q_key = section_maps.get(section_type, ('Item 1A', 'Item 1A'))
        
        for filing in filings:
            key = k_key if filing.form_type_id == '10-K' else q_key
            text = "Not available for this form type."
            
            if key:
                try:
                    from edgar import find, obj
                    sec_filing = find(filing.accession_number)
                    report = obj(sec_filing)
                    text = report[key] or "Section content not found in filing."
                except Exception as e:
                    logger.warning(f"Error loading section {key} for {filing.accession_number}: {e}")
                    text = f"Error retrieving section: {str(e)}"
            
            # Estimate quarter based on filing date / month
            period = f"FY {filing.date_filed.year}" if filing.form_type_id == '10-K' else f"Q{ (filing.date_filed.month - 1) // 3 + 1 } {filing.date_filed.year}"
            
            comparison_data.append({
                'filing': filing,
                'period': period,
                'key_used': key,
                'text': text
            })
            
        context = {
            'company': company,
            'section_type': section_type,
            'section_label': section_labels.get(section_type),
            'comparison_data': comparison_data,
            'section_labels': section_labels
        }
        return render(request, 'pages/filing_comparison.html', context)



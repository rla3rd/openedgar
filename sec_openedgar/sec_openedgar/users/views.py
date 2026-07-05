import os
import sys
import json
import logging
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse
from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import DetailView, ListView, RedirectView, UpdateView, View
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator

from .models import User
from openedgar.models import AnalystProfile, CoverageAssignment, Company, CompanyInfo
from sec_research.utils.inference import InferenceProvider

logger = logging.getLogger(__name__)

class UserDetailView(LoginRequiredMixin, DetailView):
    model = User
    slug_field = 'username'
    slug_url_kwarg = 'username'
    template_name = 'users/user_detail.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        profile = self.object.profile
        context['profile'] = profile
        context['assignments'] = profile.assignments.select_related('company').all()
        # Fetch filing list for dynamic raw QA dropdowns from user's watchlist
        from openedgar.models import Filing
        watchlisted_ciks = profile.assignments.values_list('company_id', flat=True)
        context['filings'] = Filing.objects.filter(cik_id__in=watchlisted_ciks).select_related('cik').order_by('-date_filed')[:20]
        return context


class UserRedirectView(LoginRequiredMixin, RedirectView):
    permanent = False

    def get_redirect_url(self):
        return reverse('users:detail', kwargs={'username': self.request.user.username})


class ProfileEditView(LoginRequiredMixin, View):
    """
    Combines User and AnalystProfile update operations into a single premium interface.
    """
    def get(self, request, *args, **kwargs):
        user = request.user
        profile = user.profile
        
        # Display industries as comma-separated list of SICs
        industries_str = ", ".join(profile.coverage_industries) if profile.coverage_industries else ""
        
        context = {
            'user': user,
            'profile': profile,
            'industries_str': industries_str,
        }
        return render(request, 'users/user_form.html', context)

    def post(self, request, *args, **kwargs):
        user = request.user
        profile = user.profile
        
        name = request.POST.get('name', '').strip()
        job_title = request.POST.get('job_title', '').strip()
        industries_raw = request.POST.get('coverage_industries', '').strip()
        llm_provider = request.POST.get('llm_provider', '').strip()
        openai_api_key = request.POST.get('openai_api_key', '').strip()
        anthropic_api_key = request.POST.get('anthropic_api_key', '').strip()
        gemini_api_key = request.POST.get('gemini_api_key', '').strip()

        # Parse industries comma list
        industries = [sic.strip() for sic in industries_raw.split(',') if sic.strip()]
        
        user.name = name
        user.save()
        
        profile.job_title = job_title
        profile.coverage_industries = industries
        if llm_provider in dict(profile.LLM_PROVIDER_CHOICES):
            profile.llm_provider = llm_provider
        # Only overwrite a key if the user actually submitted something
        # (blank = keep the existing key, so they don't have to re-paste it)
        if openai_api_key:
            profile.openai_api_key = openai_api_key
        if anthropic_api_key:
            profile.anthropic_api_key = anthropic_api_key
        if gemini_api_key:
            profile.gemini_api_key = gemini_api_key
        profile.save()
        
        messages.success(request, "Your profile was updated successfully.")
        return redirect(reverse('users:detail', kwargs={'username': user.username}))


class CoverageAssignmentCreateView(LoginRequiredMixin, View):
    """
    Assigns stock coverage to the analyst. Resolves CIK/Ticker via edgartools if missing locally.
    """
    def post(self, request, *args, **kwargs):
        identifier = request.POST.get('identifier', '').strip().upper()
        if not identifier:
            messages.error(request, "Please enter a valid stock ticker or CIK.")
            return redirect(reverse('users:detail', kwargs={'username': request.user.username}))
            
        try:
            # 1. Resolve company (local first, fallback to edgartools)
            company = None
            if identifier.isdigit():
                company = Company.objects.filter(cik=int(identifier)).first()
            else:
                company = Company.objects.filter(ticker=identifier).first()
                
            if not company:
                # Ensure EDGAR_LOCAL_DATA_DIR is set in os.environ before importing
                # edgar — it initialises its HTTP cache at import time using Path.home(),
                # which on macOS can resolve to the read-only /System/Volumes/Data/home
                # symlink instead of /Users/ralbright, causing OSError [Errno 45].
                # os.path.expanduser('~') is reliable on both Linux and macOS.
                import os as _os
                _home = _os.path.expanduser('~')
                _edgar_data_dir = _os.environ.get(
                    'EDGAR_LOCAL_DATA_DIR',
                    _os.path.join(_home, 'data', 'edgar')
                )
                # Normalise: strip quotes added by some .env parsers
                _edgar_data_dir = _edgar_data_dir.strip('"\'')
                # Expand ~ and make absolute (handles relative paths too)
                _edgar_data_dir = _os.path.abspath(_os.path.expanduser(_edgar_data_dir))
                _os.environ['EDGAR_LOCAL_DATA_DIR'] = _edgar_data_dir
                # Pre-create the directory so edgar doesn't need to touch home
                _os.makedirs(_edgar_data_dir, exist_ok=True)
                from edgar import Company as SECCompany
                logger.info(f"Resolving ticker/CIK {identifier} via edgartools...")
                try:
                    sec_company = SECCompany(identifier)
                except Exception as e:
                    logger.warning(f"Could not resolve {identifier} via edgartools: {e}")
                    sec_company = None
                if sec_company:
                    # Get or create company locally
                    company, created = Company.objects.get_or_create(
                        cik=sec_company.cik,
                        defaults={
                            'cik_name': sec_company.name,
                            'ticker': sec_company.tickers[0] if (hasattr(sec_company, 'tickers') and sec_company.tickers) else identifier,
                        }
                    )
                    
                    # Create corresponding CompanyInfo
                    CompanyInfo.objects.get_or_create(
                        cik=company,
                        defaults={
                            'name': sec_company.name,
                            'is_company': True,
                            'sic': str(sec_company.sic) if sec_company.sic else None,
                            'sic_description': sec_company.sic_description or "",
                            'insider_transaction_for_issuer_exists': False,
                            'insider_transaction_for_owner_exists': False,
                        }
                    )
                    
            if not company:
                messages.error(request, f"Unable to find or resolve company details for '{identifier}'.")
                return redirect(reverse('users:detail', kwargs={'username': request.user.username}))
                
            # 2. Add coverage assignment
            profile = request.user.profile
            assignment, created = CoverageAssignment.objects.get_or_create(
                analyst=profile,
                company=company,
                defaults={'is_primary': True}
            )
            
            if created:
                messages.success(request, f"Assigned coverage for {company.cik_name} ({company.ticker or company.cik}) successfully.")
            else:
                messages.info(request, f"You are already covering {company.cik_name}.")
                
        except Exception as e:
            logger.exception("Error in stock coverage assignment")
            messages.error(request, f"Failed to resolve stock coverage: {str(e)}")
            
        return redirect(reverse('users:detail', kwargs={'username': request.user.username}))


class CoverageAssignmentDeleteView(LoginRequiredMixin, View):
    """
    Removes a coverage assignment.
    """
    def post(self, request, pk, *args, **kwargs):
        assignment = get_object_or_404(CoverageAssignment, pk=pk, analyst=request.user.profile)
        company_name = assignment.company.cik_name
        assignment.delete()
        messages.success(request, f"Removed coverage assignment for {company_name}.")
        return redirect(reverse('users:detail', kwargs={'username': request.user.username}))


class AIProfileBioView(LoginRequiredMixin, View):
    """
    Generates a professional financial analyst biography using LM Studio and Qwen model.
    """
    def post(self, request, *args, **kwargs):
        try:
            profile = request.user.profile
            assignments = profile.assignments.select_related('company').all()
            
            job_title = profile.job_title or "Financial Analyst"
            industries = ", ".join(profile.coverage_industries) if profile.coverage_industries else "General Sectors"
            companies = ", ".join([f"{a.company.cik_name} ({a.company.ticker or a.company.cik})" for a in assignments])
            
            prompt = (
                f"Write a highly professional and concise financial analyst biography (under 150 words) "
                f"for {request.user.name or request.user.username}.\n"
                f"Job Title: {job_title}\n"
                f"Coverage Sectors/SICs: {industries}\n"
                f"Stock Coverage: {companies if companies else 'None assigned yet'}.\n"
                f"Write in third-person, highlighting expertise, sector specialization, and coverage portfolio. Do not include placeholders."
            )
            
            # Setup Inference Provider targeting LM Studio
            provider = InferenceProvider(
                provider_type="openai",
                model=os.getenv("LOCAL_LLM_MODEL", "qwen/qwe3.6-27b"),
                api_url=os.getenv("LOCAL_LLM_URL", "http://localhost:1234/v1/chat/completions"),
                api_key="no-key"
            )
            
            bio = provider.call(
                prompt=prompt,
                system_prompt="You are a professional business writer helping draft high-quality financial analyst profiles.",
                temperature=0.7,
                max_tokens=256
            )
            
            return JsonResponse({'bio': bio.strip()})
        except Exception as e:
            logger.exception("AI Biography generation failed")
            return JsonResponse({'error': f"LLM Bio Generation error: {str(e)}"}, status=500)


class UserRedirectView(LoginRequiredMixin, RedirectView):
    permanent = False

    def get_redirect_url(self):
        return reverse('users:detail', kwargs={'username': self.request.user.username})


class UserListView(LoginRequiredMixin, ListView):
    model = User
    slug_field = 'username'
    slug_url_kwarg = 'username'


import subprocess
import uuid
from django.conf import settings

running_commands = {}

class RunCommandView(LoginRequiredMixin, View):
    """
    Spawns a background subprocess to run django manage.py commands safely.
    """
    def post(self, request, *args, **kwargs):
        command = request.POST.get('command', '').strip()
        if not command:
            return JsonResponse({'error': 'No command provided'}, status=400)
            
        tokens = command.split()
        if not tokens:
            return JsonResponse({'error': 'Empty command'}, status=400)
            
        cmd_name = tokens[0]
        cmd_args = tokens[1:]
        
        execution_id = str(uuid.uuid4())
        log_dir = os.path.join(settings.BASE_DIR, 'command_logs')
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"{execution_id}.log")
        
        python_executable = sys.executable or "/Users/ralbright/projects/openedgar/.venv/bin/python"
        full_command = [python_executable, "manage.py", cmd_name] + cmd_args
        
        try:
            log_file = open(log_path, 'w')
            process = subprocess.Popen(
                full_command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=settings.BASE_DIR
            )
            
            running_commands[execution_id] = {
                'process': process,
                'log_path': log_path,
                'command': command,
                'log_file': log_file
            }
            
            return JsonResponse({
                'status': 'started',
                'execution_id': execution_id,
                'command': command
            })
        except Exception as e:
            logger.exception("Failed to start command subprocess")
            return JsonResponse({'error': f"Failed to run command: {str(e)}"}, status=500)


class CommandLogsView(LoginRequiredMixin, View):
    """
    Reads the logs and checks execution status for a spawned manage.py command process.
    """
    def get(self, request, *args, **kwargs):
        execution_id = request.GET.get('execution_id', '').strip()
        if not execution_id or execution_id not in running_commands:
            return JsonResponse({'error': 'Invalid or missing execution ID'}, status=400)
            
        task = running_commands[execution_id]
        process = task['process']
        log_path = task['log_path']
        
        exit_code = process.poll()
        is_running = (exit_code is None)
        
        logs = ""
        if os.path.exists(log_path):
            try:
                with open(log_path, 'r') as f:
                    logs = f.read()
            except Exception as e:
                logs = f"Error reading logs: {str(e)}"
                
        return JsonResponse({
            'is_running': is_running,
            'exit_code': exit_code,
            'logs': logs
        })


"""
MIT License

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

from django.conf import settings
from django.urls import re_path
from django.conf.urls import include
from django.conf.urls.static import static
from django.contrib import admin
from django.views.generic import TemplateView
from django.views import defaults as default_views
from openedgar.views import RAGChatView, FilingQAView, SaveQAEntryView, GenerateAnalystReportView, FilingHTMLView, FilingComparisonView

from wagtail.admin import urls as wagtailadmin_urls
from wagtail import urls as wagtail_urls
from wagtail.documents import urls as wagtaildocs_urls
from wagtail.views import serve as wagtail_serve

urlpatterns = [
                  # Home / Root route resolved via Wagtail
                  re_path(r'^$', wagtail_serve, name='home'),
                  re_path(r'^about/$', TemplateView.as_view(template_name='pages/about.html'), name='about'),

                  # API endpoints
                  re_path(r'^api/chat/$', RAGChatView.as_view(), name='rag_chat'),
                  re_path(r'^filing/qa/$', FilingQAView.as_view(), name='filing_qa'),
                  re_path(r'^filing/qa/save/$', SaveQAEntryView.as_view(), name='save_qa_entry'),
                  re_path(r'^filing/report/generate/$', GenerateAnalystReportView.as_view(), name='generate_report'),
                  re_path(r'^filing/html/$', FilingHTMLView.as_view(), name='filing_html'),
                  re_path(r'^filing/comparison/$', FilingComparisonView.as_view(), name='filing_comparison'),

                  # Django Admin, use {% url 'admin:index' %}
                  re_path(settings.ADMIN_URL, admin.site.urls),

                  # User management
                  re_path(r'^users/', include('sec_openedgar.users.urls', namespace='users')),
                  re_path(r'^accounts/', include('allauth.urls')),

                  # Wagtail
                  re_path(r'^cms/', include(wagtailadmin_urls)),
                  re_path(r'^documents/', include(wagtaildocs_urls)),
                  re_path(r'', include(wagtail_urls)),

                  # Your stuff: custom urls includes go here

              ] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

if settings.DEBUG:
    # This allows the error pages to be debugged during development, just visit
    # these url in browser to see how these error pages look like.
    urlpatterns += [
        re_path(r'^400/$', default_views.bad_request, kwargs={'exception': Exception('Bad Request!')}),
        re_path(r'^403/$', default_views.permission_denied, kwargs={'exception': Exception('Permission Denied')}),
        re_path(r'^404/$', default_views.page_not_found, kwargs={'exception': Exception('Page not Found')}),
        re_path(r'^500/$', default_views.server_error),
    ]
    if 'debug_toolbar' in settings.INSTALLED_APPS:
        import debug_toolbar

        urlpatterns = [
                          re_path(r'^__debug__/', include(debug_toolbar.urls)),
                      ] + urlpatterns

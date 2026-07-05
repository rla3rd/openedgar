from django.urls import re_path

from . import views

app_name = 'users'
urlpatterns = [
    re_path(
        r'^$',
        view=views.UserListView.as_view(),
        name='list'
    ),
    re_path(
        r'^~redirect/$',
        view=views.UserRedirectView.as_view(),
        name='redirect'
    ),
    re_path(
        r'^(?P<username>[\w.@+-]+)/$',
        view=views.UserDetailView.as_view(),
        name='detail'
    ),
    re_path(
        r'^~update/$',
        view=views.ProfileEditView.as_view(),
        name='update'
    ),
    re_path(
        r'^coverage/add/$',
        view=views.CoverageAssignmentCreateView.as_view(),
        name='add_coverage'
    ),
    re_path(
        r'^coverage/delete/(?P<pk>\d+)/$',
        view=views.CoverageAssignmentDeleteView.as_view(),
        name='delete_coverage'
    ),
    re_path(
        r'^api/bio/$',
        view=views.AIProfileBioView.as_view(),
        name='api_bio'
    ),
    re_path(
        r'^run-command/$',
        view=views.RunCommandView.as_view(),
        name='run_command'
    ),
    re_path(
        r'^command-logs/$',
        view=views.CommandLogsView.as_view(),
        name='command_logs'
    ),
]


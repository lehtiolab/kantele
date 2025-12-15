from django.urls import path
from corefac import views


app_name = 'corefac'
urlpatterns = [
    path('', views.corefac_home, name='home'),
    path('sampleprep/method/find/', views.find_sampleprep_method),
    path('sampleprep/version/add/', views.add_protocol_version),
    path('sampleprep/version/disable/', views.disable_sampleprep_method_version),
    path('sampleprep/version/enable/', views.enable_sampleprep_method_version),
    path('sampleprep/version/delete/', views.delete_protocol_version),

    path('sampleprep/pipeline/find/', views.find_pipeline),
    path('sampleprep/pipeline/save/', views.save_sampleprep_pipeline),
    path('sampleprep/pipeline/lock/', views.lock_sampleprep_pipeline),
    path('sampleprep/pipeline/disable/', views.disable_sampleprep_pipeline),
    path('sampleprep/pipeline/enable/', views.enable_sampleprep_pipeline),
    path('sampleprep/pipeline/delete/', views.delete_sampleprep_pipeline),

    path('dashboard/projects/', views.get_project_plotdata),
]

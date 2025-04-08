from django.urls import path
from analysis import views


app_name = 'analysis'
urlpatterns = [
    path('new/', views.get_analysis_init),
    path('<int:anid>/', views.get_analysis),
    path('store/', views.store_analysis),
    path('delete/', views.delete_analysis),
    path('stop/', views.stop_analysis),
    path('start/', views.start_analysis),
    path('freeze/', views.freeze_analysis),
    path('unfreeze/', views.unfreeze_analysis),
    path('undelete/', views.undelete_analysis),
    path('purge/', views.purge_analysis),
    path('dsets/<int:wfversion_id>/', views.get_datasets),
    path('baseanalysis/show/', views.get_base_analyses),
    path('baseanalysis/load/<int:wfversion_id>/<int:baseanid>/', views.load_base_analysis),
    path('resultfiles/load/<int:anid>/', views.load_analysis_resultfiles),
    path('workflow/', views.get_workflow_versioned),
    path('logappend/', views.append_analysis_log, name='appendlog'),
    path('nflogappend/', views.nextflow_analysis_log, name='nflog'),
    path('upload/', views.upload_servable_file, name='checkfileupload'),
    path('log/<int:ana_id>', views.show_analysis_log),
    path('showfile/<int:arf_id>', views.serve_analysis_file),
    path('find/datasets/', views.find_datasets),
    path('token/renew/', views.renew_token),
]

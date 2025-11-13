from django.urls import path
from staffpage import views


app_name = 'staffpage'
urlpatterns = [
    path('', views.show_staffpage, name='home'),
    path('qc/newfiles/', views.new_qcfiles),
    path('qc/rerun/', views.rerun_qcs),
    path('qc/remove/', views.remove_qcfiles),
    path('qc/trackpeptides/save/', views.save_tracked_peptides),
    path('qc/trackpeptides/delete/', views.delete_tracked_peptide_set),
    path('servers/save/', views.save_server),
    path('shares/save/', views.save_share),
    ]


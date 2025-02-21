from django.urls import path
from staffpage import views


app_name = 'staffpage'
urlpatterns = [
    path('', views.show_staffpage, name='home'),
    path('qc/searchfiles/', views.get_qc_files),
    path('qc/searchnewfiles/', views.find_unclaimed_files),
    path('qc/rerunmany/', views.rerun_qcs),
    path('qc/rerunsingle/', views.rerun_singleqc),
    path('qc/newfile/', views.new_qcfile),
    path('qc/trackpeptides/save/', views.save_tracked_peptides),
    path('qc/trackpeptides/delete/', views.delete_tracked_peptide_set),
    ]


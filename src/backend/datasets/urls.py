from django.urls import path
from datasets import views

app_name = 'datasets'
urlpatterns = [
    path('new/', views.new_dataset, name="new"),
    path('show/<int:dataset_id>/', views.show_dataset, name="show"),
    path('show/info/<int:dataset_id>/', views.dataset_info),
    path('show/info/', views.dataset_info),
    path('show/project/<int:project_id>/', views.get_project),
    path('show/files/<int:dataset_id>/', views.dataset_files, name="showfiles"),
    path('show/files/', views.dataset_files),
    path('find/files/', views.find_files),
    path('show/samples/<int:dataset_id>/', views.dataset_samples),
    path('show/samples/', views.dataset_samples),
    path('show/labelcheck/<int:dataset_id>/', views.labelcheck_samples),
    path('show/labelcheck/', views.labelcheck_samples),
    path('show/mssamples/<int:dataset_id>/', views.dataset_mssamples),
    path('show/mssamples/', views.dataset_mssamples),
    path('show/components/<int:datatype_id>/', views.get_datatype_components),
    path('show/species/', views.get_species),
    path('save/dataset/', views.save_dataset, name="savedset"),
    path('save/files/', views.save_files, name="savefiles"),
    path('save/files/pending/', views.accept_or_reject_dset_preassoc_files),
    path('save/mssamples/', views.save_mssamples),
    path('save/samples/', views.save_samples),
    path('save/labelcheck/', views.save_labelcheck),
    path('save/owner/', views.change_owners, name="changeowner"),
    path('rename/project/', views.rename_project),
    path('merge/projects/', views.merge_projects),
    path('archive/dataset/', views.move_dataset_cold),
    path('archive/project/', views.move_project_cold),
    path('undelete/dataset/', views.move_dataset_active),
    path('undelete/project/', views.move_project_active),
    path('purge/project/', views.purge_project),
    path('purge/dataset/', views.purge_dataset),
]

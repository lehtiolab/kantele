from django.urls import path
from jobs import views


app_name = 'jobs'
urlpatterns = [
    path('set/task/', views.set_task_status, name='settask'),
    path('retry/', views.retry_job, name='retry'),
    path('set/storagepathds/', views.update_storage_loc_dset, name='updatestorageds'),
    path('set/storagepath/', views.update_storagepath_file, name='updatestorage'),
    path('delete/', views.delete_job, name='deletejob'),
    path('pause/', views.pause_job, name='pausejob'),
    path('resume/', views.resume_job, name='resumejob'),
    path('deletefile/', views.purge_storedfile, name='deletefile'),
    path('deletedir/', views.removed_emptydir, name='rmdir'),
    path('set/external/', views.register_external_file, name='register_external'),
    path('set/downloaded/', views.downloaded_file, name='download_file'),
    path('pdcarchive/set/', views.created_pdc_archive, name='createpdcarchive'),
    path('pdcrestore/set/', views.restored_archive_file, name='restoredpdcarchive'),
    path('set/longqc/', views.store_longitudinal_qc, name='storelongqc'),
    path('set/analysis/', views.analysis_run_done, name='analysisdone'),
    path('set/mzmlfile/', views.mzml_convert_or_refine_file_done, name='mzmlfiledone'),
    path('set/internalfile/', views.confirm_internal_file, name='internalfiledone'),
]

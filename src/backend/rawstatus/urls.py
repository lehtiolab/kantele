from django.urls import path
from rawstatus import views


app_name = 'files'
urlpatterns = [
   path('token/', views.request_upload_token, name='req_token'),
   path('instruments/check/', views.instrument_check_in, name='check_in'),
   path('uploaded/', views.file_uploaded, name='uploaded_file'),
   path('uploaded/mzml/', views.mzml_uploaded, name='uploaded_mzml'),
   path('upload/userfile/', views.browser_userupload, name='upload_browserfile'),
   path('transfer/', views.transfer_file, name='transfer'),
   path('transferstate/', views.get_files_transferstate, name='reg_trfstate'),
   path('rename/', views.rename_file),
   path('external/scan/', views.scan_raws_tmp),
   path('external/import/', views.import_external_data),
   path('archive/', views.archive_file),
   path('undelete/', views.restore_file_from_cold),
   path('datainflow/', views.inflow_page, name='inflow'),
   path('datainflow/download/', views.download_instrument_package),
   path('classifiedraw/', views.classified_rawfile_treatment, name='classifiedraw'),
]

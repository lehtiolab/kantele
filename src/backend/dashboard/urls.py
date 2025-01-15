from django.urls import path
from dashboard import views


app_name = 'dashboard'
urlpatterns = [
    path('', views.dashboard, name='dash'),
    path('longqc/<int:instrument_id>/<str:acqmode>/<int:daysago>/<int:maxdays>', views.show_qc, name='longqc'),
    path('proddata/<int:daysago>/<int:maxdays>', views.get_file_production),
]

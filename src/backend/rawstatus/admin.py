from django.contrib import admin
from rawstatus.models import (Producer, StoredFileType, MSInstrument, MSInstrumentType,
        ServerShare, FileServer, FileserverShare, AnalysisServerProfile)

# Register your models here.

admin.site.register(Producer)
admin.site.register(StoredFileType)
admin.site.register(MSInstrumentType)
admin.site.register(MSInstrument)
admin.site.register(FileServer)
admin.site.register(ServerShare)
admin.site.register(FileserverShare)
admin.site.register(AnalysisServerProfile)

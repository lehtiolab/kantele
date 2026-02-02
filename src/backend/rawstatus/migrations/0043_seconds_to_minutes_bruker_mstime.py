from django.db import migrations
from django.db.models import F


def seconds_to_minutes(a, s):
    MSF = a.get_model('rawstatus', 'MSFileData')
    MSF.objects.filter(rawfile__producer__msinstrument__filetype__name='bruker_raw_file').update(
            mstime=F('mstime') / 60)


def back_to_seconds(a, s):
    MSF = a.get_model('rawstatus', 'MSFileData')
    MSF.objects.filter(rawfile__producer__msinstrument__filetype__name='bruker_raw_file').update(
            mstime=F('mstime') * 60)

class Migration(migrations.Migration):

    dependencies = [
        ('rawstatus', '0042_remove_analysisserverprofile_nfprofiles'),
    ]

    operations = [
        migrations.RunPython(seconds_to_minutes, back_to_seconds),
    ]

# Generated by Django 3.2.13 on 2023-01-11 19:10
from datetime import datetime

from django.db import migrations, models


def add_storage_field(apps, s):
    Analysis = apps.get_model('analysis', 'Analysis')
    for x in Analysis.objects.exclude(nextflowsearch__workflow__shortname__name='QC').exclude(nextflowsearch__isnull=True):
        x.storage_dir = f'{x.user.username}/{x.pk}_{x.nextflowsearch.workflow.shortname.name}_{x.name}_{datetime.strftime(x.date, "%Y%m%d_%H.%M")}'
        x.save()


def remove_storage_field(apps, s):
    # not needed
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0040_alter_uniprotfasta_dbtype'),
    ]

    operations = [
        migrations.AddField(
            model_name='analysis',
            name='storage_dir',
            field=models.TextField(default=''),
            preserve_default=False,
        ),

        migrations.RunPython(add_storage_field, remove_storage_field),

        migrations.CreateModel(
            name='WfOutput',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('psmfile', models.TextField()),
                ('pepfile', models.TextField()),
                ('lookup', models.TextField()),
                ('peppeptide', models.TextField()),
                ('psmcountfield', models.TextField()),
                ('pepfdrfield', models.TextField()),
                ('psmfdrfield', models.TextField()),
                ('pepqval', models.TextField()),
                ('psmfnfield', models.TextField()),
                ('scanfield', models.TextField()),
                ('psmsetname', models.TextField()),
                ('psmpeptide', models.TextField()),
                ('nfwfversion', models.OneToOneField(on_delete=models.deletion.CASCADE, to='analysis.nextflowwfversion')),
            ],
        ),
    ]
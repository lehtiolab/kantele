# Generated by Django 3.2.13 on 2022-12-07 14:57

from django.db import migrations, models
from django.db.models import F, Value


def caniso_to_canisoref(apps, s):
    up = apps.get_model('analysis', 'UniProtFasta')
    for fasta in up.objects.all():
        fasta.dbtype = int(fasta.isoforms)
        fasta.save()


def reverse(apps, s):
    up = apps.get_model('analysis', 'UniProtFasta')
    for fasta in up.objects.all():
        fasta.isoforms = bool(fasta.dbtype)
        fasta.save()


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0038_nextflowwfversion_profiles'),
    ]

    operations = [
        migrations.AddField(
            model_name='uniprotfasta',
            name='dbtype',
            field=models.IntegerField(choices=[(0, 'Swiss_canonical'), (1, 'Swiss_canonical_isoforms'), (2, 'Reference_proteome')]),
            preserve_default=False,
        ),
        migrations.RunPython(caniso_to_canisoref, reverse),

        migrations.RemoveField(
            model_name='uniprotfasta',
            name='isoforms',
        ),
    ]

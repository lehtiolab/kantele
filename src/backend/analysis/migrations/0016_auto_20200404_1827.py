# -*- coding: utf-8 -*-
# Generated by Django 1.11.23 on 2020-04-04 18:27
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion

from kantele import settings


def init_mzmls(apps, sch_editor):
    MzmlFile = apps.get_model('analysis', 'MzmlFile')
    SFile = apps.get_model('rawstatus', 'StoredFile')
    PWiz = apps.get_model('analysis', 'Proteowizard')
    NfWfV = apps.get_model('analysis', 'NextflowWfVersion')
    NfWf = apps.get_model('analysis', 'NextflowWorkflow')
    nfwf = NfWf.objects.create(description='Proteowizard msconvert WF', repo='https://github.com/lehtiolab/nf-msconvert')
    nfwfver = NfWfV.objects.create(update='Windows pwiz/old refiner', commit='fake', filename='tasks', nfworkflow=nfwf)
    convpwiz = PWiz.objects.create(id=1, version_description='v0.0 - 3.0.19127.a8f2dc212', container_version='none', is_docker=False, nf_version=nfwfver)
    MzmlFile.objects.bulk_create(
            MzmlFile(refined=False, pwiz=convpwiz, sfile=x) for x in SFile.objects.filter(filetype=settings.MZML_SFGROUP_ID)
            )
    MzmlFile.objects.bulk_create(
            MzmlFile(refined=True, pwiz=convpwiz, sfile=x) for x in SFile.objects.filter(filetype=settings.REFINEDMZML_SFGROUP_ID)
            )


def revert_mzml(apps, sch_editor):
    MzmlFile = apps.get_model('analysis', 'MzmlFile')
    PWiz = apps.get_model('analysis', 'Proteowizard')
    MzmlFile.objects.all().delete()
    PWiz.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ('rawstatus', '0016_auto_20200404_1827'),
        ('analysis', '0015_ensemblfasta_uniprotfasta'),
    ]

    operations = [
        migrations.CreateModel(
            name='MzmlFile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('refined', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='Proteowizard',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('version_description', models.TextField()),
                ('container_version', models.TextField()),
                ('date_added', models.DateTimeField(auto_now_add=True)),
                ('is_docker', models.BooleanField(default=False)),
                ('nf_version', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='analysis.NextflowWfVersion')),
            ],
        ),
        migrations.AddField(
            model_name='mzmlfile',
            name='pwiz',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='analysis.Proteowizard'),
        ),
        migrations.AddField(
            model_name='mzmlfile',
            name='sfile',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='rawstatus.StoredFile'),
        ),

        migrations.RunPython(init_mzmls, revert_mzml),
    ]
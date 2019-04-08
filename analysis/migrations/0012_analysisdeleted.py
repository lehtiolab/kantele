# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-04-05 18:23
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


def add_deleted_analyses(apps, schema_editor):
    Analysis = apps.get_model('analysis', 'Analysis')
    AnalysisDeleted = apps.get_model('analysis', 'AnalysisDeleted')
    for ana in Analysis.objects.filter(deleted=True):
        ana_del = AnalysisDeleted(analysis=ana)
        ana_del.save()


def remove_deleted_analyses(apps, schema_editor):
    AnalysisDeleted = apps.get_model('analysis', 'AnalysisDeleted')
    AnalysisDeleted.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0011_auto_20190404_0806'),
    ]

    operations = [
        migrations.CreateModel(
            name='AnalysisDeleted',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateTimeField(auto_now_add=True)),
                ('analysis', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='analysis.Analysis')),
            ],
        ),
        migrations.RunPython(add_deleted_analyses, remove_deleted_analyses),
    ]

# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-04-08 11:09
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def add_project_types_for_projects(apps, schema_editor):
    Project = apps.get_model('datasets', 'Project')
    ProjectTypeName = apps.get_model('datasets', 'ProjectTypeName')
    ProjType = apps.get_model('datasets', 'ProjType')
    ptmap = {True: ProjectTypeName(name='Core facility'), False: ProjectTypeName(name='Local')}
    [x.save() for x in ptmap.values()]
    ProjType.objects.bulk_create([ProjType(project=x, ptype=ptmap[x.corefac]) for x in Project.objects.all()])


def remove_project_types_revert_to_corefac_proj(apps, schema_editor):
    Project = apps.get_model('datasets', 'Project')
    ProjType = apps.get_model('datasets', 'ProjType')
    ProjectTypeName = apps.get_model('datasets', 'ProjectTypeName')
    cfptype = ProjectTypeName.objects.get(name='Core facility')
    for ptype in ProjType.objects.all():
        Project.objects.filter(pk=ptype.project_id).update(corefac=lambda: ptype.ptype == cfptype)
        

class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('datasets', '0004_auto_20190408_1017'),
    ]

    operations = [
        migrations.CreateModel(
            name='ProjectTypeName',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=50)),
            ],
        ),
        migrations.CreateModel(
            name='ProjType',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('project', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.Project')),
                ('ptype', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.ProjectTypeName')),
            ],
        ),
        migrations.CreateModel(
            name='UserPtype',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ptype', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.ProjectTypeName')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.RenameModel(
            old_name='CorefacDatasetContact',
            new_name='ExternalDatasetContact',
        ),
        migrations.RunPython(add_project_types_for_projects, remove_project_types_revert_to_corefac_proj),
        migrations.RemoveField(
            model_name='project',
            name='corefac',
        ),
    ]

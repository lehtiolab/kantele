# -*- coding: utf-8 -*-
# Generated by Django 1.11.3 on 2018-05-16 08:10
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('rawstatus', '__first__'),
        ('jobs', '__first__'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='CheckboxParameter',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=100)),
                ('active', models.BooleanField(default=True)),
            ],
        ),
        migrations.CreateModel(
            name='CheckboxParameterOption',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('value', models.CharField(max_length=100)),
                ('param', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.CheckboxParameter')),
            ],
        ),
        migrations.CreateModel(
            name='CheckboxParameterValue',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
            ],
        ),
        migrations.CreateModel(
            name='CorefacDatasetContact',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('email', models.CharField(max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='Dataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateTimeField(verbose_name='date created')),
                ('storage_loc', models.TextField(max_length=200)),
            ],
        ),
        migrations.CreateModel(
            name='DatasetComponent',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.TextField(max_length=50, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='DatasetComponentState',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('state', models.TextField(max_length=20)),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
            ],
        ),
        migrations.CreateModel(
            name='DatasetJob',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='jobs.Job')),
            ],
        ),
        migrations.CreateModel(
            name='DatasetRawFile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
                ('rawfile', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='rawstatus.RawFile')),
            ],
        ),
        migrations.CreateModel(
            name='DatasetSpecies',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
            ],
        ),
        migrations.CreateModel(
            name='Datatype',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.TextField(max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='DatatypeComponent',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('component', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.DatasetComponent')),
                ('datatype', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Datatype')),
            ],
        ),
        migrations.CreateModel(
            name='Enzyme',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=30)),
            ],
        ),
        migrations.CreateModel(
            name='EnzymeDataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
                ('enzyme', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Enzyme')),
            ],
        ),
        migrations.CreateModel(
            name='Experiment',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=200)),
            ],
        ),
        migrations.CreateModel(
            name='FieldParameter',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=100)),
                ('placeholder', models.CharField(max_length=100)),
                ('active', models.BooleanField(default=True)),
            ],
        ),
        migrations.CreateModel(
            name='FieldParameterValue',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('value', models.CharField(max_length=100)),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
                ('param', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.FieldParameter')),
            ],
        ),
        migrations.CreateModel(
            name='HiriefDataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
            ],
        ),
        migrations.CreateModel(
            name='HiriefRange',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('start', models.DecimalField(decimal_places=2, max_digits=5)),
                ('end', models.DecimalField(decimal_places=2, max_digits=5)),
            ],
        ),
        migrations.CreateModel(
            name='Operator',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name='OperatorDataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
                ('operator', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Operator')),
            ],
        ),
        migrations.CreateModel(
            name='ParamLabcategory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('labcategory', models.CharField(max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='ParamType',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('typename', models.CharField(max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='Prefractionation',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=20)),
            ],
        ),
        migrations.CreateModel(
            name='PrefractionationDataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
                ('prefractionation', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Prefractionation')),
            ],
        ),
        migrations.CreateModel(
            name='PrefractionationFractionAmount',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('fractions', models.IntegerField()),
                ('pfdataset', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.PrefractionationDataset')),
            ],
        ),
        migrations.CreateModel(
            name='PrefractionationLength',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('length', models.CharField(max_length=20)),
                ('pfdataset', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.PrefractionationDataset')),
            ],
        ),
        migrations.CreateModel(
            name='PrincipalInvestigator',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.TextField(max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='Project',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('corefac', models.BooleanField()),
                ('pi', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.PrincipalInvestigator')),
            ],
        ),
        migrations.CreateModel(
            name='QuantChannel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=20)),
            ],
        ),
        migrations.CreateModel(
            name='QuantChannelSample',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('sample', models.CharField(max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='QuantDataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
            ],
        ),
        migrations.CreateModel(
            name='QuantSampleFile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('sample', models.CharField(max_length=100)),
                ('rawfile', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.DatasetRawFile')),
            ],
        ),
        migrations.CreateModel(
            name='QuantType',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=20)),
                ('shortname', models.CharField(max_length=15)),
            ],
        ),
        migrations.CreateModel(
            name='QuantTypeChannel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('channel', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.QuantChannel')),
                ('quanttype', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.QuantType')),
            ],
        ),
        migrations.CreateModel(
            name='ReversePhaseDataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('length', models.CharField(max_length=20)),
                ('dataset', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
            ],
        ),
        migrations.CreateModel(
            name='RunName',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.TextField(max_length=100)),
                ('experiment', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Experiment')),
            ],
        ),
        migrations.CreateModel(
            name='SelectParameter',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=100)),
                ('active', models.BooleanField(default=True)),
                ('category', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.ParamLabcategory')),
            ],
        ),
        migrations.CreateModel(
            name='SelectParameterOption',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('value', models.CharField(max_length=100)),
                ('param', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.SelectParameter')),
            ],
        ),
        migrations.CreateModel(
            name='SelectParameterValue',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset')),
                ('value', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.SelectParameterOption')),
            ],
        ),
        migrations.CreateModel(
            name='Species',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('linnean', models.TextField(unique=True)),
                ('popname', models.TextField()),
            ],
        ),
        migrations.AddField(
            model_name='quantdataset',
            name='quanttype',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.QuantType'),
        ),
        migrations.AddField(
            model_name='quantchannelsample',
            name='channel',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.QuantTypeChannel'),
        ),
        migrations.AddField(
            model_name='quantchannelsample',
            name='dataset',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset'),
        ),
        migrations.AddField(
            model_name='hiriefdataset',
            name='hirief',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.HiriefRange'),
        ),
        migrations.AddField(
            model_name='hiriefdataset',
            name='pfdataset',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.PrefractionationDataset'),
        ),
        migrations.AddField(
            model_name='fieldparameter',
            name='category',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.ParamLabcategory'),
        ),
        migrations.AddField(
            model_name='fieldparameter',
            name='paramtype',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.ParamType'),
        ),
        migrations.AddField(
            model_name='experiment',
            name='project',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Project'),
        ),
        migrations.AddField(
            model_name='datasetspecies',
            name='species',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Species'),
        ),
        migrations.AddField(
            model_name='datasetcomponentstate',
            name='dtcomp',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.DatatypeComponent'),
        ),
        migrations.AddField(
            model_name='dataset',
            name='datatype',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Datatype'),
        ),
        migrations.AddField(
            model_name='dataset',
            name='runname',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.RunName'),
        ),
        migrations.AddField(
            model_name='dataset',
            name='user',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='corefacdatasetcontact',
            name='dataset',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset'),
        ),
        migrations.AddField(
            model_name='checkboxparametervalue',
            name='dataset',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.Dataset'),
        ),
        migrations.AddField(
            model_name='checkboxparametervalue',
            name='value',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.CheckboxParameterOption'),
        ),
        migrations.AddField(
            model_name='checkboxparameter',
            name='category',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.ParamLabcategory'),
        ),
    ]

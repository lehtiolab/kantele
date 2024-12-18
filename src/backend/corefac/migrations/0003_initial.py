# Generated by Django 4.2.13 on 2024-12-18 09:32

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('datasets', '0022_sampleprepparameter_sampleprepparameteroption_and_more'),
        ('corefac', '0002_auto_20190415_1504'),
    ]

    operations = [
        migrations.CreateModel(
            name='SamplePipeline',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.TextField(unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='PrepOptionProtocol',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('doi', models.TextField(unique=True)),
                ('version', models.TextField()),
                ('active', models.BooleanField(default=True)),
                ('paramopt', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='datasets.sampleprepparameteroption')),
            ],
        ),
        migrations.CreateModel(
            name='PipelineVersion',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('version', models.TextField()),
                ('timestamp', models.DateTimeField()),
                ('locked', models.BooleanField(default=False)),
                ('active', models.BooleanField(default=True)),
                ('pipeline', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='corefac.samplepipeline')),
            ],
        ),
        migrations.CreateModel(
            name='PipelineStep',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('index', models.IntegerField()),
                ('pipelineversion', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='corefac.pipelineversion')),
                ('step', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='corefac.prepoptionprotocol')),
            ],
        ),
        migrations.CreateModel(
            name='PipelineEnzyme',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('enzyme', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='datasets.enzyme')),
                ('pipelineversion', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='corefac.pipelineversion')),
            ],
        ),
        migrations.CreateModel(
            name='DatasetPipeline',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='datasets.dataset')),
                ('pipelineversion', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='corefac.pipelineversion')),
            ],
        ),
        migrations.CreateModel(
            name='DatasetPrepTrackingNodate',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('finished', models.BooleanField()),
                ('dspipe', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='corefac.datasetpipeline')),
                ('stage', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='corefac.pipelinestep')),
            ],
        ),
        migrations.CreateModel(
            name='DatasetPrepTracking',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('stage', models.IntegerField(choices=[(1, 'Samples arrived'), (2, 'Prep started'), (3, 'Prep finished'), (4, 'Queued on MS')])),
                ('timestamp', models.DateTimeField()),
                ('dspipe', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='corefac.datasetpipeline')),
            ],
        ),
        migrations.AddConstraint(
            model_name='pipelineversion',
            constraint=models.UniqueConstraint(fields=('pipeline', 'version'), name='uni_pipelineversion'),
        ),
        migrations.AddConstraint(
            model_name='pipelinestep',
            constraint=models.UniqueConstraint(fields=('index', 'pipelineversion_id'), name='uni_pipelinestep'),
        ),
        migrations.AddConstraint(
            model_name='pipelineenzyme',
            constraint=models.UniqueConstraint(fields=('enzyme_id', 'pipelineversion_id'), name='uni_pipelineenzyme'),
        ),
        migrations.AddConstraint(
            model_name='datasetpreptrackingnodate',
            constraint=models.UniqueConstraint(fields=('dspipe', 'stage'), name='uni_dspipenotrack'),
        ),
        migrations.AddConstraint(
            model_name='datasetpreptracking',
            constraint=models.UniqueConstraint(fields=('dspipe', 'stage'), name='uni_dspipe'),
        ),
    ]

# Generated by Django 3.1.2 on 2020-11-18 16:58

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0026_auto_20201021_1145'),
    ]

    operations = [
        migrations.CreateModel(
            name='AnalysisBaseanalysis',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('is_complement', models.BooleanField()),
                ('shadow_isoquants', models.JSONField()),
                ('shadow_dssetnames', models.JSONField()),
                ('analysis', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='analysis', to='analysis.analysis')),
                ('base_analysis', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='base_analysis', to='analysis.analysis')),
            ],
        ),
        migrations.AlterField(
            model_name='analysisdatasetsetname',
            name='setname',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='analysis.analysissetname'),
        ),
        migrations.DeleteModel(
            name='AnalysisFractionRegex',
        ),
    ]
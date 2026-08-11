import django.db.models.deletion
from django.db import migrations, models


def fake(a, s):
    pass

def fill_dsetsetname(a, s):
    ADS = a.get_model('analysis', 'AnalysisDatasetSetname')
    ASV = a.get_model('analysis', 'AnalysisSetValue')
    DSA = a.get_model('analysis', 'DatasetAnalysis')
    alldsa = {f'{x.dataset_id}__{x.analysis_id}': x for x in DSA.objects.all()}
    for asv in ASV.objects.distinct('dataset', 'analysis').values('dataset_id', 'analysis_id', 'setname_id'):
        ADS.objects.create(dsanalysis=alldsa[f'{asv["dataset_id"]}__{asv["analysis_id"]}'], setname_id=asv['setname_id'])

    # Remove duplicates in ASV (no longer dataset-specific)
    dups = ASV.objects.values('setname', 'field').annotate(c=models.Count('field')).filter(c__gt=1)
    setnames = [x['setname'] for x in dups]
    keep = ASV.objects.filter(setname__in=setnames).distinct('setname').values('pk')
    ASV.objects.filter(setname__in=setnames).exclude(pk__in=keep).delete()


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0065_remove_nfconfigfile_nfconfig_and_more'),
        ('rawstatus', '0044_analysisserverprofile_analysisoutshare_and_more'),
    ]

    operations = [
        # Remove constraints first so we dont get errors
        migrations.RemoveConstraint(
            model_name='analysisdatasetsetvalue',
            name='uni_anadsetsfields',
        ),
        migrations.RemoveConstraint(
            model_name='analysisdsinputfile',
            name='uni_anaset_infile',
        ),
        migrations.RemoveConstraint(
            model_name='analysisfilevalue',
            name='uni_anassamplefile',
        ),

        # Remove FK index
        migrations.AlterField(
            model_name='analysisdatasetsetvalue',
            name='setname',
            field=models.ForeignKey(db_index=False, on_delete=django.db.models.deletion.CASCADE, to='analysis.analysissetname'),
        ),
        # Make field not nullable
        migrations.AlterField(
            model_name='analysisdatasetsetvalue',
            name='setname',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='analysis.analysissetname'),
            preserve_default=False,
        ),
        # Create the new model without index name collision
        migrations.CreateModel(
            name='AnalysisDatasetSetname',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('setname', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='analysis.analysissetname')),
                ('dsanalysis', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='analysis.datasetanalysis')),
            ],
        ),

        # Rename the datasetsetvalue model
        migrations.RenameModel(
            old_name='AnalysisDatasetSetValue',
            new_name='AnalysisSetValue',
        ),
        # Add back FK index
        migrations.AlterField(
            model_name='analysissetvalue',
            name='setname',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='analysis.analysissetname'),
        ),


        migrations.RemoveField(
            model_name='analysisdsinputfile',
            name='analysisset',
        ),

        # Fill with data and fix the new dependencies
        migrations.RunPython(fill_dsetsetname, fake),

        migrations.AddField(
            model_name='analysisfilevalue',
            name='adsfile',
            field=models.ForeignKey(default=1, on_delete=django.db.models.deletion.CASCADE, to='analysis.analysisdsinputfile'),
            preserve_default=False,
        ),
    ]

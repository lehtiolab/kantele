# Generated by Django 3.2.13 on 2024-02-19 13:55

from django.db import migrations, models


def populate_nfwf_m2m(apps, s):
    WF = apps.get_model('analysis', 'Workflow')
    for wf in WF.objects.all():
        for nfwfv in wf.nfworkflow.nextflowwfversion_set.all():
            wf.nfworkflows.add(nfwfv)


def reverse_m2m(apps, s):
    WF = apps.get_model('analysis', 'Workflow')
    for wf in WF.objects.all():
        wf.nfworkflow = wf.nfworkflows.last().nfworkflow


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0044_deletes'),
    ]

    operations = [
        # Workflow link m2m to the nfwf version
        migrations.AddField(
            model_name='workflow',
            name='nfworkflows',
            field=models.ManyToManyField(to='analysis.NextflowWfVersion'),
        ),

        migrations.RunPython(populate_nfwf_m2m, reverse_m2m),

        migrations.RemoveField(
            model_name='workflow',
            name='nfworkflow',
        ),

        # wftype becomes an integerchoice
        migrations.AlterField(
            model_name='workflow',
            name='shortname',
            field=models.IntegerField(choices=[(1, 'Quantitative proteomics'), (2, 'Instrument quality control'), (3, 'Other proteomic, special DB'), (4, 'Proteogenomics DB generation'), (5, 'pI-separated identification'), (6, 'Special internal'), (7, 'Labelcheck')]),
        ),

        migrations.RenameField(
            model_name='workflow',
            old_name='shortname',
            new_name='wftype',
        ),

        migrations.DeleteModel(
            name='WorkflowType',
        ),

        # Model / field name changes below
        migrations.RenameModel(
            old_name='NextflowWorkflow',
            new_name='NextflowWorkflowRepo',
        ),
        migrations.RenameField(
            model_name='nextflowsearch',
            old_name='nfworkflow',
            new_name='nfwfversionparamset',
        ),
        migrations.RenameModel(
            old_name='NextflowWfVersion',
            new_name='NextflowWfVersionParamset',
        ),
        migrations.RenameModel(
            old_name='Workflow',
            new_name='UserWorkflow',
        ),
        migrations.RenameField(
            model_name='userworkflow',
            old_name='nfworkflows',
            new_name='nfwfversionparamsets',
        ),
        migrations.AlterField(
            model_name='nextflowsearch',
            name='workflow',
            field=models.ForeignKey(on_delete=models.deletion.CASCADE, to='analysis.userworkflow'),
        ),
        # Correct earlier mistake on db level (code already up and fine), where 4 wasnt LC
        # but MS search comp (which 5 also is/was)
        migrations.AlterField(
            model_name='psetcomponent',
            name='component',
            field=models.IntegerField(choices=[(1, 'Isobaric quant summarizing with denominators, median sweep, or intensity'), (2, 'Input file definition of specific type, value eg [path, instrument, sample]'), (3, 'Sampletable for isobaric quant'), (4, 'Labelcheck isoquant'), (5, 'MS search complementing earlier run or rerun from PSMs'), (6, 'Prefractionated MS data'), (7, 'HiRIEF strip tolerance')]),
        ),

    ]

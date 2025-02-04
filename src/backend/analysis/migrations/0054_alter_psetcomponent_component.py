# Generated by Django 4.2.13 on 2025-02-04 16:39

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0053_analysis_editable_alter_userworkflow_wftype_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='psetcomponent',
            name='component',
            field=models.IntegerField(choices=[(1, 'Isobaric quant summarizing with denominators, median sweep, or intensity'), (2, 'Input file definition of specific type, value eg [path, instrument, sample]'), (3, 'Sampletable for isobaric quant'), (4, 'Labelcheck isoquant'), (5, 'MS search complementing earlier run or rerun from PSMs'), (6, 'Prefractionated MS data'), (7, 'HiRIEF strip tolerance'), (8, 'Remove channel from experiment set')]),
        ),
    ]

# Generated by Django 3.2.13 on 2023-12-12 10:21

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0017_seqsamplefile'),
        ('analysis', '0043_auto_20231129_1518'),
    ]

    operations = [
        migrations.DeleteModel(
            name='AnalysisMzmldef',
        ),
        migrations.DeleteModel(
            name='WFInputComponent',
        ),
    ]

# Generated by Django 4.2.13 on 2025-02-20 12:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('dashboard', '0005_trackedpeptide_trackedpeptideset_peptidetrackdata_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='trackedpeptideset',
            name='active',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='trackedpeptideset',
            name='date',
            field=models.DateTimeField(auto_now=True),
        ),
        migrations.AlterField(
            model_name='trackedpeptideset',
            name='name',
            field=models.TextField(unique=True),
        ),
    ]

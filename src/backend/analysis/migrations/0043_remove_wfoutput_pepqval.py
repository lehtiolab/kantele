# Generated by Django 3.2.13 on 2023-01-19 21:04

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0042_wfoutput_psmscorefield'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='wfoutput',
            name='pepqval',
        ),
    ]
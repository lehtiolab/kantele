# -*- coding: utf-8 -*-
# Generated by Django 1.11.3 on 2018-10-15 12:18
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('rawstatus', '0008_auto_20181015_0925'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='rawfile',
            name='deleted',
        ),
        migrations.AddField(
            model_name='storedfile',
            name='deleted',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='swestorebackedupfile',
            name='deleted',
            field=models.BooleanField(default=False),
        ),
    ]

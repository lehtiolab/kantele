# -*- coding: utf-8 -*-
# Generated by Django 1.11.3 on 2018-10-15 12:26
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('rawstatus', '0009_auto_20181015_1218'),
    ]

    operations = [
        migrations.AlterField(
            model_name='storedfile',
            name='regdate',
            field=models.DateTimeField(auto_now=True),
        ),
    ]

# -*- coding: utf-8 -*-
# Generated by Django 1.11.3 on 2018-07-17 17:28
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analysis', '0003_nextflowsearch_workflow'),
    ]

    operations = [
        migrations.AlterField(
            model_name='analysis',
            name='name',
            field=models.CharField(max_length=500),
        ),
    ]
# -*- coding: utf-8 -*-
# Generated by Django 1.11.23 on 2020-02-14 13:48
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0010_project_registered'),
    ]

    operations = [
        migrations.AddField(
            model_name='datatype',
            name='public',
            field=models.BooleanField(default=True),
        ),
    ]
# Generated by Django 3.1.2 on 2021-03-19 12:46

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('rawstatus', '0019_msinstrument_active'),
    ]

    operations = [
        migrations.AlterField(
            model_name='producer',
            name='client_id',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='producer',
            name='name',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='producer',
            name='shortname',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='rawfile',
            name='name',
            field=models.TextField(max_length=100),
        ),
        migrations.AlterField(
            model_name='servershare',
            name='name',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='servershare',
            name='share',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='servershare',
            name='uri',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='storedfile',
            name='filename',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='storedfile',
            name='path',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='storedfiletype',
            name='filetype',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='storedfiletype',
            name='name',
            field=models.TextField(unique=True),
        ),
        migrations.AlterField(
            model_name='swestorebackedupfile',
            name='swestore_path',
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name='userfile',
            name='description',
            field=models.TextField(),
        ),
    ]
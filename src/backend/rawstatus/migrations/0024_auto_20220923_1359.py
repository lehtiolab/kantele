# Generated by Django 3.2.13 on 2022-09-23 13:59

from django.db import migrations, models
import django.db.models.deletion


def add_fileserver(apps, sched):
    FileServer = apps.get_model('rawstatus', 'FileServer')
    FileServer.objects.create(name='Fileserver', uri='example.kantele.server')


def fake_remove_fileserver(apps, sched):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('rawstatus', '0023_auto_20211213_1332_squashed_0025_auto_20211213_1534'),
    ]

    operations = [
        migrations.CreateModel(
            name='FileServer',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.TextField(unique=True)),
                ('uri', models.TextField()),
            ],
        ),

        migrations.RunPython(add_fileserver, fake_remove_fileserver),
        
        migrations.RemoveField(
            model_name='servershare',
            name='uri',
        ),
        migrations.AddField(
            model_name='servershare',
            name='server',
            field=models.ForeignKey(default=1, on_delete=django.db.models.deletion.CASCADE, to='rawstatus.fileserver'),
            preserve_default=False,
        ),
    ]

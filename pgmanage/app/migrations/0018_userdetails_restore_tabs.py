# Generated by Django 3.2.18 on 2024-02-27 14:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0017_merge_20240209_1300'),
    ]

    operations = [
        migrations.AddField(
            model_name='userdetails',
            name='restore_tabs',
            field=models.BooleanField(default=True),
        ),
    ]

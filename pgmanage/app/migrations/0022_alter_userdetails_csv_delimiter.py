# Generated by Django 4.2.11 on 2024-10-09 09:55

from django.db import migrations, models
def update_csv_separator_value(apps, schema_editor):
    UserDetails = apps.get_model('app', 'UserDetails')

    for ud in UserDetails.objects.all():
        ud.csv_delimiter = ','
        ud.save()

class Migration(migrations.Migration):

    dependencies = [
        ('app', '0021_group_unique_user_group_name'),
    ]

    operations = [
        migrations.AlterField(
            model_name='userdetails',
            name='csv_delimiter',
            field=models.CharField(default=',', max_length=10),
        ),
        migrations.RunPython(update_csv_separator_value, migrations.RunPython.noop),
    ]

from django.db import migrations, models


def set_default_credentials_extra(apps, schema_editor):
    Connection = apps.get_model("app", "Connection")

    Connection.objects.filter(technology__name="postgresql").update(
        credentials_extra={"auth_method": "user-pass"}
    )


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0032_clear_old_erd_layouts'),
    ]

    operations = [
        migrations.AddField(
            model_name='connection',
            name='credentials_extra',
            field=models.JSONField(default=dict),
        ),
        migrations.RunPython(set_default_credentials_extra, migrations.RunPython.noop)
    ]

# Generated by Django 5.0.2 on 2024-12-13 13:46

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('openedgar', '0009_delete_filingindex_rename_s3_path_companyfiling_path_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='companyfiling',
            name='name',
            field=models.CharField(db_index=True, max_length=1024, null=True),
        ),
    ]
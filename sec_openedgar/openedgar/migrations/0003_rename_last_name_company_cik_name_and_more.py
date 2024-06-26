# Generated by Django 5.0.2 on 2024-04-03 23:30

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('openedgar', '0002_auto_20180624_1319'),
    ]

    operations = [
        migrations.RenameField(
            model_name='company',
            old_name='last_name',
            new_name='cik_name',
        ),
        migrations.RenameField(
            model_name='companyinfo',
            old_name='date',
            new_name='asof',
        ),
        migrations.RenameField(
            model_name='companyinfo',
            old_name='state_incorporation',
            new_name='state_of_incorporation',
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='category',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='description',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='ein',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='entity_type',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='exchanges',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=1024), null=True, size=None),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='fiscal_year_end',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='flags',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='former_names',
            field=models.JSONField(null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='industry',
            field=models.CharField(db_index=True, max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='insider_transaction_for_issuer_exists',
            field=models.SmallIntegerField(default=0),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='insider_transaction_for_owner_exists',
            field=models.SmallIntegerField(default=0),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='investor_website',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='is_company',
            field=models.BooleanField(default=True),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='mailing_address',
            field=models.JSONField(null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='phone',
            field=models.CharField(max_length=20, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='sic_description',
            field=models.CharField(db_index=True, max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='state_of_incorporation_description',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='tickers',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=14), null=True, size=None),
        ),
        migrations.AddField(
            model_name='companyinfo',
            name='website',
            field=models.CharField(max_length=1024, null=True),
        ),
        migrations.AlterField(
            model_name='companyinfo',
            name='business_address',
            field=models.JSONField(null=True),
        ),
        migrations.AlterField(
            model_name='companyinfo',
            name='sic',
            field=models.CharField(db_index=True, max_length=4, null=True),
        ),
    ]

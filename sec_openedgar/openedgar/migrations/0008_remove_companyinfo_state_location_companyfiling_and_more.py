# Generated by Django 5.0.2 on 2024-05-09 12:22

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('openedgar', '0007_remove_companyinfo_id_alter_companyinfo_cik'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='companyinfo',
            name='state_location',
        ),
        migrations.CreateModel(
            name='CompanyFiling',
            fields=[
                ('form_type', models.CharField(db_index=True, max_length=64, null=True)),
                ('accession_number', models.CharField(max_length=1024, primary_key=True, serialize=False)),
                ('date_filed', models.DateField(db_index=True, null=True)),
                ('sha1', models.CharField(db_index=True, max_length=1024, null=True)),
                ('s3_path', models.CharField(db_index=True, max_length=1024)),
                ('document_count', models.IntegerField(default=0)),
                ('is_processed', models.BooleanField(db_index=True, default=False)),
                ('is_error', models.BooleanField(db_index=True, default=False)),
                ('cik', models.ForeignKey(db_column='cik', on_delete=django.db.models.deletion.CASCADE, to='openedgar.company')),
            ],
        ),
        migrations.CreateModel(
            name='CompanyFacts',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('fact', models.CharField(db_index=True, max_length=1024)),
                ('namespace', models.CharField(db_index=True, max_length=1024)),
                ('value', models.FloatField(db_index=True)),
                ('start_date', models.DateField()),
                ('end_date', models.DateField()),
                ('datefiled', models.DateField(db_index=True)),
                ('fiscal_year', models.IntegerField(db_index=True, max_length=1024)),
                ('fiscal_period', models.CharField(db_index=True, max_length=1024)),
                ('formtype', models.CharField(max_length=1024)),
                ('frame', models.CharField(max_length=1024)),
                ('cik', models.ForeignKey(db_column='cik', on_delete=django.db.models.deletion.CASCADE, to='openedgar.company')),
                ('accession_number', models.ForeignKey(db_column='accession_number', on_delete=django.db.models.deletion.CASCADE, to='openedgar.companyfiling')),
            ],
        ),
        migrations.AlterField(
            model_name='filingdocument',
            name='filing',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='openedgar.companyfiling'),
        ),
        migrations.DeleteModel(
            name='Filing',
        ),
    ]

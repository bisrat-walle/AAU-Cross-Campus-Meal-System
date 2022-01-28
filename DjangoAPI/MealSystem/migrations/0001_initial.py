# Generated by Django 4.0.1 on 2022-01-24 12:24

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('auth', '0012_alter_user_first_name_max_length'),
    ]

    operations = [
        migrations.CreateModel(
            name='Admin',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=40)),
                ('username', models.CharField(max_length=20)),
            ],
        ),
        migrations.CreateModel(
            name='MealStatus',
            fields=[
                ('student_id', models.CharField(max_length=20, primary_key=True, serialize=False)),
                ('breakfast', models.BooleanField(blank=True)),
                ('lunch', models.BooleanField(blank=True)),
                ('dinner', models.BooleanField(blank=True)),
            ],
        ),
        migrations.CreateModel(
            name='Schedule',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('bach', models.CharField(choices=[('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('5', '5')], max_length=50)),
                ('department', models.CharField(choices=[('software engineer', 'software engineer'), ('electrical Engineer', 'electrical Engineer'), ('mechanical Engineer', 'mechanical Engineer'), ('biomedical Engineer', 'biomedical Engineer'), ('civil Engineer', 'civil Engineer'), ('chemical Engineer', 'chemical Engineer')], max_length=30)),
                ('section', models.CharField(max_length=2)),
                ('campus', models.CharField(choices=[('4killo', '4killo'), ('5killo', '5killo'), ('6killo', '6killo')], max_length=50)),
                ('day', models.CharField(choices=[('monday', 'monday'), ('tuesday', 'tuesday'), ('wednesday', 'wednesday'), ('thursday', 'thursday'), ('friday', 'friday'), ('saturday', 'saturday'), ('sunday', 'sunday')], max_length=20)),
                ('schedule_id', models.CharField(max_length=50, null=True, unique=True)),
                ('startTime', models.TimeField()),
                ('endTime', models.TimeField()),
            ],
        ),
        migrations.CreateModel(
            name='Student',
            fields=[
                ('name', models.CharField(max_length=50)),
                ('department', models.CharField(choices=[('software engineer', 'software engineer'), ('electrical Engineer', 'electrical Engineer'), ('mechanical Engineer', 'mechanical Engineer'), ('biomedical Engineer', 'biomedical Engineer'), ('civil Engineer', 'civil Engineer'), ('chemical Engineer', 'chemical Engineer')], max_length=50)),
                ('student_id', models.CharField(max_length=14, primary_key=True, serialize=False)),
                ('bach', models.CharField(choices=[('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('5', '5')], max_length=50)),
                ('campus', models.CharField(choices=[('4killo', '4killo'), ('5killo', '5killo'), ('6killo', '6killo')], max_length=50)),
                ('section', models.CharField(max_length=2)),
                ('groups', models.ManyToManyField(blank=True, to='auth.Group')),
            ],
        ),
    ]
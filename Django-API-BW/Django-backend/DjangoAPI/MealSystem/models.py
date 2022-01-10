from django.db import models

# Create your models here.

class Student(models.Model):
    student_id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=20)
    department = models.CharField(max_length=20)
    year_of_study = models.IntegerField()
    campus = models.CharField(max_length=20)
    section = models.IntegerField()

class User(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=20)
    username = models.CharField(max_length=20)
    

class Schedule(models.Model):
    schedule_id = models.AutoField(primary_key=True);
    batch = models.IntegerField()
    department = models.CharField(max_length=20)
    section = models.IntegerField() 
    campus = models.CharField(max_length=20)
    MONDAY = 'MN'
    TUESDAY = 'TU'
    WEDNESDAY = 'WN'
    THURSDAY = 'TH'
    FRIDAY = 'FR'
    SATUARDAY = "ST"
    SUNDAY = "SN"
    DAY_CHOICES = [
        (MONDAY, 'Monday'),
        (TUESDAY, 'Tuesday'),
        (WEDNESDAY, 'Wednesday'),
        (THURSDAY, 'Thursday'),
        (FRIDAY, 'Friday'),
        (SATUARDAY, 'Satuarday'),
        (SUNDAY, 'Sunday'),
    ]
    DAY = models.CharField(
        max_length=2,
        choices=DAY_CHOICES,
        default=MONDAY,
    )
    time = models.TimeField()

class MealStatus(models.Model):
    student_id = models.ForeignKey(Student, on_delete=models.CASCADE, unique=True, primary_key=True)
    breakfast = models.BooleanField(default=False)
    lunch = models.BooleanField(default=False)
    dinner = models.BooleanField(default=False)
    



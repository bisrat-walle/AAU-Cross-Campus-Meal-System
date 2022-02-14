from django.db import models
from django.contrib.auth.models import Group



class Student(models.Model):
    year = (
        ("1","1"),
        ("2","2"),
        ("3","3"),
        ("4","4"),
        ("5","5"),
    )
    camp = (
        ("4killo","4killo"),
        ("5killo","5killo"),
        ("6killo","6killo"),
    )
    depart = (
        ("Software Engineering","Software Engineering"),
        ("Electrical Engineering","Electrical Engineering"),
        ("Mechanical Engineering","Eechanical Engineering"),
        ("Biomedical Engineering","Biomedical Engineering"),
        ("Civil Engineering","Civil Engineering"),
        ("Chemical Engineering","Chemical Engineering"),
    )
    name = models.CharField(max_length=50)
    department = models.CharField(max_length=50, choices = depart)
    student_id = models.CharField(max_length = 14, primary_key = True)
    bach = models.CharField(max_length = 50,choices = year)
    campus = models.CharField(max_length = 50, choices = camp )
    section = models.CharField(max_length =2)



class Schedule(models.Model):
    year = (
        ("1","1"),
        ("2","2"),
        ("3","3"),
        ("4","4"),
        ("5","5"),
    )
    depart = (
        ("Software Engineering","Software Engineering"),
        ("Electrical Engineering","Electrical Engineering"),
        ("Mechanical Engineering","Eechanical Engineering"),
        ("Biomedical Engineering","Biomedical Engineering"),
        ("Civil Engineering","Civil Engineering"),
        ("Chemical Engineering","Chemical Engineering"),
    )
    camp = (
        ("4killo","4killo"),
        ("5killo","5killo"),
        ("6killo","6killo"),
    )
    d = (
        ('monday','monday'),
        ('tuesday','tuesday'),
        ('wednesday','wednesday'),
        ('thursday','thursday'),
        ('friday','friday'),
        ('saturday','saturday'),
        ('sunday','sunday')
        )
    bach = models.CharField(max_length = 50,choices = year)
    department = models.CharField(max_length = 30, choices = depart)
    section = models.CharField(max_length = 2)
    campus = models.CharField(max_length = 50, choices = camp)
    day = models.CharField(max_length = 20, choices = d)
    startTime = models.TimeField()
    endTime = models.TimeField()



class MealStatus(models.Model):
    student_id = models.CharField(max_length = 20, primary_key = True)
    breakfast = models.BooleanField(blank=True)
    lunch = models.BooleanField(blank=True)
    dinner = models.BooleanField(blank=True)


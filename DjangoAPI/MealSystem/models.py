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
        ("software engineer","software engineer"),
        ("electrical Engineer","electrical Engineer"),
        ("mechanical Engineer","mechanical Engineer"),
        ("biomedical Engineer","biomedical Engineer"),
        ("civil Engineer","civil Engineer"),
        ("chemical Engineer","chemical Engineer"),
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
        ("software engineer","software engineer"),
        ("electrical Engineer","electrical Engineer"),
        ("mechanical Engineer","mechanical Engineer"),
        ("biomedical Engineer","biomedical Engineer"),
        ("civil Engineer","civil Engineer"),
        ("chemical Engineer","chemical Engineer"),
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
    schedule_id = models.CharField(max_length = 50, unique = True, null=True)
    startTime = models.TimeField()
    endTime = models.TimeField()



class MealStatus(models.Model):
    student_id = models.CharField(max_length = 20, primary_key = True)
    breakfast = models.BooleanField(blank=True)
    lunch = models.BooleanField(blank=True)
    dinner = models.BooleanField(blank=True)


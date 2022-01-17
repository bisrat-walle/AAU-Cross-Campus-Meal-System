from django.db import models
from django.contrib.auth.models import AbstractUser
from django.conf import settings

from .managers import UserManager

#from annoying.fields import AutoOneToOneField

class User(AbstractUser):
    name = models.CharField(max_length=255)
    email = models.CharField(max_length=255, null=True, blank=True)
    is_admin = models.BooleanField(default=False, blank=False, null=False)

    def __str__(self):
        return "{}".format(self.name)
    
    objects = UserManager()


# Create your models here.

class Student(models.Model):
    student_id = models.CharField(max_length=25, primary_key=True)
    name = models.CharField(max_length=255)
    department = models.CharField(max_length=255)
    year_of_study = models.IntegerField()
    campus = models.CharField(max_length=255)
    section = models.IntegerField()
    
    def save(self, force_insert=False, force_update=False):
        is_new = self.student_id is None
        super(Student, self).save(force_insert, force_update)
        if is_new:
            MealStatus.objects.create(thing=self)
    def __str__(self):
        return "{}".format(self.name)
    

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
    student_id = models.OneToOneField(Student, on_delete=models.CASCADE, unique=True, primary_key=True)
    breakfast = models.BooleanField(default=False)
    lunch = models.BooleanField(default=False)
    dinner = models.BooleanField(default=False)
    



from django.contrib import admin

from .models import Student, Schedule, MealStatus

# Register your models here.
admin.site.register(Student)
admin.site.register(Schedule)
admin.site.register(MealStatus)

from rest_framework.authtoken.admin import TokenAdmin
TokenAdmin.raw_id_fields = ['user']

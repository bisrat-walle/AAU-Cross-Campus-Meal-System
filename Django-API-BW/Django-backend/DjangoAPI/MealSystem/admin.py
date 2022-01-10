from django.contrib import admin

from .models import Student, User, Schedule, MealStatus

# Register your models here.
admin.site.register(Student)
admin.site.register(User)
admin.site.register(Schedule)
admin.site.register(MealStatus)


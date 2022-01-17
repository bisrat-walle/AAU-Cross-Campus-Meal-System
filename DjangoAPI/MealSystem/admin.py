from django.contrib import admin

from .models import Student, User, Schedule, MealStatus

class UserAdmin(admin.ModelAdmin):
    list_display = ('name', 'is_admin', 'username')

# Register your models here.
admin.site.register(Student)
admin.site.register(User, UserAdmin)
admin.site.register(Schedule)
admin.site.register(MealStatus)


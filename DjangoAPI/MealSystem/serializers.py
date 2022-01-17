from rest_framework import serializers
from MealSystem.models import Student, Schedule, MealStatus
from django.contrib.auth import get_user_model

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = get_user_model()
        fields = ('username', 'password', 'is_admin', 'name')

class StudentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Student
        fields = ('student_id', 'name', 'department', 'year_of_study', 'campus', 'section')
        
        
class ScheduleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Schedule
        fields = ('schedule_id', 'department', 'batch', 'campus', 'section', 'time', 'DAY')
        
        
class MealStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = MealStatus
        fields = ('student_id', 'breakfast', 'lunch', 'dinner')
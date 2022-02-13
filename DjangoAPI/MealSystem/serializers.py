from rest_framework import serializers
from django.contrib.auth.models import User
from MealSystem.models import Student, Schedule, MealStatus

class UserSerializer(serializers.ModelSerializer):
    password = serializers.CharField(
    style={'input_type': 'password'}
)
    class Meta:
        model = User
        fields = ("id", 'username', "password")

        

class StudentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Student
        fields = ('student_id', 'name', 'department', 'bach', 'campus', 'section')
        
        
class ScheduleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Schedule
        fields = ('schedule_id', 'department', 'bach', 'campus', 'section', 'startTime', 'endTime', 'day')
        
        
class MealStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = MealStatus
        fields = ('student_id', 'breakfast', 'lunch', 'dinner')
from rest_framework import serializers
from MealSystem.models import User, Student, Schedule, MealStatus

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ('id', 'name', 'username')

class StudentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Student
        fields = ('student_id', 'name', 'department', 'year_of_study', 'campus', 'section')
        
        
class ScheduleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Schedule
        fields = ('department', 'batch', 'campus', 'section', 'time', 'DAY')
        
        
class MealStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = MealStatus
        fields = ('student_id', 'breakfast', 'lunch', 'dinner')
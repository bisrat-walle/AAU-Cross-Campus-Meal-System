from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from django.http.response import JsonResponse
from django.contrib.auth import get_user_model

from MealSystem.models import Student, User, Schedule, MealStatus
from MealSystem.serializers import StudentSerializer, UserSerializer, ScheduleSerializer, MealStatusSerializer
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from .permissions import IsAdmin


# student api.

class StudentApi(APIView):
    permission_classes = [IsAuthenticated]
    permission_classes = [IsAdmin]
    def get(self, request):
        students = Student.objects.all()
        students_serializer = StudentSerializer(students, many=True)
        return JsonResponse(students_serializer.data, safe=False)
    def post(self):
        student_data = JSONParser().parse(request)
        student_serializer = StudentSerializer(data=student_data)
        if student_serializer.is_valid():
            student_serializer.save()
            return JsonResponse("Student Added Sucessfully!", safe=False)
        return JsonResponse("Failed to Add.")
    def put(self):
        student_data = JSONParser().parse(request)
        print(student_data)
        student = Student.objects.get(student_id=student_data["student_id"])
        student_serializer = StudentSerializer(student, data=student_data)
        if student_serializer.is_valid():
            student_serializer.save()
            return JsonResponse("Data Updated Sucessfully!", safe=False)
        return JsonResponse("Failed to Update.", safe=False)
    def delete(self, request, *args, **kwargs):
        student = Student.objects.get(student_id=student_id)
        student.delete()
        return JsonResponse("Data Deleted Sucessfully!", safe=False)
            

# user api.

class UserApi(APIView):
    permission_classes = [IsAuthenticated]
    def get(self, request):
        user = get_user_model().objects.all()
        user_serializer = UserSerializer(user, many=True)
        return JsonResponse(user_serializer.data, safe=False)
        
    def post(self, request):
        print(request.data)
        user_serializer = UserSerializer(data=request.data)
        if user_serializer.is_valid():
            user_serializer.save()
            return JsonResponse("User Added Sucessfully!", status=201, safe=False)
        return JsonResponse(user_serializer.errors, status=400, safe=False)
    def put(self, request):
        user_data = JSONParser().parse(request)
        user = get_user_model().objects.get(username=user_data["username"])
        user_serializer = UserSerializer(user, data=user_data)
        if user_serializer.is_valid():
            user_serializer.save()
            return JsonResponse("Data Updated Sucessfully!", safe=False)
        return JsonResponse(serializers.errors, safe=False)
    def delete(self, request, **kwargs):
        user = get_user_model().objects.get(username=kwargs["username"])
        user.delete()
        return JsonResponse("Data Deleted Sucessfully!", safe=False)

# schedule api.

class ScheduleApi(APIView):
    permission_classes = [IsAuthenticated]
    def get(request, schedule_id=-1):
        if schedule_id==-1:
            schedules = Schedule.objects.all()
            schedules_serializer = ScheduleSerializer(schedules, many=True)
            return JsonResponse(schedules_serializer.data, safe=False)
        else:
            schedule = Schedule.objects.get(schedule_id=schedule_id)
            if schedule is not None:
                schedule_serializer = ScheduleSerializer(schedule)
                return JsonResponse(schedule_serializer.data, safe=False)
            return JsonResponse("No such user", safe=False) 
    def post(request):
        schedule_data = JSONParser().parse(request)
        schedule_serializer = ScheduleSerializer(data=schedule_data)
        if schedule_serializer.is_valid():
            schedule_serializer.save()
            return JsonResponse("Schedule Added Sucessfully!", safe=False)
        return JsonResponse("Failed to Add.", safe=False)
    def put(request):
        schedule_data = JSONParser().parse(request)
        schedule = Schedule.objects.get(schedule_id=schedule_data["schedule_id"])
        schedule_serializer = ScheduleSerializer(schedule, data=schedule_data)
        if schedule_serializer.is_valid():
            schedule_serializer.save()
            return JsonResponse("Data Updated Sucessfully!", safe=False)
        return JsonResponse("Failed to Update.", safe=False)
    def delete(request, schedule_id):
        schedule = Schedule.objects.get(schedule_id=schedule_id)
        schedule.delete()
        return JsonResponse("Data Deleted Sucessfully!", safe=False)

@csrf_exempt
def mealStatusApi(request, id=-1):
    permission_classes = [IsAuthenticated]
    if request.method == "GET":
        if id==-1:
            status = MealStatus.objects.all()
            status_serializer = MealStatusSerializer(status, many=True)
            return JsonResponse(status_serializer.data, safe=False)
        else:
            status = MealStatus.objects.get(id=id)
            if status is not None:
                status_serializer = MealStatusSerializer(status)
                return JsonResponse(status_serializer.data, safe=False)
            return JsonResponse("No such status", safe=False) 
        
    elif request.method == "POST":
        status_data = JSONParser().parse(request)
        status_serializer = MealStatusSerializer(data=status_data)
        if status_serializer.is_valid():
            status_serializer.save()
            return JsonResponse("Status Added Sucessfully!", status=201, safe=False)
        return JsonResponse("Failed to Add.", status=400, safe=False)
    elif request.method == "PUT":
        status_data = JSONParser().parse(request)
        status = MealStatus.objects.get(id=status_data["id"])
        status_serializer = MealStatusSerializer(status, data=status_data)
        if status_serializer.is_valid():
            status_serializer.save()
            return JsonResponse("Status Updated Sucessfully!", safe=False)
        return JsonResponse("Failed to Update.", safe=False)
    elif request.method == "DELETE":
        status_data = JSONParser().parse(request)
        status = MealStatus.objects.get(student_id=status_data["student_id"])
        if status is not None:
            status.delete()
            return JsonResponse("Data Deleted Sucessfully!", safe=False)
        return JsonResponse("No such a data.", safe=False)
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from django.http.response import JsonResponse

from MealSystem.models import Student, User, Schedule, MealStatus
from MealSystem.serializers import StudentSerializer, UserSerializer, ScheduleSerializer, MealStatusSerializer

# student api.
@csrf_exempt
def studentApi(request, id=0):
    if request.method == "GET":
        students = Student.objects.all()
        students_serializer = StudentSerializer(students, many=True)
        return JsonResponse(students_serializer.data, safe=False)
    elif request.method == "POST":
        student_data = JSONParser().parse(request)
        student_serializer = StudentSerializer(data=student_data)
        if student_serializer.is_valid():
            student_serializer.save()
            return JsonResponse("Student Added Sucessfully!", safe=False)
        return JsonResponse("Failed to Add.")
    elif request.method == "PUT":
        student_data = JSONParser().parse(request)
        student = Student.objects.get(id=student_data["student_id"])
        student_serializer = StudentSerializer(student, data=student_data)
        if student_serializer.is_valid():
            student_serializer.save()
            return JsonResponse("Data Updated Sucessfully!", safe=False)
        return JsonResponse("Failed to Update.", safe=False)
    elif request.method == "DELETE":
        student_data = JSONParser().parse(request)
        student = Student.objects.get(id=student_data["student_id"], safe=False)
        if student is not None:
            student.delete()
            return JsonResponse("Data Deleted Sucessfully!", safe=False)
        return JsonResponse("No such a data.", safe=False)
            

# user api.
@csrf_exempt
def userApi(request, id=-1):
    if request.method == "GET":
        if id==-1:
            users = User.objects.all()
            users_serializer = UserSerializer(users, many=True)
            return JsonResponse(users_serializer.data, safe=False)
        else:
            user = User.objects.get(id=id)
            if user is not None:
                user_serializer = UserSerializer(user)
                return JsonResponse(users_serializer.data, safe=False)
            return JsonResponse("No such user", safe=False) 
        
    elif request.method == "POST":
        user_data = JSONParser().parse(request)
        user_serializer = UserSerializer(data=user_data)
        if user_serializer.is_valid():
            user_serializer.save()
            return JsonResponse("User Added Sucessfully!", status=201, safe=False)
        return JsonResponse("Failed to Add.", status=400, safe=False)
    elif request.method == "PUT":
        user_data = JSONParser().parse(request)
        user = User.objects.get(id=user_data["id"])
        user_serializer = UserSerializer(user, data=user_data)
        if user_serializer.is_valid():
            user_serializer.save()
            return JsonResponse("Data Updated Sucessfully!", safe=False)
        return JsonResponse("Failed to Update.", safe=False)
    elif request.method == "DELETE":
        print(id)
        user_data = JSONParser().parse(request)
        user = User.objects.get(id=user_data["id"])
        user.delete()
        return JsonResponse("Data Deleted Sucessfully!", safe=False)

# schedule api.
@csrf_exempt
def scheduleApi(request, id=0):
    if request.method == "GET":
        schedules = Schedule.objects.all()
        schedules_serializer = ScheduleSerializer(schedules, many=True)
        return JsonResponse(schedules_serializer.data, safe=False)
    elif request.method == "POST":
        schedule_data = JSONParser().parse(request)
        schedule_serializer = ScheduleSerializer(data=schedule_data)
        if schedule_serializer.is_valid():
            schedule_serializer.save()
            return JsonResponse("Schedule Added Sucessfully!", safe=False)
        return JsonResponse("Failed to Add.", safe=False)
    elif request.method == "PUT":
        schedule_data = JSONParser().parse(request)
        schedule = Schedule.objects.get(schedule_id=schedule_data["schedule_id"])
        schedule_serializer = ScheduleSerializer(schedule, data=schedule_data)
        if schedule_serializer.is_valid():
            schedule_serializer.save()
            return JsonResponse("Data Updated Sucessfully!", safe=False)
        return JsonResponse("Failed to Update.", safe=False)
    elif request.method == "DELETE":
        schedule_data = JSONParser().parse(request)
        schedule = Schedule.objects.get(schedule_id=schedule_data["schedule_id"])
        if schedule is not None:
            schedule.delete()
            return JsonResponse("Data Deleted Sucessfully!", safe=False)
        return JsonResponse("No such a data.", safe=False)

@csrf_exempt
def mealStatusApi(request, id=-1):
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
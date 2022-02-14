from django.shortcuts import render
from datetime import date
from datetime import datetime
import calendar
from rest_framework.response import Response
from django.db.models import Q
from django.contrib.auth.models import User
from rest_framework.permissions import IsAuthenticated
from django.contrib.auth.hashers import make_password, check_password

from rest_framework.decorators import api_view, authentication_classes, permission_classes

from django.contrib.auth.models import Group

from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from django.http.response import JsonResponse

from MealSystem.models import Student, Schedule, MealStatus
from MealSystem.serializers import StudentSerializer, UserSerializer, ScheduleSerializer, MealStatusSerializer
from MealSystem.decorators import admin_only, allowed_users


from django.shortcuts import render
import cv2 as cv
from pyzbar.pyzbar import decode

from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.views import TokenObtainPairView

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def profileApi(request, format=None):
    content = {
        'user': str(request.user),
        'role': str(request.user.groups.all()[0]),  
    }
    return Response(content)


@csrf_exempt
@api_view (['GET', 'POST', 'DELETE', 'PUT'])
@permission_classes([IsAuthenticated])
@admin_only
def studentApi(request, student_id=-1):
    if request.method == "GET":
        if student_id==-1:
            students = Student.objects.all()
            students_serializer = StudentSerializer(students, many=True)
            return JsonResponse(students_serializer.data, safe=False)
        else:
            try:
                student = User.objects.get(student_id=student_id)
                if student is not None:
                    student_serializer = StudentSerializer(student)
                    return JsonResponse(student_serializer.data, safe=False)
            except:
                return JsonResponse("No such student", safe=False) 
    elif request.method == "POST":
        student_data = JSONParser().parse(request)

        student_serializer = StudentSerializer(data=student_data)
        
        if student_serializer.is_valid():
            student_serializer.save()
            meal_data = {}
            meal_data["student_id"] = student_data["student_id"]
            meal_data["breakfast"] = False
            meal_data["lunch"] = False
            meal_data["dinner"] = False
            meal_serializer = MealStatusSerializer(data=meal_data)
            if meal_serializer.is_valid():
                meal_serializer.save()
                return JsonResponse("Student Added Sucessfully!", safe=False)
        return JsonResponse(student_serializer.errors, safe=False)
    elif request.method == "PUT":
        student_data = JSONParser().parse(request)
        try:
            student = Student.objects.get(student_id=student_data["student_id"])
            student_serializer = StudentSerializer(student, data=student_data)
            if student_serializer.is_valid():
                student_serializer.save()
                return JsonResponse("Data Updated Sucessfully!", safe=False)
            return JsonResponse("Unable to Update!", safe=False)
        except:
            return JsonResponse("The same ID is already in use", safe=False)
    elif request.method == "DELETE":
        try:
            student = Student.objects.get(student_id=student_id)
            student.delete()
            return JsonResponse("Data Deleted Sucessfully!", safe=False)
        except:
            return JsonResponse("Unable to Delete data!", safe=False)
@csrf_exempt
@api_view (['GET', 'POST', 'DELETE', 'PUT'])
@permission_classes([IsAuthenticated])
@admin_only
def userApi(request, id=-1):
    if request.method == "GET":
        if id==-1:
            users = User.objects.filter(groups__name = 'user')
            users_serializer = UserSerializer(users, many=True)
            return JsonResponse(users_serializer.data, safe=False)
        else:
            try:
                user = User.objects.get(id=id)
                if user is not None:
                    user_serializer = UserSerializer(user)
                    return JsonResponse(user_serializer.data, safe=False)
            except:
                return JsonResponse("No such user", safe=False) 
        
    elif request.method == "POST":
        user_data = JSONParser().parse(request)
        try :
            other_user = User.objects.get(username = user_data["username"], groups__name = "user")
            return JsonResponse("the user name already exist", safe=False)
        except:
            user_data["password"] = make_password(user_data["password"])
            user_serializer = UserSerializer(data=user_data)
            if user_serializer.is_valid():
                user_serializer.save()
                return JsonResponse("User Added Sucessfully!", status=201, safe=False)
            return JsonResponse("Failed to Add.", status=400, safe=False)
    elif request.method == "PUT":
        user_data = JSONParser().parse(request)
        try:
            user = User.objects.get(id=user_data["id"])
            user_data["password"] = make_password(user_data["password"])
            user_serializer = UserSerializer(user, data=user_data)
            if user_serializer.is_valid():
                user_serializer.save()
                return JsonResponse("Data Updated Sucessfully!", safe=False)
        except:
            return JsonResponse("Failed to Update.", safe=False)
    elif request.method == "DELETE":
        try:
            user = User.objects.get(id=id)
            if user is not None:
                user.delete()
                return JsonResponse("Data Deleted Sucessfully!", safe=False)
        except:
            return JsonResponse("No such user.", safe=False)

@csrf_exempt
@api_view (['GET', 'POST', 'DELETE', 'PUT'])
@permission_classes([IsAuthenticated])
@admin_only
def scheduleApi(request, id=-1):
    if request.method == "GET":
        if id==-1:
            schedules = Schedule.objects.all()
            schedules_serializer = ScheduleSerializer(schedules, many=True)
            return JsonResponse(schedules_serializer.data, safe=False)
        else:
            try:
                schedule = Schedule.objects.get(id=id)
                if schedule is not None:
                    schedule_serializer = ScheduleSerializer(schedule)
                    return JsonResponse(schedule_serializer.data, safe=False)
            except:
                return JsonResponse("No such schdule", safe=False) 
    elif request.method == "POST":
        schedule_data = JSONParser().parse(request)
        print(schedule_data)
        if tC(schedule_data["startTime"]) > tC(schedule_data["endTime"]):
            return JsonResponse("Failed your start time is grater than than end time.", safe=False)
        possible_clashs = Schedule.objects.filter(day=schedule_data["day"], section = schedule_data["section"],bach = schedule_data["bach"], department = schedule_data["department"])
        for p in possible_clashs:
            if tC(schedule_data["startTime"]) in range(tC(p.startTime), tC(p.endTime)) or tC(schedule_data["endTime"]) in range(tC(p.startTime), tC(p.endTime)) or (tC(schedule_data["startTime"]) <= tC(p.startTime) and tC(schedule_data["endTime"])>=tC(p.endTime)):
                return JsonResponse("schdule clash", safe = False)
        schedule_serializer = ScheduleSerializer(data=schedule_data)
        if schedule_serializer.is_valid():
            schedule_serializer.save()
            return JsonResponse("Schedule Added Sucessfully!", safe=False)
        return JsonResponse("Not valid input Change the schedule_id possibly the date.", safe=False)
    elif request.method == "PUT":
        schedule_data = JSONParser().parse(request)
        if schedule_data["startTime"] > schedule_data["endTime"]:
            return JsonResponse("Failed your start time is grater than than end time.", safe=False)
        possible_clashs = Schedule.objects.filter(day=schedule_data["day"], section = schedule_data["id"],bach = schedule_data["bach"], department = schedule_data["department"]).exclude(id = schedule_data["id"])
        for p in possible_clashs:
            print(schedule_data)
            if (tC(schedule_data["startTime"]) in range(tC(p.startTime), tC(p.endTime)) or tC(schedule_data["endTime"]) in range(tC(p.startTime), tC(p.endTime) or (tC(schedule_data["startTime"]) <= tC(p.startTime) and tC(schedule_data["endTime"])>=tC(p.endTime)))):
                return JsonResponse("schdule clash", safe = False)
        try:
            schedule = Schedule.objects.get(id=schedule_data["id"])
            schedule_serializer = ScheduleSerializer(schedule, data=schedule_data)
            if schedule_serializer.is_valid():
                schedule_serializer.save()
                return JsonResponse("Data Updated Sucessfully!", safe=False)
            return JsonResponse(schedules_serializer.errors, safe=False)
        except:
            return JsonResponse("Failed to Update.", safe=False)
    elif request.method == "DELETE":
        try:
            schedule = Schedule.objects.get(id=id)
            schedule.delete()
            return JsonResponse("Data Deleted Sucessfully!", safe=False)
        except:
            return JsonResponse("Data Deleted Failed!", safe=False)

def tC(time):
    val = ""
    for i in str(time):
        if i != ":":
            val += i
    return int(val)


@csrf_exempt
@api_view (['GET', 'PUT'])
@permission_classes([IsAuthenticated])
@admin_only
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
    elif request.method == "DELETE":
        students = Student.objects.first(campus = "6killo")
        now = datetime.now()
        current_time = now.strftime("%H")
        for student in students:
            meal_status = MealStatus.objects.filter(student_id=student.student_id)
            # so the user delete only in from night to lunch time
            if current_time in range(0, 8):
                try:
                    meal_status["lunch"] = False
                    meal_status["breakfast"] = False
                    meal_status["dinner"] = False
                    return JsonResponse("Data Deleted Sucessfully!")
                except:
                    return JsonResponse("No records found", safe=False)
            





@csrf_exempt
@api_view (['GET', 'POST', 'DELETE', 'PUT'])
@permission_classes([IsAuthenticated])
@allowed_users(["user"])
def scanned(request):
    id = decode_barcode()
    print(id)
    meal_status = {}
    meal_status = MealStatus.objects.filter(student_id = id)
    check = False
    try:
        student = Student.objects.get(student_id = id)
    except:
        return JsonResponse({
            "status":False,
            "reason":"barcode not detected"
        }, safe=False)
    now = datetime.now()
    current_time = now.strftime("%H")
    curr_date = date.today()
    dayOfWeek = calendar.day_name[curr_date.weekday()]
    today = dayOfWeek.lower()
    schedules = Schedule.objects.filter(day=today)
    
    for schedule in schedules:
        try:
            studentvalid = Student.objects.filter(student_id =student["student_id"], campus= "AAiT", section = schedule.section, bach = schedule.bach, department = schedule.department)
        except:
            return JsonResponse({
            "status":False,
            "reason":"you don't have schedule"
        })
    if current_time in range(5, 8):
        if meal_status["breakfast"] == True:
            return JsonResponse({
            "status":False,
            "reason":"you have already taken your breakfast"
        }, safe=False)
        meal_status["breakfast"] = True
        return JsonResponse({
            "status":True,
            "reason":"you can take your breakfast"
        }, safe=False)
    if current_time in range(8, 12):
        if meal_status["lunch"] == True:
            return JsonResponse({
            "status":False,
            "reason":"you have already taken your lunch"
        }, safe=False)
        meal_status["lunch"] = True
        meal_status["breakfast"] = True
        return JsonResponse({
            "status":True,
            "reason":"you can take your lunch"
        }, safe=False)
    elif current_time in range(13, 15):
        if meal_status["dinner"] == True:
            return JsonResponse({
            "status":False,
            "reason":"you have already taken your dinner"
        }, safe=False)
        meal_status["dinner"] = True
        return JsonResponse({
            "status":True,
            "reason":"you can take your dinner"
        }, safe=False)
    

def read_bar(filename):
    stat = 0
    img = cv.imread(filename)
    detectedBarcodes = decode(img)
    if not detectedBarcodes:
        stat = 1
        return JsonResponse("Barcode Not Detected or your barcode is blank/corrupted!", safe=False)
    else:
        for barcode in detectedBarcodes: 
            (x, y, w, h) = barcode.rect
            cv.rectangle(img, (x-10, y-10),
                          (x + w+10, y + h+10),
                          (255, 0, 0), 2)
             
            dec = ""
            for i in barcode.data:
                dec += str(chr(i))
            return dec

def decode_barcode():
    cam = cv.VideoCapture(0)
    #cam = cv.VideoCapture("http://10.6.194.82:4747/video")
    img_counter = 0
    
    while True:

        ret, frame = cam.read()
        if not ret:
            break
        cv.imshow("Scan", frame)
        k = cv.waitKey(1)
        if k%256 == 27:
            cv.destroyAllWindows()
            break
        elif k%256 == 13 or k%256==32:
            cv.imwrite("barcode__.jpg", frame) 
            cv.destroyAllWindows()
            break
        
    return read_bar("barcode__.jpg")
    
@csrf_exempt
@api_view (['GET'])
@permission_classes([IsAuthenticated])
@admin_only
# these function will count how many students will eat for breakfat, lunch and dinner
def studentCounter(request):
    curr_date = date.today()
    dayOfWeek = calendar.day_name[curr_date.weekday()]
    today = dayOfWeek.lower()
    now = datetime.now()
    current_time = now.strftime("%H")
    students_at_breakfast = 0
    students_at_lunch = 0
    student_at_dinner = 0
    schedules = Schedule.objects.filter(day = today) # breakfast eater
    for schedule in schedules:
        students = Student.objects.filter(section = schedule.section, bach = schedule.bach, department = schedule.department)
        for student in students:
            if tC(schedule.endTime)//10000  in range(5, 9):
                students_at_lunch += 1
            elif tC(schedule.endTime)//10000 in range(11, 12):
                student_at_dinner += 1
            elif tC(schedule.endTime)//10000 in range(1, 3):
                students_at_breakfast
    info ="todays meal report: for breakfast=",str(students_at_breakfast)," for lunch=",str(students_at_lunch), "for dinner=",str(student_at_dinner)
    return JsonResponse(info, safe=False)
    
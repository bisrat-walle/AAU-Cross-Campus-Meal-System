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
import cv2 #as cv
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
                return JsonResponse("No Such Student!", safe=False) 
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
            return JsonResponse("Unable To Update!", safe=False)
        except:
            return JsonResponse("The Same ID Is Already In Use!", safe=False)
    elif request.method == "DELETE":
        try:
            student = Student.objects.get(student_id=student_id)
            meal_status = MealStatus.objects.get(student_id = student_id)
            student.delete()
            meal_status.delete()
            return JsonResponse("Data Deleted Sucessfully!", safe=False)
        except:
            return JsonResponse("Unable To Delete Data!", safe=False)
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
                return JsonResponse("No Such User!", safe=False) 
        
    elif request.method == "POST":
        user_data = JSONParser().parse(request)
        try :
            other_user = User.objects.get(username = user_data["username"], groups__name = "user")
            return JsonResponse("The User Name Already Exist!", safe=False)
        except:
            user_data["password"] = make_password(user_data["password"])
            user_serializer = UserSerializer(data=user_data)
            if user_serializer.is_valid():
                user_serializer.save()
                return JsonResponse("User Added Sucessfully!", status=201, safe=False)
            return JsonResponse("Failed To Add!", status=400, safe=False)
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
            return JsonResponse("Failed To Update!", safe=False)
    elif request.method == "DELETE":
        try:
            user = User.objects.get(id=id)
            if user is not None:
                user.delete()
                return JsonResponse("Data Deleted Sucessfully!", safe=False)
        except:
            return JsonResponse("No Such User!", safe=False)

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
                return JsonResponse("No Such Schdule!", safe=False) 
    elif request.method == "POST":
        schedule_data = JSONParser().parse(request)
        if tC(schedule_data["startTime"]) > tC(schedule_data["endTime"]):
            return JsonResponse("Failed Your Start Time Is Grater Than End Time!", safe=False)
        possible_clashs = Schedule.objects.filter(day=schedule_data["day"], section = schedule_data["section"],bach = schedule_data["bach"], department = schedule_data["department"])
        for p in possible_clashs:
            if tC(schedule_data["startTime"]) in range(tC(p.startTime), tC(p.endTime)) or tC(schedule_data["endTime"]) in range(tC(p.startTime), tC(p.endTime)) or (tC(schedule_data["startTime"]) <= tC(p.startTime) and tC(schedule_data["endTime"])>=tC(p.endTime)):
                return JsonResponse("schdule clash", safe = False)
        schedule_serializer = ScheduleSerializer(data=schedule_data)
        if schedule_serializer.is_valid():
            schedule_serializer.save()
            return JsonResponse("Schedule Added Sucessfully!", safe=False)
        return JsonResponse("Not Valid Input Change The Td Or Possibly The Date!", safe=False)
    elif request.method == "PUT":
        schedule_data = JSONParser().parse(request)
        if schedule_data["startTime"] > schedule_data["endTime"]:
            return JsonResponse("Failed Your Start Time Is Grater Than End Time!", safe=False)
        possible_clashs = Schedule.objects.filter(day=schedule_data["day"], section = schedule_data["section"],bach = schedule_data["bach"], department = schedule_data["department"]).exclude(id = schedule_data["id"])
        for p in possible_clashs:
            if (tC(schedule_data["startTime"]) in range(tC(p.startTime), tC(p.endTime)) or tC(schedule_data["endTime"]) in range(tC(p.startTime), tC(p.endTime) or (tC(schedule_data["startTime"]) <= tC(p.startTime) and tC(schedule_data["endTime"])>=tC(p.endTime)))):
                return JsonResponse("Schdule clash!", safe = False)
        try:
            schedule = Schedule.objects.get(id=schedule_data["id"])
            schedule_serializer = ScheduleSerializer(schedule, data=schedule_data)
            if schedule_serializer.is_valid():
                schedule_serializer.save()
                return JsonResponse("Data Updated Sucessfully!", safe=False)
        except:
            return JsonResponse("Failed To Update!", safe=False)
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



def get_local(utc):
    return int(utc) - 6


def filterSchedules(startTime, endTime, current_time):
    if abs(int(endTime)-current_time-6) < 1 or abs(int(startTime) - current_time-6) :
        return True
    return False

@csrf_exempt
@api_view (['GET', 'POST', 'DELETE', 'PUT'])
@permission_classes([IsAuthenticated])
@allowed_users(["user"])
def scanned(request):

    now = datetime.now()
    current_time = now.strftime("%H")
    current_time = get_local(current_time)
    
    curr_date = date.today()
    dayOfWeek = calendar.day_name[curr_date.weekday()]
    today = dayOfWeek.lower()


    id = decode_barcode()
    
    try:
        student = Student.objects.get(student_id = id)
        meal_status = MealStatus.objects.get(student_id = id)
        if meal_status.day != today:
            meal_status.breakfast = False
            meal_status.lunch = False
            meal_status.dinner = False
            meal_status.day = today
            meal_status.save()
    except:
        return JsonResponse({
            "status":False,
            "reason":"Barcode Not Detected!"
        }, safe=False)

    
    
    if student.campus== request.user.last_name:
        if current_time in range(0, 24):
            if meal_status.breakfast == True:
                return JsonResponse({
                "status":False,
                "reason":"You Have Already Taken Your Breakfast!"
            }, safe=False)
            meal_status.breakfast = True
            meal_status.save()
            return JsonResponse({
                "status":True,
                "reason":"You Can Take Your Breakfast!"
            }, safe=False)
        elif current_time in range(0, 24):
            if meal_status.lunch == True:
                return JsonResponse({
                "status":False,
                "reason":"You Have Already Taken Your Lunch!"
            }, safe=False)
            meal_status.lunch = True
            meal_status.save()
            return JsonResponse({
                "status":True,
                "reason":"You Can Take Your Lunch!"
            }, safe=False)
        elif current_time in range(0, 24):
            if meal_status.dinner == True:
                return JsonResponse({
                "status":False,
                "reason":"You Have Already Taken Your Dinner!"
            }, safe=False)
            meal_status.dinner = True
            meal_status.save()
            return JsonResponse({
                "status":True,
                "reason":"You Can Take Your Dinner!"
            }, safe=False)
       
        return JsonResponse(
            {
                "status":False,
                "reason":"The Time Not For Meal!"
            }, safe=False
        )
    if (request.user.last_name != "5killo") and (request.user.last_name != student.campus):
        return JsonResponse(
        {
            "status":False,
            "reason":"You Are Not In Your Campus!"
        }, safe=False
    )
    if request.user.last_name=="5killo":
        schedules = Schedule.objects.filter(day=today, department = student.department, bach = student.bach, section = student.section)
        
        has_schedule = False
        for schedule in schedules:
            startTime = schedule.startTime.strftime("%H")
            endTime = schedule.endTime.strftime("%H")
            if filterSchedules(startTime,endTime, current_time):
                has_schedule = True
                break
    
        if not has_schedule:
            return JsonResponse(
            {
                "status":False,
                "reason":"You Have No Schedule!"
            }, safe=False
    )
        

        if current_time in range(0, 24):
            if meal_status.breakfast == True:
                return JsonResponse({
                "status":False,
                "reason":"You Have Already Taken Your Breakfast!"
            }, safe=False)
            meal_status.breakfast = True
            meal_status.save()
            return JsonResponse({
                "status":True,
                "reason":"You Can Take Your Breakfast!"
            }, safe=False)
        if current_time in range(0, 24):
            if meal_status.lunch == True:
                return JsonResponse({
                "status":False,
                "reason":"You Have Already Taken Your Lunch!"
            }, safe=False)
            meal_status.lunch = True
            meal_status.save()
            return JsonResponse({
                "status":True,
                "reason":"You Can Take Your Lunch!"
            }, safe=False)
        if current_time in range(0, 24):
            if meal_status.dinner == True:
                return JsonResponse({
                "status":False,
                "reason":"You Have Already Taken Your Dinner!"
            }, safe=False)
            meal_status.dinner = True
            meal_status.save()
            return JsonResponse({
                "status":True,
                "reason":"You Can Take Your Dinner!"
            }, safe=False)

    return JsonResponse(
            {
                "status":False,
                "reason":"The Time Not For Meal!"
            }, safe=False
        )
def decode_barcode():
    cap = cv2.VideoCapture("http://192.168.137.173:4747/video")
    cap.set(3, 640)
    cap.set(4,480)

    while True:
        
        succes, img = cap.read()
        for barcode in decode(img):
            myData = barcode.data.decode('utf-8')
            cv2.destroyAllWindows()
            return myData
        cv2.imshow("Result", img)
        cv2.waitKey(1)
    

# def read_bar(filename):
#     stat = 0
#     img = cv.imread(filename)
#     detectedBarcodes = decode(img)
#     if not detectedBarcodes:
#         stat = 1
#         return JsonResponse("Barcode Not Detected or your barcode is blank/corrupted!", safe=False)
#     else:
#         for barcode in detectedBarcodes: 
#             (x, y, w, h) = barcode.rect
#             cv.rectangle(img, (x-10, y-10),
#                           (x + w+10, y + h+10),
#                           (255, 0, 0), 2)
             
#             dec = ""
#             for i in barcode.data:
#                 dec += str(chr(i))
#             return dec

# def decode_barcode():
#     # cam = cv.VideoCapture(0)
#     cam = cv.VideoCapture("http://192.168.137.173:4747/video")
#     img_counter = 0
    
    # while True:

    #     ret, frame = cam.read()
    #     if not ret:
    #         break
    #     cv.imshow("Scan", frame)
    #     k = cv.waitKey(1)
    #     if k%256 == 27:
    #         cv.destroyAllWindows()
    #         break
    #     elif k%256 == 13 or k%256==32:
    #         cv.imwrite("barcode__.jpg", frame) 
    #         cv.destroyAllWindows()
    #         break
        
    # # return read_bar("bisrat_id.jpg")
    # return read_bar("barcode__.jpg")
    
@csrf_exempt
@api_view (['GET'])
@permission_classes([IsAuthenticated])
@allowed_users(allowed_roles=["user"])
def studentCounter(request):
    curr_date = date.today()
    dayOfWeek = calendar.day_name[curr_date.weekday()]
    today = dayOfWeek.lower()
    now = datetime.now()
    current_time = now.strftime("%H")
    students_at_breakfast = 0
    students_at_lunch = 0
    student_at_dinner = 0
    total_student = Student.objects.all().count()
    schedules = Schedule.objects.filter(day = today)
    for schedule in schedules:
        startTime = int(schedule.startTime.strftime("%H"))
        endTime = int(schedule.endTime.strftime("%H"))
        students = Student.objects.filter(section = schedule.section, bach = schedule.bach, department = schedule.department)
        for student in students:
            if startTime  in range(0, 24) or endTime in range(0, 24):
                students_at_breakfast += 1
            elif startTime  in range(5, 7) or endTime in range(5, 7):
                students_at_lunch += 1
            elif startTime  in range(10, 12) or endTime in range(10, 12):
                student_at_dinner += 1
    info ={
        "breakfast":students_at_breakfast,
        "lunch":students_at_lunch,
        "dinner":student_at_dinner,
        "allStudents":total_student,
        "day":today
    }
    return JsonResponse(info, safe=False)
    
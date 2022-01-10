from django.conf.urls import url
from MealSystem import views


urlpatterns = [
    url(r'student/$', views.studentApi),
    url(r'student/[a-zA-Z0-9]+$', views.studentApi),
    url(r'user/$', views.userApi),
    url(r'user/[a-zA-Z0-9]+$', views.userApi),
    url(r'schedule/$', views.scheduleApi),
    url(r'schedule/[a-zA-Z0-9]+$', views.scheduleApi),
    url(r'meal-status/$', views.mealStatusApi),
    url(r'meal-status/[a-zA-Z0-9]+$', views.mealStatusApi)
]
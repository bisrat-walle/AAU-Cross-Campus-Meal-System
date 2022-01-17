from django.conf.urls import url
from MealSystem import views

from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
    TokenVerifyView, 
)


urlpatterns = [
    url(r'^student/$', views.StudentApi.as_view()),
    url(r'^student/(?P<student_id>[/a-zA-Z0-9]+)$', views.StudentApi.as_view()),
    url(r'^user/$', views.UserApi.as_view()),
    url(r'^user/(?P<username>[a-zA-Z0-9]+)$', views.UserApi.as_view()),
    url(r'^schedule/$', views.ScheduleApi.as_view()),
    url(r'^schedule/(?P<schedule_id>[a-zA-Z0-9]+)$', views.ScheduleApi.as_view()),
#    url(r'^meal-status/$', views.mealStatusApi),
#    url(r'^meal-status/(?P<student_id>[a-zA-Z0-9]+)$', views.mealStatusApi),
    url(r'^api/login/$', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    url(r'^api/token/refresh/$', TokenRefreshView.as_view(), name='token_refresh'),
    url(r'^api/token/verify/$', TokenVerifyView.as_view(), name='token_verify'),
    
]
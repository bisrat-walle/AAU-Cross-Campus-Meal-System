from django.urls import re_path
from MealSystem import views
from rest_framework.urlpatterns import format_suffix_patterns
from rest_framework.authtoken.views import ObtainAuthToken
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)


urlpatterns = [
    re_path(r'^profile/$', views.profileApi, name = 'profile'),
    re_path(r'^student/$', views.studentApi),
    re_path(r'^student/(?P<student_id>[/a-zA-Z0-9]+)$', views.studentApi),
    re_path(r'^user/$', views.userApi),
    re_path(r'^user/(?P<id>[a-zA-Z0-9]+)$', views.userApi),
    re_path(r'^scan/', views.scanned),
    re_path(r'^schedule/$', views.scheduleApi),
    re_path(r'^schedule/(?P<id>[a-zA-Z0-9]+)$', views.scheduleApi),
    re_path(r'^students/expected/$', views.studentCounter),
    re_path('api/login/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    re_path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),

]

urlpatterns = format_suffix_patterns(urlpatterns)
from rest_framework import permissions


class IsAdmin(permissions.BasePermission):

    edit_methods = ("PUT", "PATCH")

    def has_permission(self, request, view):
        if request.user["is_admin"] == True:
            return True
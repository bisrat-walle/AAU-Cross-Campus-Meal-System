from lib2to3.pytree import Base
from tkinter import E
from django.db import models
from django.contrib.auth import get_user_model
from django.contrib.auth.models import BaseUserManager

class UserManager(BaseUserManager):
    def create_user(self, username, password, name, is_admin, email=None, **extra_fields):
        """
        Create and save a User with the given email and password.
        """
        if not username:
            raise ValueError(_('The Username must be set'))
        user = self.model(username=username, name=name, email=email, **extra_fields)
        user.set_password(password)
        user.is_admin = is_admin
        user.save()
        return user

    def create_superuser(self, username, password, is_admin=True, first_name="admin", last_name="admin", **extra_fields):
        """
        Create and save a SuperUser with the given email and password.
        """
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        extra_fields.setdefault('is_active', True)

        if extra_fields.get('is_staff') is not True:
            raise ValueError(_('Superuser must have is_staff=True.'))
        if extra_fields.get('is_superuser') is not True:
            raise ValueError(_('Superuser must have is_superuser=True.'))
        return self.create_user(username=username, password=password, name="ADMIN", is_admin=is_admin, first_name=first_name, last_name=last_name, **extra_fields)
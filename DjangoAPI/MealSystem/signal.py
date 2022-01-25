from django.db.models.signals import post_save
from django.contrib.auth.models import User
from django.contrib.auth.models import Group
from django.dispatch import receiver
from .models import*


@receiver(post_save, sender=User)
def userCreate(sender, instance, created, **kwargs):
    if created:
        try:
            group = Group.objects.get(name='user')
        except:
            group = Group(name="user")
            group.save()
            group = Group.objects.get(name="user")

        instance.groups.add(group)
        

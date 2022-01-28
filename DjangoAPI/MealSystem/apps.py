from django.apps import AppConfig


class MealsystemConfig(AppConfig):
    name = 'MealSystem'
    
    def ready(self):
    	import MealSystem.signal
    

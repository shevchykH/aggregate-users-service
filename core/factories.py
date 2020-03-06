import factory
from django.conf import settings

from core.models import Event


class UserFactory(factory.DjangoModelFactory):
    class Meta:
        model = settings.AUTH_USER_MODEL
        django_get_or_create = ('email',)
    email = factory.Sequence(lambda n: f'user{n}@gmail.com')


class EventFactory(factory.DjangoModelFactory):
    class Meta:
        model = Event
    name = factory.Sequence(lambda n: f'Custom event {n}')

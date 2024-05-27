from uuid import uuid4

from django.db import models
from base.models import ApplicationBaseModel


class User(ApplicationBaseModel):
    user_id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    is_active = models.BooleanField(default=True)
    is_deleted = models.BooleanField(default=False)
    name = models.CharField(max_length=150, null=True)
    email = models.EmailField(null=True)
    password = models.CharField()

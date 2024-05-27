from dirtyfields import DirtyFieldsMixin
from django.db import models


class ApplicationBaseModel(models.Model):
    modified_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        abstract = True

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        if isinstance(self, DirtyFieldsMixin) and not self._state.adding and not self.get_deferred_fields():
            check_relationship = False
            if hasattr(self, 'check_relationship'):
                check_relationship = self.check_relationship
            dirty_fields = self.get_dirty_fields(check_relationship=check_relationship)
            update_fields = dirty_fields.keys()

        return super().save(force_insert, force_update, using, update_fields)

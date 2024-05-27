from rest_framework import serializers


class CreateOnlyFieldValidator:
    requires_context = True

    def __call__(self, value, field):
        instance = field.parent.instance
        if instance is not None:
            initial = field.get_attribute(instance)
            if initial != value:
                raise serializers.ValidationError(
                    'Updation of {field_name} is not allowed. It is a create-only field'.format(
                        field_name=field.field_name))

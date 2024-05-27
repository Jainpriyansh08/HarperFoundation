import os

from django.core.exceptions import ImproperlyConfigured


def get_env_value(name: str) -> str:
    try:
        return os.environ[name]
    except KeyError:
        error_msg = 'Set the {} environment variable'.format(name)
        raise ImproperlyConfigured(error_msg)
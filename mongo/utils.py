import secrets
import string
from datetime import datetime, date

import pytz
import simplejson as json

IST = pytz.timezone('Asia/Calcutta')


def json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime) or isinstance(obj, date):
        serial = obj.isoformat()
        return serial
    if isinstance(obj, object):
        return str(obj)
    json_encoder = json.JSONEncoder()
    return json_encoder.default(obj)


def get_random_string(length, upper_case=True, lower_case=True, number=True):
    char_range = ""
    if upper_case:
        char_range += string.ascii_uppercase
    if lower_case:
        char_range += string.ascii_lowercase
    if number:
        char_range += string.digits
    return "".join(secrets.choice(char_range) for _ in range(length))

def deslugify(slug):
    words = slug.split('-')
    capitalized_words = [word.capitalize() for word in words]
    return ' '.join(capitalized_words)

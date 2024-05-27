import jwt
import time

from django.utils import timezone
from datetime import timedelta


def decode_jwt(encoded_jwt, secret, audience="BETTERHALF_ADMIN"):
    """"""
    return jwt.decode(encoded_jwt, secret, algorithms=["HS256"], audience=audience)


def encode_jwt(payload, secret, exp, token_type, aud="BETTERHALF_ADMIN", iss="BETTERHALF"):
    """"""
    payload.update({"iss": iss,
                    "iat": int(time.time()),
                    "exp": exp,
                    "aud": aud,
                    "tokenType": token_type
                    })
    return jwt.encode(payload, secret, algorithm="HS256")


def generate_access_refresh_token(payload, secret, aud="BETTERHALF_ADMIN", iss="BETTERHALF", expiry_time_in_minutes=30):
    """"""
    access_token_exp = timezone.now() + timedelta(minutes=expiry_time_in_minutes)
    refresh_token_time = timezone.now() + timedelta(days=45)
    access_token = encode_jwt(payload, secret, time.mktime(access_token_exp.timetuple()), "ACCESS", aud, iss)
    refresh_token = encode_jwt(payload, secret, time.mktime(refresh_token_time.timetuple()), "REFRESH", aud, iss)
    return access_token, refresh_token, time.mktime(access_token_exp.timetuple())

import atexit
import hashlib

from pymongo import MongoClient
from bson import Decimal128
from decimal import Decimal
from datetime import datetime, date, timedelta
from bson.objectid import ObjectId


def get_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()


class MongoDBClient:
    CONNECTION_CACHE = {}

    def __init__(self, *, connection_string=None, use_cache=True, connection=None, idle_timeout_in_seconds=None,
                 **kwargs):
        if connection is not None:
            self._client = connection
        else:
            if use_cache:
                self._client = self._cache_mongo_connection(connection_string, idle_timeout_in_seconds)
            else:
                self._client = MongoClient(connection_string)
        self.use_cache = use_cache
        self._default_db = self._client.get_default_database()

    def get_default_database(self):
        return self._default_db

    @property
    def client(self):
        return self._client

    @classmethod
    def python_to_bson(cls, data):
        if isinstance(data, dict):
            for key, value in data.items():
                data[key] = cls.python_to_bson(value)
        elif isinstance(data, list):
            temp = []
            for i in data:
                temp.append(cls.python_to_bson(i))
            data = temp
        elif isinstance(data, datetime):
            if data.tzinfo is None:
                raise Exception("Timezone unaware datetime object is passed to Mongo")
        elif isinstance(data, date):
            data = datetime(year=data.year, month=data.month, day=data.day)
        elif isinstance(data, Decimal):
            data = Decimal128(data)
        return data

    @classmethod
    def bson_to_python(cls, data):
        if isinstance(data, dict):
            for key, value in data.items():
                data[key] = cls.bson_to_python(value)
        elif isinstance(data, list):
            temp = []
            for i in data:
                temp.append(cls.bson_to_python(i))
            data = temp
        elif isinstance(data, Decimal128):
            data = data.to_decimal()
        return data

    @classmethod
    def clean_cached_connection(cls):
        for conn_hash, db_conn in cls.CONNECTION_CACHE.items():
            db_conn.get('connection').close()

    @staticmethod
    def get_object_id(object_id):
        return ObjectId(object_id)

    @classmethod
    def flatten(cls, data, prefix=''):
        temp = {}
        for key, value in data.items():
            if isinstance(value, dict):
                temp.update(cls.flatten(value, f"{prefix}{key}."))
            else:
                temp[f"{prefix}{key}"] = value
        return temp

    @classmethod
    def _cache_mongo_connection(cls, connection_string, idle_timeout_in_seconds):
        if idle_timeout_in_seconds is None:
            idle_timeout_in_seconds = 10 * 60
        conn_hash = get_hash(connection_string)
        cached_connection = cls.CONNECTION_CACHE.get(conn_hash)
        if cached_connection is None or cached_connection.get('expiresAt') < datetime.now():
            if cached_connection is not None:
                cached_connection.get('connection').close()
            connection = MongoClient(connection_string, maxIdleTimeMS=idle_timeout_in_seconds * 100)
            cls.CONNECTION_CACHE[conn_hash] = {
                "connection": connection
                , "expiresAt": datetime.now() + timedelta(seconds=idle_timeout_in_seconds - 5)
            }
            print("NEW_CONNECTION")
        else:
            connection = cached_connection.get('connection')
            print("CONNECTION_REUSED")
        return connection

    @classmethod
    def get_client(cls, connection_string, use_cache=True, idle_timeout_in_seconds=None, **kwargs):
        if use_cache:
            connection = cls._cache_mongo_connection(connection_string, idle_timeout_in_seconds)
        else:
            connection = MongoClient(connection_string)
        return cls(use_cache=use_cache, connection=connection, **kwargs)

    def __del__(self):
        self._default_db = None
        if self.use_cache is False and self._client:
            self._client.close()
        self._client = None


atexit.register(MongoDBClient.clean_cached_connection)

from HarperFoundation.utils.get_env import get_env_value
from mongo import MongoDBClient

print("CONNECTION_STR", get_env_value("CONNECTION_STRING"))

MONGO_CONNECTION = MongoDBClient.get_client(
    connection_string=get_env_value("CONNECTION_STRING")
)

MONGO_CONNECTION_INHOUSE_SERVICES = MongoDBClient.get_client(
    connection_string=get_env_value("CONNECTION_STRING_IN_HOUSE")
)
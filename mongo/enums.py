from enum import Enum


class MongoOperations(Enum):
    INSERT_ONE = "insert_one"
    INSERT_MANY = "insert_many"
    UPDATE_ONE = "update_one"
    UPDATE_MANY = "update_many"
    DELETE_ONE = "delete_one"
    DELETE_MANY = "delete_many"
    FIND_ONE_AND_UPDATE = "find_one_and_update"
    AGGREGATE = 'aggregate'
    FIND_ONE = "find_one"
    FIND = 'find'

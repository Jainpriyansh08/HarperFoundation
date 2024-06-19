from dataclasses import dataclass

import pymongo
from pymongo.collection import Collection

from mongo.mongo import MongoOperations


@dataclass
class MongoOperationDataClass:
    collection: Collection
    operation_name: MongoOperations
    operation_params: dict | None = None
    aggregation_query: list | None = None
    bulk_query: list | None = None

    def execute(self, session=None):
        """"""
        if self.operation_name == MongoOperations.AGGREGATE:
            return getattr(self.collection, self.operation_name.value)(self.aggregation_query, session=session)
        if self.operation_name in MongoOperations:
            if self.bulk_query:
                operation_name_in_pascal = ''.join(x.title() for x in self.operation_name.value.split('_'))
                return self.collection.bulk_write(
                    [getattr(pymongo, operation_name_in_pascal)(**operation_param)
                     for operation_param in self.bulk_query], session=session)
            return getattr(self.collection, self.operation_name.value)(**self.operation_params, session=session)
        else:
            raise ValueError(f"Unsupported operation: {self.operation_name}")

    @classmethod
    def get_client(cls, **kwargs):
        """"""
        return cls(**kwargs)

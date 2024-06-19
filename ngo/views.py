from rest_framework.decorators import action

from base.views import ApplicationBaseViewSet
from mongo import MongoCollection
from utils import MONGO_CONNECTION


# Create your views here.


class NGOResourceViewSet(ApplicationBaseViewSet):

    def __init__(self, name, city, **kwargs):
        """"""
        super().__init__(**kwargs)
        self.config = MongoCollection(connection=MONGO_CONNECTION, collection_name="config")

    @action(methods=["GET"], detail=False, url_path="")
    def get_ngo_details(self, request, *args, **kwargs):
        """"""

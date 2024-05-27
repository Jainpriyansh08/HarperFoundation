def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


MongoKeyMap = {
    "id": "id",
    "city": "address.city",
    "ratings": "gmapRatings"
}


class MongoFilter:

    def get_val(self, value):
        if type(value) == list:
            return {"$in": value}
        if isfloat(value):
            return {"$gt": float(value)}
        return value

    def get_filter_dict(self) -> dict:
        """"""
        # TODO: need to handle errors/edge cases
        # TODO: skip some filter like page, skip
        _filter = {}
        for key, value in self.request.query_params.items():
            valueArray = [x for x in value.split(',')]
            try:
                _filter[MongoKeyMap[key]] = self.get_val(valueArray if len(valueArray) > 1 else value)
            except KeyError:
                pass
        print("_filter", _filter)
        return _filter

# Valid Sample Query
# {"address.city": {"$in": ["Bengaluru","Sathanur"]}, "gmapRatings":{"$gt" :4}, "gmapPlaceId": {"$in": ["ChIJdaPRwmgZrjsRutwI3WXRMf8"]}, "isActive":False}

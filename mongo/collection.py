import os
from datetime import datetime

from bson.codec_options import CodecOptions

from filters.constant import sort_key_map
from .mongo import MongoDBClient
from .utils import IST

BUCKET = os.environ.get("BUCKET_NAME", "bh_dev_bucket")
GCP_CDN_URL = os.environ.get("GCP_CDN_URL", "https://weddingimage.betterhalf.ai")
URL_PREFIX = GCP_CDN_URL + "/"


class QueryIterator:
    def __init__(self, cursor, convertor_function, size=None):
        self.cursor = cursor
        self.convertor_function = convertor_function
        self.size = size

    def __iter__(self):
        return self

    def __next__(self):
        data = next(self.cursor)
        data = self.convertor_function(data)
        self.update_media_url(data)
        return data

    def update_media_url(self, data):
        """"""
        try:
            data["media"] = data["media"][:4]
        except:
            pass


class MongoCollection(MongoDBClient):
    # PerPageLimit = 0  # No limit
    MaxPerPageLimit = 30

    # Page = 1

    def __init__(self, *, collection_name, **kwargs):
        super(MongoCollection, self).__init__(**kwargs)
        self.collection = self._default_db.get_collection(collection_name).with_options(
            CodecOptions(tz_aware=True, tzinfo=IST)
        )

    def insert_one(self, *, document, action_by=None, **kwargs):
        if not document:
            return
        audit_stamp = datetime.now(IST)
        document["createdAt"] = audit_stamp
        document["updatedAt"] = audit_stamp
        if action_by is not None:
            document["createdBy"] = action_by
            document["updatedBy"] = action_by
        kwargs["document"] = self.python_to_bson(document)
        return self.collection.insert_one(**kwargs)

    def insert_many(self, *, documents, action_by=None, **kwargs):
        audit_stamp = datetime.now(IST)
        document_list = []
        for doc in documents:
            doc["createdAt"] = audit_stamp
            doc["updatedAt"] = audit_stamp
            if action_by is not None:
                doc["createdBy"] = action_by
                doc["updatedBy"] = action_by
            document_list.append(doc)
        kwargs["documents"] = self.python_to_bson(document_list)
        return self.collection.insert_many(**kwargs)

    def find(self, *args, **kwargs):
        return QueryIterator(self.collection.find(*args, **kwargs), self.bson_to_python)

    def find_one(self, *args, **kwargs):
        result = self.collection.find_one(*args, **kwargs)
        if result is not None:
            result = self.bson_to_python(result)
        return result

    def update_one(self, *, update, unset=None, action_by=None, push=None, **kwargs):
        update["updatedAt"] = datetime.now(IST)
        if action_by is not None:
            update["updatedBy"] = action_by
        update = self.python_to_bson(update)
        kwargs["update"] = {"$set": update}
        if unset:
            kwargs["update"] = {**kwargs['update'], "$unset": unset}
        if push:
            kwargs["update"]["$push"] = push
        return self.collection.update_one(**kwargs)

    def update_many(self, *, update, set_on_insert=None, action_by=None, **kwargs):
        update["updatedAt"] = datetime.now(IST)
        if action_by is not None:
            update["updatedBy"] = action_by
        update = self.python_to_bson(update)
        kwargs["update"] = {"$set": update}
        if set_on_insert:
            kwargs["update"].update({"$set"})
        return self.collection.update_many(**kwargs)

    def __del__(self):
        self.collection = None
        super().__del__()


class HelperCollection(MongoCollection):
    # def sample_args(arg_1, arg_2, *args, kw_1="shark", kw_2="blobfish", **kwargs):
    def get_limit_value(self, limit):
        """To prevent limit value tends to infinite"""
        return min(self.MaxPerPageLimit, limit)

    def find(
            self,
            *args,
            defaults: dict = None,
            sort_by: dict = None,
            page: int = 1,
            limit: int = 10,
            **kwargs,
    ):
        """"""

        if defaults:
            try:
                args = ({**args[0], **defaults}, args[1])
            except:
                print(f"Exception while setting defaults filter: {args}")

        base_query = self.collection.find(*args, **kwargs)
        base_query.skip((limit - 1) * (page - 1)).limit(limit)
        if sort_by:
            base_query = base_query.sort(
                sort_by["key"], -1 if sort_by.get("DESC", True) else 1
            )

        return QueryIterator(base_query, self.bson_to_python)

    def aggregate_count(self, defaults: [dict] = None):
        filter_list = []
        matches = [d for d in defaults]
        match_list = {"$match": {"$and": matches}} if defaults else {"$match": {}}
        filter_list.append(match_list)
        x = self.collection.aggregate([filter_list[0], {
            "$group": {
                "_id": "_id",
                "count": {
                    "$sum": 1
                }
            }
        }])
        data_list = list(x)
        if len(list(data_list)):
            return list(data_list)[0].get('count')
        return 0


class VendorCollection(HelperCollection):
    base = {}
    category = "VENUE"
    admin_req_details = {}

    def get_vendor_projection(self):
        return {
            "_id": 0,
            "vendorId": "$vendorId",
            "urlSlug": "$urlSlug",
            "media": {
                "$map": {
                    "input": "$vm",
                    "as": "u",
                    "in": {
                        "url": {"$concat": [URL_PREFIX, "$$u.mediaUrl"]},
                        "mimeType": "$$u.mimeType",
                        "alt": "$$u.alt" if "$$u.alt" else None,
                        "mediaId": "$$u.mediaId"
                    },
                },
            },
            # "mediaUrl": {"$slice": ["$vm.mediaUrl", 4]},
            "venueName": "$name",
            "formattedAddress": "$formattedAddress",
            "city": "$address.city",
            "citySlug": "$address.citySlug",
            "shortAddress": {
                "$cond": {
                    "if": {"$eq": ["$address.city", "$address.locality"]},
                    "then": "$address.city",
                    "else": {"$concat": ["$address.locality", ", ", "$address.city"]}
                }
            },
            "userRating": {
                "$cond": {
                    "if": {"$eq": ["$gmapRatings", ""]},
                    "then": 1,
                    "else": "$gmapRatings"
                }
            },
            # TODO: need to change gmapRatings with rating after admin team fill rating data of venues.
            "meta": {
                "perPlateCost": {"$ifNull": ["$perPlateCost", None]},
                "perDayCost": {"$ifNull": ["$perDayCost", None]},
                "parkingCount": {"$ifNull": ["$parkingCount", None]},
                "roomCount": {"$ifNull": ["$roomCount", None]},
                "areasAvailable": {"$ifNull": ["$areasAvailable", None]},
            },
            "sort_by_status": "$sort_priority",
            "specialTags": {"$ifNull": ["$specialTags", []]},
            "isBhPartner": {"$ifNull": ["$bhPartnerStatus", False]},
            "bhPartnerDealText": {"$ifNull": ["$bhPartnerDealText", None]},
            "userRatingCount": {"$ifNull": ["$gmapsUserRatingsCount", None]},
            "coordinates": "$geoJsonCoordinates.coordinates",
        }

    def aggregate(
            self,
            defaults: [dict] = None,
            sort_by: dict = None,
            page: int = 1,
            limit: int = 10,
            vendor_id_list=None
    ):
        filter_list = [
            {
                "$lookup": {
                    "from": "vendorMedia",
                    "localField": "vendorId",
                    "foreignField": "vendorId",
                    "as": "vm",
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$isActive",
                                                True
                                            ]
                                        },
                                        {
                                            "$eq": [
                                                "$mediaType",
                                                "IMAGE"
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "$sort": {"priority": 1}
                        }
                    ]
                }
            }
        ]
        matches = [d for d in defaults]
        if vendor_id_list:
            matches.insert(0, {"vendorId": {"$in": vendor_id_list}})
        matches.append({"businessCategory": self.category})
        match_list = {"$match": {"$and": matches if defaults else [{"businessCategory": self.category}]}}
        size = None
        if page == 1:
            size = self.aggregate_count(matches)
        # match_list = {"$match": {"$and": [d for d in defaults] if defaults else []}}
        filter_list.append(match_list)
        if sort_by:
            filter_list.append({"$sort": {
                **{"bhPartnerStatusValue": -1, "specialTagsValue": -1, "rating": -1, "verificationStatusValue": -1},
                **sort_by,
                **{"createdAt": 1}
            }})

        filter_list.append({"$skip": (limit - 1) * (page - 1)})
        limit = self.get_limit_value(limit)
        filter_list.append({"$limit": limit})

        project_dict = {
            "$project": {
                **self.base, **self.get_vendor_projection()
            }
        }
        filter_list.append(project_dict)
        return QueryIterator(
            self.collection.aggregate(filter_list), self.bson_to_python, size
        )


class DecorCollection(VendorCollection):
    category = "DECORATION"

    def get_vendor_projection(self):
        return {
            "_id": 0,
            "vendorId": "$vendorId",
            "urlSlug": "$urlSlug",
            "media": {
                "$map": {
                    "input": "$vm",
                    "as": "u",
                    "in": {
                        "url": {"$concat": [URL_PREFIX, "$$u.mediaUrl"]},
                        "mimeType": "$$u.mimeType",
                        "alt": "$$u.alt" if "$$u.alt" else None,
                        "mediaId": "$$u.mediaId"
                    },
                },
            },
            # "mediaUrl": {"$slice": ["$vm.mediaUrl", 4]},
            "venueName": "$name",
            "meta": {
                "startsAt": {
                    "$cond": [
                        {
                            "$eq": [
                                {
                                    "$min": [
                                        {"$ifNull": ["$indoorPrice", 99999999]},
                                        {"$ifNull": ["$outdoorPrice", 99999999]}
                                    ]
                                },
                                99999999
                            ]
                        },
                        0,
                        {
                            "$min": [
                                {"$ifNull": ["$indoorPrice", 99999999]},
                                {"$ifNull": ["$outdoorPrice", 99999999]}
                            ]
                        }
                    ]
                }
            },
            "formattedAddress": "$formattedAddress",
            "city": "$address.city",
            "citySlug": "$address.citySlug",
            "shortAddress": {
                "$cond": {
                    "if": {"$eq": ["$address.city", "$address.locality"]},
                    "then": "$address.city",
                    "else": {"$concat": ["$address.locality", ", ", "$address.city"]}
                }
            },
            "userRating": {
                "$cond": {
                    "if": {"$eq": ["$gmapRatings", ""]},
                    "then": 1,
                    "else": "$gmapRatings"
                }
            },
            # TODO: need to change gmapRatings with rating after admin team fill rating data of venues.
            "businessCategory": "$businessCategory",
            "specialTags": {"$ifNull": ["$specialTags", []]},
            "isBhPartner": {"$ifNull": ["$bhPartnerStatus", False]},
            "bhPartnerDealText": {"$ifNull": ["$bhPartnerDealText", None]},
            "userRatingCount": {"$ifNull": ["$gmapsUserRatingsCount", None]},
            "coordinates": "$geoJsonCoordinates.coordinates",
        }


class PhotographyCollection(DecorCollection):
    category = "PHOTOGRAPHY"

    def get_vendor_projection(self):
        return {
            "_id": 0,
            "vendorId": "$vendorId",
            "urlSlug": "$urlSlug",
            "media": {
                "$map": {
                    "input": "$vm",
                    "as": "u",
                    "in": {
                        "url": {"$concat": [URL_PREFIX, "$$u.mediaUrl"]},
                        "mimeType": "$$u.mimeType",
                        "alt": "$$u.alt" if "$$u.alt" else None,
                        "mediaId": "$$u.mediaId"
                    },
                },
            },
            # "mediaUrl": {"$slice": ["$vm.mediaUrl", 4]},
            "venueName": "$name",
            "meta": {
                "startsAt": {
                    "$min": [
                        {"$ifNull": ["$indoorPrice", 99999999]},
                        {"$ifNull": ["$outdoorPrice", 99999999]}
                    ]
                },
                "services": {"$ifNull": ["$services", None]},
                "experience": {"$ifNull": ["$experience", None]}
            },
            "formattedAddress": "$formattedAddress",
            "city": "$address.city",
            "citySlug": "$address.citySlug",
            "shortAddress": {
                "$cond": {
                    "if": {"$eq": ["$address.city", "$address.locality"]},
                    "then": "$address.city",
                    "else": {"$concat": ["$address.locality", ", ", "$address.city"]}
                }
            },
            "userRating": {
                "$cond": {
                    "if": {"$eq": ["$gmapRatings", ""]},
                    "then": 1,
                    "else": "$gmapRatings"
                }
            },
            # TODO: need to change gmapRatings with rating after admin team fill rating data of venues.
            "businessCategory": "$businessCategory",
            "specialTags": {"$ifNull": ["$specialTags", []]},
            "isBhPartner": {"$ifNull": ["$bhPartnerStatus", False]},
            "bhPartnerDealText": {"$ifNull": ["$bhPartnerDealText", None]},
            "userRatingCount": {"$ifNull": ["$gmapsUserRatingsCount", None]},
            "coordinates": "$geoJsonCoordinates.coordinates",
        }


class AdminVendorCollection(VendorCollection):
    base = {"mobile.phoneNumber": 1, "mobile.countryCode": 1, "verificationStatus": 1, "seoStatus": 1}


class AdminDecorCollection(DecorCollection):
    base = {"mobile.phoneNumber": 1, "mobile.countryCode": 1, "verificationStatus": 1, "seoStatus": 1}


class AdminPhotographyCollection(PhotographyCollection):
    base = {"mobile.phoneNumber": 1, "mobile.countryCode": 1, "verificationStatus": 1, "seoStatus": 1}


class VendorMediaCollection(HelperCollection):
    def find(
            self,
            *args,
            defaults: dict = None,
            sort_by: dict = None,
            page: int = 1,
            limit: int = None,
            **kwargs,
    ):
        if not sort_by:
            sort_by = {"key": "priority", "DESC": False}  # default sort_by desc
        if not limit:
            limit = 25  # default limit
        return HelperCollection.find(
            self,
            *args,
            defaults=defaults,
            sort_by=sort_by,
            page=page,
            limit=limit,
            **kwargs,
        )


class VendorReviewsCollection(HelperCollection):
    def find(
            self,
            *args,
            defaults: dict = None,
            sort_by: dict = None,
            page: int = 1,
            limit: int = 10,
            **kwargs,
    ):
        if not sort_by:
            sort_by = {"key": "rating"}  # default sort_by desc
        return HelperCollection.find(
            self,
            *args,
            defaults=defaults,
            sort_by=sort_by,
            page=page,
            limit=limit,
            **kwargs,
        )


class VendorAppUserCollection(HelperCollection):
    base = {}

    def get_vendor_projection(self):
        return {
            "_id": 0,
            "vendorUserId": 1,
            "onboardingSource": 1,
            "userOnboardingState": 1,
            "isNewVendor": 1,
            "mobile": 1,
            "email": 1,
            "userVendorDiscoverabilityType": 1,
            "vendorIdClaimedState": 1,
            "alternateMobile": 1,
            "businessCategory": 1,
            "businessDetails.name": 1,
            "businessDetails.city": 1,
            "businessDetails.businessCategory": 1,
            "businessDetails.mobile.phoneNumber": 1,
            "firstName": 1,
            "lastName": 1,
        }

    def aggregate(
            self,
            defaults: [dict] = None,
            sort_by: dict = None,
            page: int = 1,
            limit: int = 10,
            sort_key: str = None,
            sort_key_column: str = "businessDetails.name",
            use_limit: bool = True
    ):
        filter_list = []
        matches = [d for d in defaults]
        match_list = {"$match": {"$and": matches}} if defaults else {"$match": {}}
        filter_list.append(match_list)
        filter_list.append({"$sort": {"createdAt": -1}})
        if sort_by:
            filter_list.append({"$sort": {**sort_by, **{"createdAt": -1}}})
        if sort_key:
            filter_list.append({"$sort": {sort_key_column: sort_key_map[sort_key]}})
        if use_limit:
            filter_list.append({"$skip": (limit - 1) * (page - 1)})
            limit = self.get_limit_value(limit)
            filter_list.append({"$limit": limit})
        project_dict = {
            "$project": {
                **self.base, **self.get_vendor_projection()
            }
        }
        filter_list.append(project_dict)
        return QueryIterator(
            self.collection.aggregate(filter_list), self.bson_to_python
        )


class LeadsCollection(VendorAppUserCollection):
    def get_vendor_projection(self):
        return {
            "_id": 0,
            "leadId": 1,
            "clientMobile": 1,
            "clientName": 1,
            "eventDate": 1,
            "clientAddress": 1,
            "leadServiceableStatus": 1,
            "leadRelevanceStatus": 1,
            "services": 1,
            "leadStatus": 1,
            "createdAt": 1,
            "initialOrderValue": 1,
            "createdBy": 1
        }

    def get_vendor_assignment_csv_projection(self):
        return {
            "vendorName": "$vendorDetails.vendorName",
            "assignmentType": "$vendorDetails.assignmentType",
            "vendorNumber": "$vendorDetails.mobile",
            "leadStatus": "$leadStatusByVendor",
            "leadCommunicationState": "$vendorLeadCommunicationState",
            "totalBudgetValue": "$totalBudgetValue",
            "vendorRejectionReason": "$vendorRejectionReason",
            "vendorBusinessCategory": "$businessCategory",
            "vendorEmail": "$vendorDetails.email",
            "isActive": "$isActive"
        }

    def aggregate_excluding_limit(
            self,
            defaults: [dict] = None,
            sort_by: dict = None,
            sort_key: str = None
    ):
        filter_list = []
        matches = [d for d in defaults]
        match_list = {"$match": {"$and": matches}} if defaults else {"$match": {}}
        filter_list.append(match_list)
        filter_list.append({"$sort": {"createdAt": -1}})
        if sort_by:
            filter_list.append({"$sort": {**sort_by, **{"createdAt": -1}}})
        if sort_key:
            filter_list.append({"$sort": {"businessDetails.name": sort_key_map[sort_key]}})

        project_dict = {
            "$project": {
                **self.base, **self.get_vendor_projection()
            }
        }
        lookup_exp = {
            "$lookup": {
                "from": "vendorLeadsAssignment",
                "localField": "leadId",
                "foreignField": "leadId",
                "as": "vl",
                "pipeline": [
                    {
                        "$project": self.get_vendor_assignment_csv_projection()
                    }
                ]
            }
        }
        filter_list.append(project_dict)
        filter_list.append(lookup_exp)
        return QueryIterator(
            self.collection.aggregate(filter_list), self.bson_to_python
        )


class LeadsVendorCollection(VendorAppUserCollection):
    def get_vendor_projection(self):
        return {
            "_id": 0,
            "leadId": 1,
            "vendorDetails": 1,
            "totalBudgetValue": 1,
            "leadStatusByVendor": 1,
            "businessCategory": 1,
            "vendorLeadCommunicationState": 1,
        }


class NotificationContentCollection(HelperCollection):
    def get_vendor_projection(self):
        return {
            '_id': 0,
            'templateId': 1,
            'data': 1,
            'notification': 1,
            'priority': 1,
        }


class VendorLeadsCSVCollection(HelperCollection):
    base = {}

    @staticmethod
    def get_vendor_leads_csv_projection():
        return {
            "_id": 0,
            "documentId": 1,
            "jobId": 1,
            "fileNameAtClient": 1,
            "documentType": 1,
            "mimeType": 1,
            "documentUrl": {"$concat": [URL_PREFIX, "$documentUrl"]},
            "adminEmail": "$va.email",
            "status": 1,
            "error": 1,
            "createdAt": {
                "$dateToString": {
                    "format": "%Y-%m-%dT%H:%M:%S.%L%z",
                    "date": "$createdAt"
                }
            }
        }

    def aggregate(
            self,
            defaults: [dict] = None,
            page: int = 1,
            limit: int = 10,
            use_limit: bool = True
    ):
        filter_list = [{
            "$lookup": {
                "from": "adminUser",
                "localField": "createdBy",
                "foreignField": "adminUserId",
                "as": "va",
                "pipeline": [{
                    "$project": {
                        "_id": 0,
                        "email": 1
                    }}
                ]
            }
        }]
        matches = [d for d in defaults]
        match_list = {"$match": {"$and": matches}} if defaults else {"$match": {}}
        filter_list.insert(0, match_list)
        filter_list.append({"$sort": {"createdAt": -1}})
        if use_limit:
            filter_list.append({"$skip": (limit - 1) * (page - 1)})
            limit = self.get_limit_value(limit)
            filter_list.append({"$limit": limit})
        project_dict = {
            "$project": {
                **self.base, **self.get_vendor_leads_csv_projection()
            }
        }
        size = None
        if page == 1:
            size = self.aggregate_count(matches)
        filter_list.append(project_dict)
        filter_list.append({"$unwind": "$adminEmail"})
        return QueryIterator(
            self.collection.aggregate(filter_list), self.bson_to_python, size
        )

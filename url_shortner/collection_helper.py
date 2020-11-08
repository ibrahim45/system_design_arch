from pymongo import ReturnDocument
from url_shortner.constants import TIMESTAMP_FORMAT
from datetime import datetime
import dateutil.parser
import json
from bson.objectid import ObjectId
from bson.json_util import dumps
from bson.json_util import loads
from datetime import datetime


class Collection:
    def __init__(self, coll_obj):
        self.obj = coll_obj

    def create_data(self, data):
        data['created_at'] = datetime.now()
        return data

    def create(self, data):
        data = self.create_data(data)
        is_inserted = self.obj.insert_one(data)
        return True, is_inserted.inserted_id if is_inserted.inserted_id else False, data

    def update(self, latest_record):
        ele = latest_record[0]
        _id = ele['_id']
        new_value = {"$set": {
            "xyz": "xyz",
            }
        }
        updated_data = self.obj.find_one_and_update({"_id": _id}, new_value, return_document=ReturnDocument.AFTER)
        return True if updated_data else False, updated_data

    def fetch_latest_record(self, query):
        query_result = self.obj.find(query).sort([('_id', -1)]).limit(1)
        counter = query_result.count()
        return {
            'query_result': query_result,
            'count': counter,
            'is_exists': True if counter > 0 else False,
            }

def convert_timestamp_to_datetime(val):
    return datetime.strptime(val, TIMESTAMP_FORMAT)

def dateutil_parser(val):
    return dateutil.parser.parse(val)

class JSONEncoder(json.JSONEncoder):
    ''' extend json-encoder class'''
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)


def cursor_to_dict_converter(cursor):
    return loads(dumps(cursor))

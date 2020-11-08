import hashlib
import sqlite3
import json
import logging
import os
from flask import Flask
from flask import request
from flask_api import status as api_status
from flask_pymongo import PyMongo
from url_shortner.collection_helper import Collection, JSONEncoder
from url_shortner.constants import result_update
from pymongo.errors import DuplicateKeyError
from datetime import datetime
from flask_script import Command, Manager
from flask_redis import FlaskRedis
import redis



class Config(object):
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    MONGO_URI = os.environ.get("MONGO_URI", None).strip()
    REDIS_URL = os.environ.get("REDIS_URL", None).strip()




app = Flask(__name__)
app.config.from_object(Config)
mongo = PyMongo(app, maxPoolSize=200)
core_collection = Collection(mongo.db['core'])
core_collection.obj.create_index("hash", unique=True)
redis_client = FlaskRedis(app)

# command_manager = Manager(app)

class SeedData(Command):
    """
    Seeding Data
    """
    def run(self):
        base_url = "https://www.google.com/search?q={0}"
        # range_val = 1000000
        range_val = 100
        from tqdm import tqdm

        # result = core_collection.obj.insert_many(
        #     [
        #         {
        #             'hash': shorten(base_url.fomat(i)),
        #             'original': base_url.fomat(i),
        #             'created_at': datetime.now(),
        #
        #         } for i in range(range_val)]
        # )
        #
        for i in tqdm(range_val):
            print(i)

# command_manager.run({'seed_data': SeedData()})


def shorten(arg):
    """
    Input pattern:
        A-Z(26)
        a-z(26)
        0–9(10)
        _, -(2)
        26 + 26 + 10 + 2 => 64
        long to shorten 7 characters => 64 ** 7 => 4398046511104
         -- 4, 398, 046, 511, 104 (combinations, or url can generate) ==>  4 trillion urls
         4398046511104 secs -> 139461.1400020294 years
        -- (4398046511104  /1000)  sec/per 1k requests -->  13 years to complete the unique id combinations
    """
    result = hashlib.md5(arg.encode('utf-8'))
    return result.hexdigest()


@app.route("/hello")
def hello():
    applicationVersion = os.environ['applicationVersion']
    return "<h1>Hello Trip services, we are running version -" + str(applicationVersion) + "</h1>"


@app.route("/url-minify", methods=['POST'])
def minify_url():
    result = {}
    result_status = api_status.HTTP_400_BAD_REQUEST
    data = request.get_json()
    input_data = data['val'].strip()
    hashed_input_data = shorten(input_data)[:7]
    try:
        uniq_record_query = core_collection.fetch_latest_record({'hash': hashed_input_data})
        if not uniq_record_query['is_exists']:
            result = core_collection.create({'hash': hashed_input_data, 'original': input_data})
        else:
            result = uniq_record_query['query_result'][0]
        return json.dumps(result, cls=JSONEncoder), api_status.HTTP_200_OK
    except DuplicateKeyError:
        uniq_record_query = core_collection.fetch_latest_record({'hash': hashed_input_data})
        result = result_update(False, {"db error": "Duplicate value exists"}, "Unexpected error occured")
        result['info'] = uniq_record_query['query_result'][0]
        result_status = api_status.HTTP_500_INTERNAL_SERVER_ERROR
    return json.dumps(result, cls=JSONEncoder), result_status


@app.route("/url-get-minify/<hash_id>", methods=['GET'])
def get_minify_url(hash_id):
    result_status = api_status.HTTP_200_OK
    redis_result = redis_client.get(hash_id.strip())
    # r = redis.StrictRedis()
    # redis_result = r.execute_command('JSON.GET', hash_id.strip())
    if not redis_result:
        uniq_record_query = core_collection.fetch_latest_record({'hash': hash_id.strip()})
        result = uniq_record_query['query_result'][0] if uniq_record_query['is_exists'] else {}
        redis_client.set(hash_id.strip(), json.dumps(result, cls=JSONEncoder))

        # r.execute_command('JSON.SET', hash_id.strip(), '.', json.dumps(result, cls=JSONEncoder))
    else:
        result = json.loads(redis_result.decode('utf-8'))
    # create_seed_data()
    return result, result_status





def create_seed_data():
    base_url = "https://www.google.com/search?q={0}"
    range_val = 1000000
    # range_val = 100
    from tqdm import tqdm
    # result = core_collection.obj.insert_many(
    #     [
    #         {
    #             'hash': shorten(base_url.format(i)),
    #             'original': base_url.format(i),
    #             'created_at': datetime.now(),
    #
    #         } for i in range(range_val)]
    # )
    data = []
    for i in tqdm(list(range(0, 1000000))):
        data.append(
            {
                        'hash': shorten(base_url.format(i)),
                        'original': base_url.format(i),
                        'created_at': datetime.now(),

             }
        )
    print(data)
    print(len(data))
    result = core_collection.obj.insert_many(data)


if __name__ == "__main__":
    # create_seed_data()
    app.run()


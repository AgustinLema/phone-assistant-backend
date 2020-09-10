import os

from flask import Flask
from flask_cors import CORS, cross_origin

from flask_pymongo import PyMongo
from bson import json_util
# from bson.objectid import ObjectId

import logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
app.debug = True
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

MONGO_USER = os.getenv("MONGO_DB_USER")
MONGO_PASS = os.getenv("MONGO_DB_PASS")
MONGO_HOST = os.getenv("MONGO_DB_HOST")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

MONGO_CONNECTION_STRING = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/{MONGO_DB_NAME}?authSource=admin"

print("Connection string: ", MONGO_CONNECTION_STRING)
app.config["MONGO_URI"] = MONGO_CONNECTION_STRING
mongo = PyMongo(app)


@app.route('/phone-data', methods=['GET'])
@cross_origin()
def get_phone_data():
    mobile_phones = mongo.db.mobile_phone.find()
    mobile_phones = [phone for phone in mobile_phones if "prices" in phone and phone["prices"]]
    response = json_util.dumps(mobile_phones)
    # logging.debug(["Data found", response])
    return response


@app.route('/phone-data/<int:id>', methods=['GET'])
@cross_origin()
def get_phone_details(id):
    mobile_phones = mongo.db.mobile_phone.find_one({'phone_id': id})
#     # json_mobile_phones = json_util.dumps(mobile_phones)
    if mobile_phones is None:
        return {}
    prices_data = list(mongo.db.offer_history.find({'classified_mobile_phone': mobile_phones['unique_name']}))
#     # json_prices_data = json_util.dumps(prices_data)
    mobile_phones['offers'] = prices_data
#     # logging.debug(["Data found", response])
#     logging.debug("Phone data")
#     logging.debug(mobile_phones)
    return json_util.dumps(mobile_phones)
    # except Exception as e:
    #     logging.error(e)
    #     # raise


@app.route('/phone-data/<int:id>/price-summary', methods=['GET'])
@cross_origin()
def get_phone_price_summary(id):
    price_summary = mongo.db.weekly_phone_price.find({'phone_id': id})
    if price_summary is None:
        price_summary = {}
    return json_util.dumps(price_summary)


@app.route('/phone-data/<name>', methods=['GET'])
@cross_origin()
def get_phone_details_by_name(name):
    mobile_phones = mongo.db.mobile_phone.find_one({'unique_name': name})
#     # json_mobile_phones = json_util.dumps(mobile_phones)
    if mobile_phones is None:
        return {}
#     prices_data = list(mongo.db.offer_history.find({'classified_mobile_phone': name}))
#     # json_prices_data = json_util.dumps(prices_data)
#     logging.debug(str(prices_data))
#     mobile_phones['offers'] = prices_data
#     # logging.debug(["Data found", response])
#     logging.debug("Phone data")
#     logging.debug(mobile_phones)
#     return json_util.dumps(mobile_phones)
    # except Exception as e:
    #     logging.error(e)
    #     # raise
    return get_phone_data()


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)

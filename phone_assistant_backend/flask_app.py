import os

from flask import Flask
from flask_cors import CORS, cross_origin

from flask_pymongo import PyMongo
from bson import json_util
# from bson.objectid import ObjectId

import logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
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
    phone_data = mongo.db.phone_data.find()
    response = json_util.dumps(phone_data)
    logging.debug(["Data found", response])
    return response


@app.route('/phone-data/<name>', methods=['GET'])
@cross_origin()
def get_phone_details(name):
    phone_data = mongo.db.phone_data.find_one({'unique_name': name})
    # json_phone_data = json_util.dumps(phone_data)
    if phone_data is None:
        return
    prices_data = list(mongo.db.phone_data_old.find({'classified': name}))  # TODO: Update collection
    # json_prices_data = json_util.dumps(prices_data)
    logging.debug(str(prices_data))
    phone_data['offers'] = prices_data
    # logging.debug(["Data found", response])
    return json_util.dumps(phone_data)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)

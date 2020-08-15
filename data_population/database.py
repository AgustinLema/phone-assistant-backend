import os

from pymongo import MongoClient

MONGO_USER = os.getenv("MONGO_DB_USER")
MONGO_PASS = os.getenv("MONGO_DB_PASS")
MONGO_HOST = os.getenv("MONGO_DB_HOST")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

MONGO_CONNECTION_STRING = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/{MONGO_DB_NAME}?authSource=admin"

client = MongoClient(MONGO_CONNECTION_STRING)

db = client.phone_assistant

mercadolibre_raw_data = db['mercadolibre_raw_data']
price_history = db['price_history']
categorized_price_history = db['categorized_price_history']
phone_data = db['phone_data']

# mercadolibre_raw_data.insert_one({"test": "created"})

print(db.list_collection_names())

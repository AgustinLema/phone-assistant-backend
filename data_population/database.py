import os
import logging

from pymongo import MongoClient, errors, ReplaceOne, UpdateOne, ASCENDING

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


MONGO_USER = os.getenv("MONGO_DB_USER")
MONGO_PASS = os.getenv("MONGO_DB_PASS")
MONGO_HOST = os.getenv("MONGO_DB_HOST")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

MONGO_CONNECTION_STRING = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/{MONGO_DB_NAME}?authSource=admin"

client = MongoClient(MONGO_CONNECTION_STRING)

db = client.phone_assistant

mercadolibre_raw_data = db['mercadolibre_raw_data']
ebay_raw_data = db['ebay_raw_data']
offer_history = db['offer_history']
# categorized_offer_history = db['categorized_offer_history']
mobile_phone = db['mobile_phone']
phone_classifications = db['phone_classifications']
weekly_phone_price = db['weekly_phone_price']


def insert_many_ignore_duplicates(collection, objs):
    try:
        result = collection.insert_many(objs, ordered=False)  # with ordered=false, even if there are duplicates the rest continue
        logger.info(f"Inserted documents: {len(result.inserted_ids)}")
    except errors.BulkWriteError as e:
        details = e.details
        del details['writeErrors']  # remove since it is too vebose
        logger.warning("Bulk write error")
        logger.warning(details)


def upsert_many(collection, objs, keys=["_id"]):
    upserts = []
    for obj in objs:
        obj_no_id = obj.copy()
        if '_id' in obj:
            del obj_no_id['_id']

        upserts.append(ReplaceOne(
                {
                    k: obj.get(k)
                    for k in keys
                },
                obj_no_id,
                upsert=True
            )
        )
    result = collection.bulk_write(upserts)
    logger.info(f"Inserted documents: {result.inserted_count}, Updated documents: {result.modified_count}")


def add_field_update_many(collection, filters_and_new_values_tuples):
    updates = []
    for filters, new_values in filters_and_new_values_tuples:
        updates.append(UpdateOne(
                filters,
                new_values,
            )
        )
    result = collection.bulk_write(updates)
    logger.info(f"Inserted documents: {result.inserted_count}, Updated documents: {result.modified_count}")


def create_collection_indexes():
    mobile_phone_indexes = [
        [
            ("unique_name", ASCENDING),
        ],
        [
            ("dataset_unique_name", ASCENDING),
        ],
        [
            ("phone_id", ASCENDING),
        ],
    ]
    for mobile_phone_index in mobile_phone_indexes:
        mobile_phone.create_index(mobile_phone_index, unique=True)

    offer_history_index = [
        ("date", ASCENDING),
        ("eshop", ASCENDING),
        ("link", ASCENDING),
    ]
    offer_history.create_index(offer_history_index, unique=True)

    phone_classifications_index = [
        ("offer_title", ASCENDING),
    ]
    phone_classifications.create_index(phone_classifications_index, unique=True)

    mercadolibre_raw_data_index = [
        ("id", ASCENDING),
        ("api_fetch_date", ASCENDING),
        ("price", ASCENDING),
    ]
    mercadolibre_raw_data.create_index(mercadolibre_raw_data_index, unique=True)

    ebay_raw_data_index = [
        ("itemId", ASCENDING),
        ("extracted_current_price", ASCENDING),
        ("api_fetch_date", ASCENDING),
    ]
    ebay_raw_data.create_index(ebay_raw_data_index, unique=True)

    weekly_phone_price_index = [
        ("phone_id", ASCENDING),
        ("week_of_year", ASCENDING),
        ("currency", ASCENDING),
    ]
    weekly_phone_price.create_index(weekly_phone_price_index, unique=True)


# mercadolibre_raw_data.insert_one({"test": "created"})
create_collection_indexes()
print(db.list_collection_names())

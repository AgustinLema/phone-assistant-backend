import database
import pymongo

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

cleaned_items = []
for item in database.mercadolibre_raw_data.find():
    if item["condition"] != "new":
        logger.error(["Invalid item condition", item["condition"]])
        continue
    if item["sold_quantity"] < 1:
        # logger.debug(["Skipping item with 0 sold quantity", item['title']])
        continue
    cleaned_items.append(item)
    # print(item['title'])
print(len(cleaned_items))

offer_history = {}
for item in cleaned_items:
    obj = {
        'date': item['api_fetch_time'].strftime("%d-%m-%Y"),
        'eshop': 'MLA',
        'title': item['title'],
        'link': item['permalink'],
        'currency': item['currency_id'],
        'amount': item['price'],
        'sold_quantity': item['sold_quantity'],
        # 'mobile_phone_id': item[],
    }
    key = f"{obj['date']} - {obj['eshop']} - {obj['link']}"
    offer_history[key] = obj

print(len(offer_history.keys()))

try:
    database.offer_history.insert_many(offer_history.values(), ordered=False)  # with ordered=false, even if there are duplicates the rest continue
except pymongo.errors.BulkWriteError as e:
    details = e.details
    del details['writeErrors']  # remove since it is too vebose
    logger.warning("Bulk write error")
    logger.warning(details)

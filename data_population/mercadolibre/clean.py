import database
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
        # continue
        pass
    cleaned_items.append(item)
    # print(item['title'])
print(len(cleaned_items))

price_history = {}
for item in cleaned_items:
    obj = {
        'date': item['api_fetch_time'].strftime("%d-%m-%Y"),
        'eshop': 'MLA',
        'title': item['title'],
        'link': item['permalink'],
        'currency': item['currency_id'],
        'amount': item['price'],
        # 'mobile_phone_id': item[],
    }
    key = f"{obj['date']} - {obj['eshop']} - {obj['link']}"
    price_history[key] = obj

print(len(price_history.keys()))
database.price_history.insert_many(price_history.values())

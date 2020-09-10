import datetime

from . import ml_api
import database


def main():
    category = 'MLA1055'
    query = None
    only_new = True
    find_most_sold = True
    products = ml_api.find_products_by_category(category, query, only_new, find_most_sold)

    for item in products:
        date_now = datetime.datetime.utcnow()
        item['api_fetch_time'] = date_now
        item['api_fetch_date'] = datetime.datetime(date_now.year, date_now.month, date_now.day)

    database.upsert_many(database.mercadolibre_raw_data, products, ['id', 'price', 'api_fetch_date'])


if __name__ == "__main__":
    main()

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
        item['api_fetch_time'] = datetime.datetime.utcnow()

    database.mercadolibre_raw_data.insert_many(products)


if __name__ == "__main__":
    main()

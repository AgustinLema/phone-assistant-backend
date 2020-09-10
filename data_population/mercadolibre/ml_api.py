import requests
import os
import json


def cache_on_disk(func):
    CACHE_DIR = '.cache'

    def inner(*args, **kwargs):
        file_name = "_".join([func.__name__] + list(map(str, list(args))))
        full_path = os.path.join(CACHE_DIR, file_name)
        print("Will check on disk for ", file_name)
        if os.path.exists(full_path):
            with open(full_path, 'r') as f:
                return json.load(f)
        print("Not found, calling function")
        feedback_tuples = func(*args, **kwargs)
        print("Will save value ", feedback_tuples)
        with open(full_path, 'w') as f:
            json.dump(feedback_tuples, f)
        return feedback_tuples
    return inner


def _get_item_info(item_id):
    url = f'https://api.mercadolibre.com/reviews/item/{item_id}?limit=10000'
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_feedback_tuple(item_info):
    review_tuples = []
    for review in item_info['reviews']:
        review_tuples.append((review['content'], review['rate']))
    # print ("Adding tuples: ", review_tuples)
    return review_tuples


def get_search_results(base_url, page_size=50, max_offset=1000):
    total, offset = 1, 0
    total_results = []
    base_url = base_url + "&limit={}".format(page_size) + "&offset={offset}"
    while offset < total and offset <= max_offset:
        url = base_url.format(offset=offset)
        print(url)
        response = requests.get(url)
        response.raise_for_status()
        response_body = response.json()
        total_results += response_body['results']
        offset += page_size
        total = response_body['paging']['total']
    return total_results


def find_products_by_name(words):
    SITE_ID = 'MLA'
    total_results = []
    total, offset = 1, 0
    page_size = 50
    words = " ".join(words)
    url_base = f'https://api.mercadolibre.com//sites/{SITE_ID}/search?q={words}&limit={page_size}' + "&offset={offset}"

    while offset < total and offset <= 1000:
        url = url_base.format(offset=offset)
        print(url)
        response = requests.get(url)
        response.raise_for_status()
        response_body = response.json()
        total_results += response_body['results']
        offset += page_size
        total = response_body['paging']['total']

    return total_results


# @cache_on_disk
def find_products_by_category(category, query=None, only_new=False, find_most_sold=False):
    url = f"https://api.mercadolibre.com/sites/MLA/search?category={category}"
    if query is not None:
        url = f"{url}&q={query}"
    if only_new:
        url = f"{url}&ITEM_CONDITION=2230284"
    if find_most_sold:
        url = f"{url}&sort=sold_quantity"
    return get_search_results(url)


def _title_has_all_words(title, words):
    l_title = title.lower()
    for word in words:
        if word.lower() not in l_title.split():
            return False
    return True


def filter_products_by_name(products, words):
    filtered_products = []
    for product in products:
        if _title_has_all_words(product['title'], words):
            filtered_products.append(product)
    return filtered_products


@cache_on_disk
def get_feedback_tuples_by_word_search(words):
    products = find_products_by_name(words)
    products = filter_products_by_name(products, words)
    feedback_tuples = []
    product_ids = {}
    for product in products:
        if product['sold_quantity'] < -1:
            continue
        print(product['title'], product['id'], product['catalog_product_id'])
        # item_id = 'MLA723647586'
        product_ids[product['id']] = True
        if product['catalog_product_id'] is not None:
            product_ids[product['catalog_product_id']] = True
    for product_id in product_ids.keys():
        item_info = _get_item_info(product_id)
        feedback_tuples += get_feedback_tuple(item_info)
    print("Number of feedbacks:", len(feedback_tuples))
    feedback_tuples = list(set(feedback_tuples))
    print("Number of unique feedbacks:", len(feedback_tuples))
    return feedback_tuples


if __name__ == "__main__":
    words = ['arduino', 'uno']
    feedbacks = get_feedback_tuples_by_word_search(words)
    # print(feedbacks)

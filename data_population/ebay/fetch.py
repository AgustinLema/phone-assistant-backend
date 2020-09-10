import os
import requests
import datetime

import database


EBAY_API_KEY = os.getenv('EBAY_API_KEY')
API_VERSION = "1.13.0"
SMARTPHONE_CATEGORY = "9355"


def advanced_search(category, pages=1, item_filters=None, keywords=None):
    service_url = "https://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findItemsAdvanced"
    query_string_params = {
        "SERVICE-VERSION": API_VERSION,
        "SECURITY-APPNAME": EBAY_API_KEY,
        "RESPONSE-DATA-FORMAT": "JSON",
        "categoryId": category,
    }
    if keywords:
        query_string_params["keywords"] = keywords

    item_filter_counter = 0
    for name, value in item_filters.items():
        filter_group = f"itemFilter({item_filter_counter})"
        query_string_params[f"{filter_group}.name"] = name
        if type(value) == list:
            for i in range(len(value)):
                query_string_params[f"{filter_group}.value({i})"] = value[i]
        else:
            query_string_params[f"{filter_group}.value"] = value
        item_filter_counter += 1

    query_string_params_str = "&".join([f"{k}={v}" for k, v in query_string_params.items()])
    url = f"{service_url}&{query_string_params_str}"
    return get_paginated_result(url, pages)

# outputSelector=AspectHistogram&itemFilter.name=Condition&itemFilter.value=1000&
# itemFilter(0).name=ListingType&itemFilter(0).value(0)=Classified&itemFilter(0).value(1)=FixedPrice&itemFilter(0).value(2)=StoreInventory&
# itemFilter(1).name=HideDuplicateItems&itemFilter(1).value=true%20
# &itemFilter(2).name=TopRatedSellerOnly&itemFilter(2).value=true
# &keywords=motorola&paginationInput.pageNumber=1


def get_paginated_result(url, pages):
    total_results = []
    for page in range(1, pages + 1):
        page_url = f"{url}&paginationInput.pageNumber={page}"
        print(page_url)
        response = requests.get(page_url)
        page_results = list(response.json().values())[0][0]['searchResult'][0]["item"]
        total_results.extend(page_results)
    return total_results


def main():
    item_filters = {
        "Condition": "1000",  # NEW
        "ListingType": [
            "Classified",
            "FixedPrice",
            "StoreInventory",
        ],
        "HideDuplicateItems": "true",
        "TopRatedSellerOnly": "true"
    }
    pages = 10
    brands = ["Samsung", "Xiaomi", "Motorola", "Apple", "Huawei", "Nokia", "LG", "Sony", "Honor", "Google"]
    results = []
    for brand in brands:
        brand_results = advanced_search(SMARTPHONE_CATEGORY, pages, item_filters, brand)
        results.extend(brand_results)

    for item in results:
        current_price = item['sellingStatus'][0]["currentPrice"][0]
        item['extracted_current_price'] = current_price["__value__"]
        item['extracted_current_currency'] = current_price["@currencyId"]
        date_now = datetime.datetime.utcnow()
        item['api_fetch_time'] = date_now
        item['api_fetch_date'] = datetime.datetime(date_now.year, date_now.month, date_now.day)

    database.upsert_many(database.ebay_raw_data, results, ['itemId', 'extracted_current_price', 'api_fetch_date'])


if __name__ == "__main__":
    main()

import textdistance
import csv

import ml_api
import models

from datetime import datetime


MODEL_LIST = [
    'SAMSUNG S10',
    'SAMSUNG S10 e',
    'SAMSUNG S10+',
    'SAMSUNG S10 lite',
]

MODEL_LIST = models.SAMSUNG


def get_best_match(title, options):
    best_score = 0
    normalized_best_score = 0
    match = None
    # TODO: Sanitize title by removing commas and entra words. Also how to handle extra spaces? Maybe aliases?
    title_tokens = title.lower().split()
    for option in options:
        option_tokens = option.lower().split()
        # Multiplier so that matches were products with longer names have more priority than just brand.
        multiplier = min(len(title_tokens), len(option_tokens))
        # Minus weight to prefer full match with few terms that partial match with many.
        score = (textdistance.overlap(title_tokens,
                                      option_tokens) - 0.01) * multiplier
        if score > best_score:
            best_score = score
            normalized_best_score = score / multiplier
            match = option
    return match, normalized_best_score


def main():
    category = 'MLA1055'
    query = 'samsung'
    products = ml_api.find_products_by_category(category, query, True)
    print(products)
    product_by_model = {}
    for product in products:
        product_title = product['title']
        best_match, score = get_best_match(product_title, MODEL_LIST)
        if score == 0:
            best_match = 'UNCLASSIFIED'
        if best_match not in product_by_model:
            product_by_model[best_match] = []
        product_by_model[best_match].append((product, score))

    rows = []
    for model in product_by_model.keys():
        scored_product = product_by_model[model]
        sorted_scored_product = sorted(
            scored_product, key=lambda product: product[1])
        print(f"######{model}######")
        for product, score in sorted_scored_product:
            row_data = list(map(str, [score, product['title'], product['price'],
                                      product['condition'], product['sold_quantity'], product['permalink']]))
            rows.append([model] + row_data)
            print(";".join(row_data))

    model_averages = {}
    for model in product_by_model.keys():
        summed = sum([w for p, w in product_by_model[model]])
        avg = summed / len(product_by_model[model])
        model_averages[model] = avg

    sorted_averages = sorted(model_averages.items(),
                             key=lambda item: item[1], reverse=True)
    for row in sorted_averages:
        print(row)

    export_file_name = 'exports/export.{}.csv'.format(
        datetime.now().timestamp())
    with open(export_file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


if __name__ == "__main__":
    main()

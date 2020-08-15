import database
import logging
import textdistance

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main():
    database.categorized_price_history.drop()

    phone_prices = list(database.price_history.find())
    phones = ['MOTOROLA', 'LENOVO', 'SAMSUNG']
    phones = list(database.phone_data.find())
    phone_match_names = [phone['unique_name'] for phone in phones]

    classify_prices_by_phone(phone_prices, phone_match_names)

    database.categorized_price_history.insert_many(phone_prices)


def classify_prices_by_phone(objs, phones, key='title'):
    counter = 0
    total = len(objs)
    last_percentage = -1
    for obj in objs:
        percentage = (counter * 100) // total
        if ((percentage % 1) == 0) and last_percentage != percentage:
            print(f"Matching titles... {percentage}% completed.")
            last_percentage = percentage
        counter += 1
        text_to_classify = obj[key]
        best_match, score = get_best_match(text_to_classify, phones)
        if score < 0:
            continue
        obj['classified'] = best_match
        obj['classification_score'] = score
    return
    # product_by_model = {}
    # for product in products:
    #     product_title = product['title']
    #     best_match, score = get_best_match(product_title, MODEL_LIST)
    #     if score == 0:
    #         best_match = 'UNCLASSIFIED'
    #     if best_match not in product_by_model:
    #         product_by_model[best_match] = []
    #     product_by_model[best_match].append((product, score))


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


if __name__ == "__main__":
    main()

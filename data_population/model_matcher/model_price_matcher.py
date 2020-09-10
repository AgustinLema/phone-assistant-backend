import database
import logging
import textdistance

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main():
    # Get list of titles from offers
    offers = list(database.offer_history.find())
    for offer in offers:
        title = offer['title']
        if type(title) != str:
            print(offer)
    offer_titles = {offer['title'] for offer in offers}

    # Check which titles are not classfied yet
    classifications = list(database.phone_classifications.find())
    classified_titles = {classification['offer_title'] for classification in classifications}
    unclassified_titles = offer_titles.difference(classified_titles)

    if unclassified_titles:
        logger.info("{len(unclassified_titles) new titles to classify}")

        # Load phone data for classification
        phones = list(database.mobile_phone.find())

        # Classify titles
        new_phone_classifications = get_classified_titles_by_phone(unclassified_titles, phones)

        # Save classified titles
        database.insert_many_ignore_duplicates(database.phone_classifications, new_phone_classifications)

        # Update list of classifications in memory
        classifications.extend(new_phone_classifications)
    else:
        logger.info("There are no new titles to classify")

    # Update all offers with the classification
    classification_by_title = {classification["offer_title"]: classification for classification in classifications}
    reclassification_count = 0
    updated_offers = []
    for offer in offers:
        if offer['title'] in classification_by_title:
            classification = classification_by_title[offer['title']]
            if ("classified_mobile_phone" not in offer or offer["classified_mobile_phone"] != classification['classified_mobile_phone'] or
                    "classified_mobile_phone_id" not in offer or offer["classified_mobile_phone_id"] != classification['classified_mobile_phone_id']):
                reclassification_count += 1
                offer["classified_mobile_phone"] = classification['classified_mobile_phone']
                offer["classified_mobile_phone_id"] = classification['classified_mobile_phone_id']
                offer["classification_score"] = classification['classification_score']
                offer["visible_classification"] = classification['classification_score'] > 0.5
                updated_offers.append(offer)

    if updated_offers:
        logger.info(f"Updating offer history with {reclassification_count} new reclassification")
        database.upsert_many(database.offer_history, updated_offers)
        logger.info("Classification completed")
    else:
        logger.info("There are no offers to classify")


def get_classified_titles_by_phone(titles, phones):
    """ Returns a list of objects with the classification info for each title """
    phone_ids_by_name = {phone['unique_name']: phone['phone_id'] for phone in phones}
    phone_match_names = phone_ids_by_name.keys()
    associations = get_options_associations(titles, phone_match_names)

    association_objs = [
        {
            "offer_title": title,
            "classified_mobile_phone": associations[title][0] or None,
            "classification_score": associations[title][1],
            "classified_mobile_phone_id": phone_ids_by_name[associations[title][0]] if associations[title][0] else None,
        }
        for title in associations.keys()
    ]
    return association_objs


def get_options_associations(values, options):
    unique_values = {obj for obj in values}
    total = len(unique_values)
    logger.info(
        f"There are {len(values)} to classify, with {len(unique_values)} unique values accross {len(options)} options")

    matches = {}
    counter = 0
    last_percentage = -1
    options_tokens = get_options_tokens(options)
    for value in unique_values:
        percentage = (counter * 100) // total
        if ((percentage % 1) == 0) and last_percentage != percentage:
            logger.info(f"Matching options... {percentage}% completed.")
            last_percentage = percentage
        counter += 1
        best_match, score = get_best_match(value, options, options_tokens)
        if score < 0:
            continue
        matches[value] = best_match, score
    return matches


def get_options_tokens(options):
    return {
        option: option.lower().split()
        for option in options
    }


def get_best_match(title, options, options_tokens=None):
    best_score = 0
    normalized_best_score = 0
    match = None
    if options_tokens is None:
        options_tokens = get_options_tokens(options)
    # TODO: Sanitize title by removing commas and entra words. Also how to handle extra spaces? Maybe aliases?
    title_tokens = title.lower().split()
    for option in options:
        option_tokens = options_tokens[option]
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

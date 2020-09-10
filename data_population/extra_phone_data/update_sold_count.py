import logging
from datetime import datetime

import statistics
import database

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main():
    # Get all offers
    offer_history = list(database.offer_history.find())

    # Keep one of per link
    offers_by_link = {}
    for offer in offer_history:
        # Skip offers with low classification score
        if not offer["visible_classification"]:
            continue
        if offer.get("sold_quantity", 0) <= 0:  # Skip offers with no sold items
            continue

        link = offer["link"]
        if link in offers_by_link:
            seen_offer = offers_by_link[link]
            if offer["sold_quantity"] <= seen_offer["sold_quantity"]:
                continue  # If we already have something with more sells, keep it
        offers_by_link[link] = offer

    # Get sold count by phone
    sold_by_phone = {}
    for offer in offers_by_link.values():
        phone_id = offer["classified_mobile_phone_id"]
        sold_by_phone[phone_id] = sold_by_phone.get(phone_id, 0) + offer["sold_quantity"]

    # Update sold count
    phones = database.mobile_phone.find()
    filters_and_new_values_tuples = []
    for phone in phones:
        phone_id = phone["phone_id"]
        new_amount = sold_by_phone.get(phone_id, 0)
        if phone.get("sold_quantity", 0) == new_amount:
            continue  # No changes in amount, continue

        filters = {
            'phone_id': phone_id,
        }
        new_values = {
            "$set": {
                "sold_quantity": new_amount,
            }
        }
        filters_and_new_values_tuples.append((filters, new_values))

    if filters_and_new_values_tuples:
        database.add_field_update_many(database.mobile_phone, filters_and_new_values_tuples)
        logger.info("Done updating sold quantity for each phone")
    else:
        logger.info("There are no updates needed in the sold quantity of phones")


if __name__ == "__main__":
    main()

import database
import logging
import statistics

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main():
    # Get prices from last 7 days from price summary
    last_7_days_price_summary = database.weekly_phone_price.find({'week_of_year': 'LATEST_7_DAYS'})
    if not last_7_days_price_summary:
        logger.error("No price summary data found with latest 7 days week")
        return
    data_by_phone = {data['phone_id']: data for data in last_7_days_price_summary if data['currency'] == 'ARS'}

    # Get all phone data
    phones = list(database.mobile_phone.find())
    logger.info(f"Updating current prices for all phones ({len(phones)} found)")

    filters_and_new_values_tuples = []
    for phone in phones:
        phone_id = phone["phone_id"]
        price_summary = data_by_phone.get(phone_id, {})  # Blank prices if there is no data in last 7 days
        price_fields = ['min', 'max', "mean", "median", "st_deviation"]
        price_summary = {k: v for k, v in price_summary.items() if k in price_fields}
        if "prices" in phone and phone["prices"] == price_summary:
            continue

        filters = {
            'phone_id': phone_id,
        }
        new_values = {
            "$set": {
                "prices": price_summary,
            }
        }
        filters_and_new_values_tuples.append((filters, new_values))

    if filters_and_new_values_tuples:
        database.add_field_update_many(database.mobile_phone, filters_and_new_values_tuples)
        logger.info("Done updating price summary for each phone")
    else:
        logger.info("No new prices to update, prices remain the same as last run")


def main_deprecated():
    # Get phone prices from offer_history
    offer_history = database.offer_history.find()

    # Make prices dict for each classified_mobile_phone # TODO: Do with phone id
    prices_by_phone = {}
    for offer in offer_history:
        if not offer['visible_classification']:
            continue  # Ignore offers that have low classification score to avoid noise in the data
        offer_mobile_phone = offer["classified_mobile_phone"]  # TODO: Move to IDs
        prices = prices_by_phone.get(offer_mobile_phone, [])
        # TODO: Filter prices of old dates
        prices.append({
            'amount': offer['amount'],  # TODO: Keep only prices from recent week and avoid duplicate prices in offers
            'currency': offer['currency'],
        })
        prices_by_phone[offer_mobile_phone] = prices

    # logger.info(prices_by_phone)
    price_summary_by_phone = {}
    for phone, prices in prices_by_phone.items():
        raw_prices = [price['amount'] for price in prices if price['currency'] == 'ARS']
        if not raw_prices:
            continue

        price_summary_by_phone[phone] = {
            "min": min(raw_prices),
            "max": max(raw_prices),
            "mean": statistics.mean(raw_prices),
            "median": statistics.median(raw_prices),
            "st_deviation": statistics.stdev(raw_prices) if len(raw_prices) > 1 else 0,
        }
        currencies = {price['currency'] for price in prices if price['currency'] != 'ARS'}
        for currency in currencies:
            currency_prices = [price['amount'] for price in prices if price['currency'] == currency]
            median_price = statistics.median(currency_prices)
            price_summary_by_phone[phone][f'median_price_for_offers_in_{currency}'] = median_price
            price_summary_by_phone[phone][f'price_factor_{currency}'] = price_summary_by_phone[phone]['median'] / median_price

    # print({phone: price for phone, price in price_summary_by_phone.items() if price['st_deviation'] != 0})
    # Perform update of prices on mobile_phone database
    phones = list(database.mobile_phone.find({'dataset_unique_name': {"$in": list(price_summary_by_phone.keys())}}))
    logger.info(f"Updating phones prices, {len(phones)} found")

    filters_and_new_values_tuples = []
    for phone in phones:
        phone_unique_name = phone["unique_name"]
        price_summary = price_summary_by_phone[phone_unique_name]
        filters = {
            'dataset_unique_name': phone_unique_name,
        }
        new_values = {
            "$set": {
                "prices": price_summary,
            }
        }
        filters_and_new_values_tuples.append((filters, new_values))

    if filters_and_new_values_tuples:
        database.add_field_update_many(database.mobile_phone, filters_and_new_values_tuples)
        logger.info("Done updating price summary for each phone")
    else:
        logger.warn("Nothing to update in price matcher, are there no offers matching phones with good scores?")


if __name__ == "__main__":
    main()

import logging
from datetime import datetime

import statistics
import database

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main():
    # Get phone prices from offer_history
    offer_history = list(database.offer_history.find())

    # Group prices by phone, week and currency
    grouped_offers = {}
    today = datetime.utcnow()
    for offer in offer_history:
        if not offer['visible_classification']:
            continue  # Ignore offers that have low classification score to avoid noise in the data
        offer_mobile_phone_id = offer["classified_mobile_phone_id"]
        week = datetime.strptime(offer["date"], "%d-%m-%Y").isocalendar()[1]
        if (today.isocalendar()[1]) == week:
            continue  # Skip current week until it is over
        week -= 1  # Current week starts in 1 in isocalendar
        currency = offer["currency"]
        idx = (offer_mobile_phone_id, week, currency)
        offers_by_link = grouped_offers.get(idx, {})
        offer_link = offer['link']
        if offer_link in offers_by_link:
            existing_offer = offers_by_link[offer_link]
            if existing_offer['date'] < offer['date']:
                offers_by_link[offer_link] = offer
        else:
            offers_by_link[offer_link] = offer
        grouped_offers[idx] = offers_by_link

    grouped_prices = {}
    for idx, offers_by_link in grouped_offers.items():
        prices = [offer['amount'] for offer in offers_by_link.values()]
        grouped_prices[idx] = prices

    weekly_price_summary = []
    for (phone_id, week, currency), prices in grouped_prices.items():
        price_summary = {
            "phone_id": phone_id,
            "week_of_year": week,
            "currency": currency,
            # TODO: Mover year to column in data
            "end_of_week": datetime.strptime(f'2020-{week}-0', "%Y-%W-%w").strftime("%d-%m-%Y"),
        }
        price_statistics = get_statistics(prices)
        price_summary.update(price_statistics)
        weekly_price_summary.append(price_summary)

    # Upsert prices in weekly phone prices
    if weekly_price_summary:
        database.upsert_many(database.weekly_phone_price, weekly_price_summary, [
                             'phone_id', 'week_of_year', 'currency'])
        logger.info("Weekly prices updated")
    else:
        logger.warn(
            "Nothing to summarize in weekly prices, are there no offers matching phones with good scores?")

    logger.info("Collecting prices per phone for the last 7 days")

    generate_last_7_days_prices(offer_history)


def generate_last_7_days_prices(offer_history):
    # Dedup multiple offers with same link, keeping last price in for the link in the last 7 days
    offers_by_link = {}
    today = datetime.utcnow()
    for offer in offer_history:
        if not offer['visible_classification']:
            continue  # Ignore offers that have low classification score to avoid noise in the data
        offer_date = datetime.strptime(offer["date"], "%d-%m-%Y")
        if (today - offer_date).days > 7:
            continue
        offer_link = offer['link']
        if offer_link in offers_by_link:
            existing_offer = offers_by_link[offer_link]
            if existing_offer['date'] < offer['date']:
                offers_by_link[offer_link] = offer
        else:
            offers_by_link[offer_link] = offer

    # Get amounts by currency
    grouped_amounts = {}
    for offer in offers_by_link.values():
        currency = offer['currency']
        phone_id = offer['classified_mobile_phone_id']
        idx = (currency, phone_id)
        amounts = grouped_amounts.get(idx, [])
        amounts.append(offer['amount'])
        grouped_amounts[idx] = amounts

    # Calculate summary
    last_7_days_summary = []
    for (currency, phone_id), amounts in grouped_amounts.items():
        price_statistics = get_statistics(amounts)
        price_summary = {
            "phone_id": phone_id,
            "week_of_year": 'LATEST_7_DAYS',
            "currency": currency,
            # Not actual end of week, but end of the last 7 days "week"
            "end_of_week": today.strftime("%d-%m-%Y"),
        }
        price_summary.update(price_statistics)
        last_7_days_summary.append(price_summary)

    # Clear and insert price summary for last 7 days
    if last_7_days_summary:
        logger.info(
            f"Inserting {len(last_7_days_summary)} summaries for last 7 days")
        database.weekly_phone_price.delete_many(
            {'week_of_year': 'LATEST_7_DAYS'})
        database.insert_many_ignore_duplicates(
            database.weekly_phone_price, last_7_days_summary)
        logger.info("Last 7 days prices regenerated")
    else:
        logger.warn("No prices found in the last 7 days. Keeping old data")


def get_statistics(numbers):
    return {
        "min": min(numbers),
        "max": max(numbers),
        "mean": statistics.mean(numbers),
        "median": statistics.median(numbers),
        "st_deviation": statistics.stdev(numbers) if len(numbers) > 1 else 0,
        "count": len(numbers)
    }


if __name__ == "__main__":
    main()

python -m mercadolibre.fetch &&
python -m mercadolibre.clean &&
python -m ebay.fetch &&
python -m ebay.clean &&
python -m model_matcher.model_price_matcher &&
python -m price_updates.weekly_price_summary &&
python -m price_updates.mobile_price_updater &&
python -m extra_phone_data.update_sold_count
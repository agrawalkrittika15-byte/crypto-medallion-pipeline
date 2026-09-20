SELECT
    date,
    coin_id,
    upper(symbol) AS symbol,
    name,
    market_cap_rank,
    current_price_usd,
    market_cap_usd,
    total_volume_usd,
    high_24h,
    low_24h,
    price_change_pct_24h,
    price_change_pct_7d,
    price_change_pct_30d,
    circulating_supply,
    total_supply,
    max_supply,
    ath,
    ath_change_pct,
    ath_date,
    atl,
    atl_change_pct,
    atl_date
FROM crypto_pipeline.bronze_crypto_snapshot
WHERE current_price_usd IS NOT NULL
  AND market_cap_rank IS NOT NULL

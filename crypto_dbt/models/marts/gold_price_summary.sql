SELECT
    symbol,
    name,
    category,
    market_cap_tier,
    avg(price) AS avg_price,
    min(price) AS min_price,
    max(price) AS max_price,
    max(price) - min(price) AS price_range,
    count(*) AS tick_count,
    max(event_time) AS last_seen
FROM {{ ref('silver_ticks') }}
GROUP BY symbol, name, category, market_cap_tier

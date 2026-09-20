SELECT
    category,
    market_cap_tier,
    count(*) AS coin_count,
    groupArray(symbol) AS symbols
FROM {{ ref('silver_baserow_coins') }}
GROUP BY category, market_cap_tier
ORDER BY category, market_cap_tier
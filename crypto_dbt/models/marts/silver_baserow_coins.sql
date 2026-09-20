SELECT
    row_id,
    name,
    upper(symbol) AS symbol,
    category,
    market_cap_tier
FROM {{ ref('stg_baserow_coins') }}
GROUP BY row_id, name, symbol, category, market_cap_tier
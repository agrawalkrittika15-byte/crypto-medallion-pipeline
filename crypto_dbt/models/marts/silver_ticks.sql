SELECT
    t.symbol,
    c.name,
    c.category,
    c.market_cap_tier,
    t.price,
    t.volume,
    t.event_time
FROM {{ ref('stg_bronze_ticks') }} AS t
LEFT JOIN crypto_pipeline.coins_dim AS c
    ON t.symbol = c.symbol

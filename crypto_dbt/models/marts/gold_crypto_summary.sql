WITH ranked AS (
    SELECT
        *,
        row_number() OVER (ORDER BY price_change_pct_24h DESC) AS gain_rank,
        row_number() OVER (ORDER BY price_change_pct_24h ASC) AS loss_rank
    FROM {{ ref('silver_crypto') }}
)
SELECT
    coin_id,
    symbol,
    name,
    market_cap_rank,
    current_price_usd,
    market_cap_usd,
    total_volume_usd,
    price_change_pct_24h,
    price_change_pct_7d,
    category,
    market_cap_tier,
    CASE
        WHEN gain_rank <= 10 THEN 'top_gainer'
        WHEN loss_rank <= 10 THEN 'top_loser'
        ELSE 'mid'
    END AS movement_flag
FROM ranked
ORDER BY market_cap_rank ASC
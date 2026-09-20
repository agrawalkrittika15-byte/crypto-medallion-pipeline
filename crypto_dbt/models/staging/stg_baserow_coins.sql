SELECT
    row_id,
    name,
    symbol,
    category,
    market_cap_tier
FROM crypto_pipeline.bronze_baserow_coins
WHERE name IS NOT NULL
  AND name != ''

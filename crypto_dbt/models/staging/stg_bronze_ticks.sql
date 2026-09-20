SELECT
    symbol,
    price,
    volume,
    event_time,
    ingested_at
FROM crypto_pipeline.bronze_ticks

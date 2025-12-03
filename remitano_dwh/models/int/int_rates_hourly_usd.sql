{{ config(materialized='view') }}

WITH base AS (
    SELECT
        symbol,
        open_time,
        close,
        REPLACE(symbol, 'USDT', '') AS base_currency
    FROM {{ ref('stg_rates') }}
),

dedup AS (
    -- Nếu có trùng open_time ở nhiều file, chọn record duy nhất
    SELECT
        base_currency,
        open_time,
        close AS price_usd
    FROM base
)

SELECT * FROM dedup;

{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'rates') }}
),

cleaned AS (
    SELECT
        UPPER(symbol)                                         AS symbol,
        CAST(open_time AS TIMESTAMP)                          AS open_time,
        CAST(close_time AS TIMESTAMP)                         AS close_time,
        CAST(open AS FLOAT)                                   AS open,
        CAST(high AS FLOAT)                                   AS high,
        CAST(low AS FLOAT)                                    AS low,
        CAST(close AS FLOAT)                                  AS close,
        CAST(volume AS FLOAT)                                 AS volume,
        CAST(quote_asset_volume AS FLOAT)                     AS quote_asset_volume,
        CAST(number_of_trades AS INTEGER)                     AS number_of_trades,
        CAST(taker_buy_base_asset_volume AS FLOAT)            AS taker_buy_base_asset_volume,
        CAST(taker_buy_quote_asset_volume AS FLOAT)           AS taker_buy_quote_asset_volume
    FROM src
)

SELECT * FROM cleaned;

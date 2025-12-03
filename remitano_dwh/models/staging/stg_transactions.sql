{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
),

cleaned AS (
    SELECT
        CAST(txn_id AS BIGINT)                        AS txn_id,
        CAST(user_id AS BIGINT)                       AS user_id,
        UPPER(status)                                 AS status,
        UPPER(source_currency)                        AS source_currency,
        UPPER(destination_currency)                   AS destination_currency,
        CAST(source_amount AS FLOAT)                  AS source_amount,
        CAST(destination_amount AS FLOAT)             AS destination_amount,
        CAST(created_at AS TIMESTAMP)                 AS created_at,

        -- Chuẩn hóa về giờ để join với rates (vì Binance hiệu lực theo giờ)
        DATE_TRUNC('hour', CAST(created_at AS TIMESTAMP)) AS transaction_hour
    FROM src
)

SELECT * FROM cleaned;

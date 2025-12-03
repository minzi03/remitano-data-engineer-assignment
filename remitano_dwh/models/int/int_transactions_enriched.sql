{{ config(materialized='view') }}

WITH tx AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

rates AS (
    SELECT * FROM {{ ref('int_rates_hourly_usd') }}
),

kyc_hist AS (
    SELECT * FROM {{ ref('int_user_kyc_history') }}
),

tx_with_rates AS (
    SELECT
        t.txn_id,
        t.user_id,
        t.status,
        t.source_currency,
        t.destination_currency,
        t.source_amount,
        t.destination_amount,
        t.created_at,
        t.transaction_hour,

        r.price_usd AS dest_price_usd,
        (t.destination_amount * r.price_usd) AS destination_amount_usd

    FROM tx t
    LEFT JOIN rates r
        ON UPPER(t.destination_currency) = UPPER(r.base_currency)
       AND t.transaction_hour = r.open_time
),

tx_with_kyc AS (
    SELECT
        txr.*,
        k.kyc_level AS kyc_level_at_txn
    FROM tx_with_rates txr
    LEFT JOIN kyc_hist k
        ON txr.user_id = k.user_id
       AND txr.created_at >= k.kyc_valid_from
       AND (k.kyc_valid_to IS NULL OR txr.created_at < k.kyc_valid_to)
)

SELECT * FROM tx_with_kyc;

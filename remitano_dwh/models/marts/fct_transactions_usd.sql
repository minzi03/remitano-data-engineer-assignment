{{ config(materialized='table') }}

WITH base AS (

    SELECT * 
    FROM {{ ref('int_transactions_enriched') }}

),

completed AS (

    SELECT
        txn_id,
        user_id,
        status,
        source_currency,
        destination_currency,
        source_amount,
        destination_amount,
        destination_amount_usd,
        kyc_level_at_txn,

        created_at,
        transaction_hour,

        -- Date grains cho BI
        CAST(created_at AS DATE)                       AS transaction_date,
        DATE_TRUNC('month', created_at)                AS transaction_month,
        DATE_TRUNC('quarter', created_at)              AS transaction_quarter

    FROM base
    WHERE UPPER(status) = 'COMPLETED'

)

SELECT * FROM completed;

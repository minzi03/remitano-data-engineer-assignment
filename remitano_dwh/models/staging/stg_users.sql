{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw', 'users') }}
),

cleaned AS (
    SELECT
        CAST(user_id AS BIGINT)               AS user_id,
        CAST(kyc_level AS INTEGER)            AS kyc_level,
        CAST(created_at AS TIMESTAMP)         AS created_at,
        CAST(updated_at AS TIMESTAMP)         AS updated_at
    FROM src
)

SELECT * FROM cleaned;

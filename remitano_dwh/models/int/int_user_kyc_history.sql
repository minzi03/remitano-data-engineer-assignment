{{ config(materialized='view') }}

-- Base user info
WITH base AS (
    SELECT
        user_id,
        kyc_level,
        created_at,
        updated_at
    FROM {{ ref('stg_users') }}
),

-- Vì bài test không có lịch sử thay đổi,
-- ta mô phỏng SCD Type 2 bằng cách coi mỗi user có 1 record với:
-- kyc_valid_from = created_at
-- kyc_valid_to = updated_at (hoặc null nếu muốn coi là current)
scd2 AS (
    SELECT
        user_id,
        kyc_level,
        created_at AS kyc_valid_from,
        updated_at AS kyc_valid_to
    FROM base
)

SELECT * FROM scd2;

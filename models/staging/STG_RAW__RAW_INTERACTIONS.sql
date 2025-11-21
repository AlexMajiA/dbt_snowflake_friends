{{
  config(
    materialized='table'
  )
}}

WITH raw_episode_rating_source AS (
    SELECT *
    FROM {{ source('raw', 'raw_episode_rating') }}
),

episode_cleaned AS (
    SELECT
        MD5(CONCAT(epseason, '-', epnum)) AS episode_id,
        CAST(epseason AS INTEGER) AS season,
        CAST(epnum AS INTEGER) AS episode_number,
        rating,
        TRIM(dynamics) AS combination_code
    FROM raw_episode_rating_source
    WHERE epseason IS NOT NULL
      AND epnum IS NOT NULL
      AND dynamics IS NOT NULL
)

SELECT
    MD5(CONCAT(episode_id, '-', combination_code)) AS id_dynamics,
    episode_id,
    combination_code,
    rating,
    season,
    episode_number
FROM episode_cleaned
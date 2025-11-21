{{
  config(
    materialized='table'
  )
}}

WITH raw_episode_meta_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_episode_meta') }}
),

views_cleaned AS (
    SELECT

        MD5(CONCAT(SEASON, '-', EPISODE_NUMBER)) AS id_episode,

        CAST(NULLIF(REPLACE(STARS, ',', '.'), '') AS FLOAT) AS stars,
        CAST(NULLIF(VOTES, '') AS INTEGER) AS votes,
        CAST(NULLIF(REPLACE(US_VIEWS_MILLIONS, ',', '.'), '') AS FLOAT) AS us_views_millions
        
    FROM raw_episode_meta_source
    WHERE SEASON IS NOT NULL 
        AND EPISODE_NUMBER IS NOT NULL
)

SELECT * FROM views_cleaned
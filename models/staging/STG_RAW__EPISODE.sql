{{
  config(
    materialized='table',
    unique_key='id_episode'
  )
}}

WITH raw_episode_meta_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_episode_meta') }}
),

episodes_cleaned AS (
    SELECT
        MD5(CONCAT(SEASON, '-', EPISODE_NUMBER)) AS id_episode,
        MD5(TRIM(UPPER(DIRECTOR))) AS id_director,

        CAST(SEASON AS INTEGER) AS season,
        CAST(EPISODE_NUMBER AS INTEGER) AS episode_number,
        CAST(YEAR_OF_PROD AS INTEGER) AS year,
        TRIM(EPISODE_TITLE) AS title,
        CAST(DURATION AS INTEGER) AS duration,
        TRIM(SUMMARY) AS summary,
        
        CAST(NULLIF(replace(STARS, ',', '.'), '') as FLOAT) AS STARS,
        CAST(NULLIF(VOTES, '') as INTEGER) AS VOTES ,
        CAST(NULLIF(replace(US_VIEWS_MILLIONS, ',', '.'), '') as FLOAT) AS US_VIEWS_MILLIONS

    FROM raw_episode_meta_source

    WHERE YEAR_OF_PROD IS NOT NULL
        AND SEASON IS NOT NULL
        AND EPISODE_NUMBER IS NOT NULL
        AND EPISODE_TITLE IS NOT NULL
        AND DURATION IS NOT NULL
        AND DIRECTOR IS NOT NULL
)

SELECT * FROM episodes_cleaned
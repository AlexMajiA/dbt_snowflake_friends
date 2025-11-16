
{{
  config(
    materialized='view'
  )
}}

WITH raw_episode_meta_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_episode_meta') }}
    ),

raw_episode_meta_cleaned AS (
    SELECT
        cast(YEAR_OF_PROD as INTEGER) as YEAR_OF_PROD,
        cast(SEASON as INTEGER) as SEASON,
        cast(EPISODE_NUMBER as INTEGER) as EPISODE_NUMBER,
        trim(EPISODE_TITLE) as EPISODE_TITLE,
        cast(DURATION as INTEGER) as DURATION_MINUTES,
        trim(SUMMARY) as SUMMARY,
        trim(DIRECTOR) as DIRECTOR,
        cast(NULLIF(replace(STARS, ',', '.'), '') as FLOAT) AS STARS,
        cast(NULLIF(VOTES, '') as INTEGER) AS VOTES ,
        cast(NULLIF(replace(US_VIEWS_MILLIONS, ',', '.'), '') as FLOAT) AS US_VIEWS_MILLIONS,

        CURRENT_TIMESTAMP() as processed_at

    FROM raw_episode_meta_source
        WHERE YEAR_OF_PROD IS NOT NULL
            AND SEASON IS NOT NULL
            AND EPISODE_NUMBER IS NOT NULL
            AND EPISODE_TITLE IS NOT NULL
            AND DURATION IS NOT NULL
            AND DIRECTOR IS NOT NULL
    )

SELECT * FROM raw_episode_meta_cleaned
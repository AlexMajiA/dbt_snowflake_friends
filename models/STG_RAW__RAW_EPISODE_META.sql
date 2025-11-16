
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
        YEAR_OF_PROD ,
        SEASON ,
        EPISODE_NUMBER ,
        EPISODE_TITLE ,
        DURATION ,
        SUMMARY,
        DIRECTOR ,
        STARS ,
        VOTES ,
        US_VIEWS_MILLIONS 

    FROM raw_episode_meta_source
    )

SELECT * FROM raw_episode_meta_cleaned
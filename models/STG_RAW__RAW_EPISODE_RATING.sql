
{{
  config(
    materialized='view'
  )
}}

WITH episode_rating_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_episode_meta') }}
    ),

raw_episode_rating_cleaned AS (
    SELECT
    EPSEASON ,
    EPNUM ,
    EPNAME ,
    RATING ,
    DYNAMICS 
    
    FROM episode_rating_source
    )

SELECT * FROM raw_episode_rating_cleaned
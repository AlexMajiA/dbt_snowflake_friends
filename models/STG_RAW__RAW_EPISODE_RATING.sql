
{{
  config(
    materialized='view'
  )
}}

WITH episode_rating_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_episode_rating') }}
    ),

raw_episode_rating_cleaned AS (
    SELECT
        cast(EPSEASON as INTEGER) as EPSEASON,
        cast(EPNUM as INTEGER) as EPNUM,
        trim(EPNAME) as EPNAME,
        cast(NULLIF(RATING, '') as FLOAT) as RATING ,
        cast(DYNAMICS as INTEGER) as DYNAMICS, 
        
        CURRENT_TIMESTAMP() as processed_at

    FROM episode_rating_source
        WHERE EPSEASON IS NOT NULL
            AND EPNUM IS NOT NULL

    )

SELECT * FROM raw_episode_rating_cleaned

--Falta crear una tabla para dynamics, donde se relacionen con actores 

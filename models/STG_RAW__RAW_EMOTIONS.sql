
{{
  config(
    materialized='view'
  )
}}

WITH raw_emotions_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_emotions') }}
    ),

raw_emotions_cleaned AS (
    SELECT
        cast(SEASON as INTEGER) as SEASON,
        cast(EPISODE as INTEGER) as EPISODE,
        cast(SCENE as INTEGER) as SCENE,
        cast(UTTERANCE as INTEGER) as UTTERANCE,
        trim(EMOTION) as EMOTION,
        trim(SPEAKER)as SPEAKER,

        CURRENT_TIMESTAMP() as processed_at

    FROM raw_emotions_source
        WHERE SEASON IS NOT NULL
            AND EPISODE IS NOT NULL
            AND SCENE IS NOT NULL
            AND UTTERANCE IS NOT NULL
            AND EMOTION IS NOT NULL
            
    )

SELECT * FROM raw_emotions_cleaned
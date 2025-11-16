
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
        SEASON ,
        EPISODE ,
        SCENE ,
        UTTERANCE ,
        EMOTION ,
        SPEAKER 

    FROM raw_emotions_source
    )

SELECT * FROM raw_emotions_cleaned
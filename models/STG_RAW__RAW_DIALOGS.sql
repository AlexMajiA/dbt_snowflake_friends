
{{
  config(
    materialized='view'
  )
}}

WITH raw_dialogs_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_dialogs') }}
    ),

raw_dialogs_cleaned AS (
    SELECT
        SEASON ,
        EPISODE,
        SCENE ,
        UTTERANCE ,
        SPEAKER ,
        TEXT ,
        TOKENS 

    FROM raw_dialogs_source
    )

SELECT * FROM raw_dialogs_cleaned

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
        CAST(SEASON AS INTEGER) as SEASON ,
        CAST(EPISODE AS INTEGER) as EPISODE,
        CAST(SCENE AS INTEGER) as SCENE,
        CAST(UTTERANCE AS INTEGER) as UTTERANCE,
        TRIM(SPEAKER) AS SPEAKER ,
        TRIM(TEXT) AS TEXT ,

        --Tokens convertidos a un formato que pueda usar en la capa intermedia para hacer análisis. 
        PARSE_JSON(TOKENS) AS TOKENS_ARRAY,

        --Conteo de elementos del array, solo muestra cuantos tokens tiene para no cambiar la granularidad.
        ARRAY_SIZE(PARSE_JSON(TOKENS)) AS TOKEN_COUNT,
        CURRENT_TIMESTAMP() as processed_at

    FROM raw_dialogs_source
    WHERE SEASON IS NOT NULL
        AND EPISODE IS NOT NULL
        AND SCENE IS NOT NULL
        AND UTTERANCE IS NOT NULL
    )

SELECT * FROM raw_dialogs_cleaned
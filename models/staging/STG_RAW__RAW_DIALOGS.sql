
{{
  config(
    materialized='table'
  )
}}

WITH raw_dialogs_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_dialogs') }}
    ),

raw_dialogs_cleaned AS (
    SELECT

        md5(CONCAT(season, '-', episode, '-', scene, '-', utterance)) AS dialog_id,

        --FK al episodio
        MD5(CONCAT(season, '-', episode)) AS episode_id,

        CAST(SEASON AS INTEGER) as season,
        CAST(EPISODE AS INTEGER) as episode,
        CAST(SCENE AS INTEGER) as scene,
        CAST(UTTERANCE AS INTEGER) as utterance,

        --FK a utterance
        MD5(TRIM(UPPER(speaker))) AS speaker_id,

        TRIM(TEXT) AS text ,

        --Indico si el texto esta limpio con "0"
        CASE 
            WHEN text IS NULL OR TRIM(text) = '' OR tokens IS NULL OR TRIM(tokens) = '' THEN 0
            ELSE 1
        END AS cleaned_flag,


        --Tokens convertidos a un formato que pueda usar en la capa intermedia para hacer análisis. 
        PARSE_JSON(TOKENS) AS tokens_array,

        --Conteo de elementos del array, solo muestra cuantos tokens tiene para no cambiar la granularidad.
        ARRAY_SIZE(PARSE_JSON(TOKENS)) AS token_count,

        --Metadata
        CURRENT_TIMESTAMP() as processed_at

    FROM raw_dialogs_source
    WHERE season IS NOT NULL
        AND episode IS NOT NULL
        AND scene IS NOT NULL
        AND utterance IS NOT NULL
    )

SELECT * FROM raw_dialogs_cleaned
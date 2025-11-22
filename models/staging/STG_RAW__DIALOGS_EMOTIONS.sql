
{{
    config(
    materialized='table'
    )
}}

WITH raw_emotions_source AS (
    SELECT *
    FROM {{ source('raw', 'raw_emotions') }}
),

cleaned AS (
    SELECT

        --Hago el mismo formato que en stg_dialogs para poder unirlos después
        MD5(CONCAT(season, '-', episode, '-', scene, '-', utterance)) AS dialog_id,

        --FK a la emoción
        MD5(TRIM(UPPER(emotion))) AS emotion_id,

        --FK al speaker
        MD5(TRIM(UPPER(speaker))) AS speaker_id,

        CAST(season AS INTEGER) AS season,
        CAST(episode AS INTEGER) AS episode,
        CAST(scene AS INTEGER) AS scene,
        CAST(utterance AS INTEGER) AS utterance,

        CURRENT_TIMESTAMP() AS processed_at

    FROM raw_emotions_source
    WHERE season IS NOT NULL
      AND episode IS NOT NULL
      AND scene IS NOT NULL
      AND utterance IS NOT NULL
      AND emotion IS NOT NULL
      AND speaker IS NOT NULL
)

SELECT * FROM cleaned

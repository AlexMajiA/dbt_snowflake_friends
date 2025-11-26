{{ 
    config(
    materialized='table'
    ) 
}}

WITH src AS (
    SELECT *
    FROM {{ source('raw', 'raw_interactions') }}
),

clean AS (
    SELECT
        -- ID de interacción
        MD5(CONCAT(season, '-', episode, '-', character1, '-', character2)) AS interaction_id,

        MD5(CONCAT(season, '-', episode)) AS episode_id,

        CAST(season AS INTEGER) AS season,
        CAST(episode AS INTEGER) AS episode_number,

        -- IDs de personaje 
        MD5(UPPER(TRIM(character1))) AS character1_id,
        MD5(UPPER(TRIM(character2))) AS character2_id,

        CURRENT_TIMESTAMP() AS processed_at
    FROM src
    WHERE season IS NOT NULL
      AND episode IS NOT NULL
      AND character1 IS NOT NULL
      AND character2 IS NOT NULL
)

SELECT * FROM clean

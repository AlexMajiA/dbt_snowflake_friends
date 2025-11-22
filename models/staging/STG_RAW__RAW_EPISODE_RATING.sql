{{ 
    config(
        materialized='table'
    ) 
}}

WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_episode_rating') }}
),

cleaned AS (
    SELECT
        -- ID consistente con todos los episodios (estándar para toda Silver)
        MD5(
            CONCAT(TRIM(CAST(EPSEASON AS STRING)),'-', TRIM(CAST(EPNUM AS STRING)))
        ) AS episode_id,

        CAST(EPSEASON AS INTEGER) AS season,
        CAST(EPNUM AS INTEGER) AS episode_number,
        TRIM(EPNAME) AS title,

        CAST(NULLIF(RATING, '') AS FLOAT) AS rating,
        CAST(NULLIF(DYNAMICS, '') AS INTEGER) AS dynamics_code,

        CURRENT_TIMESTAMP() AS processed_at
    FROM source_data
    WHERE EPSEASON IS NOT NULL
      AND EPNUM IS NOT NULL
)

SELECT * FROM cleaned




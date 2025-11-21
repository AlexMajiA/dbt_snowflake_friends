
{{
  config(
    materialized='view',
    unique_key='interaction_id'
  )
}}

WITH raw_interactions_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_interactions') }}
),

raw_interactions_cleaned AS (
    SELECT
        --ID único para la interacción
        MD5(CONCAT(season, '-', episode, '-', character1, '-', character2)) AS interaction_id,
        
        --FK al episodio
        MD5(CONCAT(season, '-', episode)) AS episode_id,
        
        --FKs a los speakers
        MD5(TRIM(UPPER(character1))) AS speaker1_id,
        MD5(TRIM(UPPER(character2))) AS speaker2_id,
        
        --Lo añado por si fallan los id y quiero ver datos originales y para relaciones directas sin joins.
        CAST(season AS INTEGER) AS season,
        CAST(episode AS INTEGER) AS episode,
        TRIM(character1) AS character1_name,
        TRIM(character2) AS character2_name
        
    FROM raw_interactions_source
    WHERE season IS NOT NULL
      AND episode IS NOT NULL
      AND character1 IS NOT NULL
      AND character2 IS NOT NULL
      AND character1 != character2 --Con esto, evito que sea el mismo.
)

SELECT * FROM raw_interactions_cleaned
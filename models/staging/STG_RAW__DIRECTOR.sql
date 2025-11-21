{{
  config(
    materialized='table',
    unique_key='id_director'
  )
}}

WITH raw_episode_meta_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_episode_meta') }}
),

directors_cleaned AS (
    SELECT DISTINCT

        MD5(TRIM(UPPER(DIRECTOR))) AS id_director,
        TRIM(DIRECTOR) AS name
        
    FROM raw_episode_meta_source
    WHERE DIRECTOR IS NOT NULL
        AND TRIM(DIRECTOR) != ''
)

SELECT * FROM directors_cleaned
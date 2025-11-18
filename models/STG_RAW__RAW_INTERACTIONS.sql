
{{
  config(
    materialized='view'
  )
}}

WITH raw_interactions_source AS (
    SELECT * 
    FROM {{ source('raw', 'raw_interactions') }}
    ),

raw_interactions_cleaned AS (
    SELECT
        cast(SEASON as INTEGER) as SEASON,
        cast(EPISODE as INTEGER) as EPISODE,
        trim(CHARACTER1) as CHARACTER1,
        trim(CHARACTER2) as CHARACTER2
    
    FROM raw_interactions_source
    )

SELECT * FROM raw_interactions_cleaned
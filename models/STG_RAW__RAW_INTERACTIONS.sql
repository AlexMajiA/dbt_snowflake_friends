
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
    SEASON ,
    EPISODE ,
    CHARACTER1 ,
    CHARACTER2
    
    FROM raw_interactions_source
    )

SELECT * FROM raw_interactions_cleaned
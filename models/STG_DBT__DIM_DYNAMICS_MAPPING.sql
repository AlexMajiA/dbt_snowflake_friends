{{ 
    config(materialized='table')
 }}

SELECT
    cast(dynamics_code AS INTEGER) AS dynamics_code,
    trim(dynamics_name) AS dynamics_name,
    trim(replace(characters_involved, ';', ', ')) AS characters_involved,
    cast(character_count as integer) AS character_count,
    trim(category) AS category


FROM {{ ref('dim_dynamics_mapping') }}

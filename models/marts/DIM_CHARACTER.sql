{{ config(
    materialized = 'table',
    unique_key   = 'id_character'
) }}

with base as (
    select
        id_character,
        speaker_name as character_name
    from {{ ref('STG_RAW__RAW_EMOTIONS') }}
),

character_distinct as (
    select distinct
        id_character,
        character_name
    from base

)

select *
from character_distinct
order by character_name

{{ config(
    materialized = 'table',
    unique_key   = 'id_emotion'
    )
}}

with base as (
    select

        id_emotion,
        emotion_name
        
    from {{ ref('STG_RAW__RAW_EMOTIONS') }}
),
emotion_distinct as (

    select distinct
        id_emotion,
        emotion_name
    from base
)
select *
from emotion_distinct
order by emotion_name

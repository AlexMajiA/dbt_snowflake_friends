{{ config(
    materialized = 'table',
    unique_key   = 'id_scene'
) }}

with base as (
    select
        id_scene,
        id_episode,
        season,
        episode,
        scene_number
    from {{ ref('STG_RAW__RAW_EMOTIONS') }}
),

scene_distinct as (
    select distinct

        id_scene,
        id_episode,
        season,
        episode,
        scene_number
    from base
)

select *
from scene_distinct
order by season, episode, scene_number

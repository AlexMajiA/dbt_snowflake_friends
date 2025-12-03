{{ config(
    materialized = 'table',
    unique_key   = 'id_event',
    tags         = ['gold', 'fact_emotions']
) }}

with stg as (
    select
        id_event,
        id_episode,
        id_scene,
        id_character,
        id_emotion,
        season,
        episode,
        scene_number,
        utterance_number,
        processed_at
    from {{ ref('STG_RAW__RAW_EMOTIONS') }}
),

validated as (
    select
        s.id_event,

        --Claves hacia dimensiones
        s.id_episode,
        s.id_scene,
        s.id_character,
        s.id_emotion,

        --Atributos propios del hecho
        s.season,
        s.episode,
        s.scene_number,
        s.utterance_number,
        s.processed_at

    from stg s
    inner join {{ ref('DIM_EPISODE') }}   d_ep 
        on s.id_episode   = d_ep.id_episode
    inner join {{ ref('DIM_SCENE') }}     d_sc 
        on s.id_scene     = d_sc.id_scene
    inner join {{ ref('DIM_CHARACTER') }} d_ch 
        on s.id_character = d_ch.id_character
    inner join {{ ref('DIM_EMOTION') }}   d_em 
        on s.id_emotion   = d_em.id_emotion
)

select *
from validated
order by season, episode, scene_number, utterance_number



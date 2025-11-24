{{ config(
    materialized = 'table',
    unique_key = 'id_event'
) }}

with raw as (
    select

        cast(season as integer)         as season,
        cast(episode as integer)        as episode,
        cast(scene as integer)          as scene_number,
        cast(utterance as integer)      as utterance_number,
        trim(emotion)                   as emotion_name,
        trim(speaker)                   as speaker_name

    from {{ source('raw', 'raw_emotions') }}
    where season is not null
      and episode is not null
      and scene is not null
      and utterance is not null
      and emotion is not null
      and speaker is not null
),

with_ids as (
    select

        md5( concat_ws('-', season, episode) ) as id_episode,
        md5( concat_ws('-', season, episode, scene_number) ) as id_scene,
        md5( upper(speaker_name) ) as id_character,
        md5( upper(emotion_name) ) as id_emotion,

        md5(concat_ws('-',season, episode, scene_number, utterance_number, upper(speaker_name), upper(emotion_name))
        ) as id_event,

        *
    from raw
)

select * from with_ids


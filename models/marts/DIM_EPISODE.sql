{{ config(materialized='table') }}

with episode_base as (

    select
        id_episode,
        id_director,
        season,
        episode_number,
        year,
        title,
        duration,
        summary
    from {{ ref('STG_RAW__EPISODE') }}

),

--Elimino duplicados si los hubiera
dedup as (
    select distinct * from episode_base
)

select * from dedup

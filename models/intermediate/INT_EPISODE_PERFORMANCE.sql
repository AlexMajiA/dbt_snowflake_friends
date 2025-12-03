{{ 
    config(
        materialized='incremental',
        unique_key='id_episode',
        tags=['incremental']
    ) 
}}

with episode_source as (

    select
        id_episode,
        id_director,
        season,
        episode_number,
        year,
        title,
        duration,
        summary,
        stars,
        votes,
        us_views_millions,

        current_timestamp() as ingest_timestamp

    from {{ ref('STG_RAW__EPISODE') }}

),

episode_enriched as (

    select
        id_episode,
        id_director,
        season,
        episode_number,
        year,
        title,
        duration,
        summary,
        stars,
        votes,
        us_views_millions,

        ingest_timestamp,

        case
            when stars >= 8 then 'High-rated'
            when stars >= 6 then 'Medium-rated'
            else 'Low-rated'
        end as rating_category

    from episode_source
),
episode_dedup as (

    select *
    from episode_enriched
    qualify row_number() over (
        partition by id_episode
        order by ingest_timestamp desc
    ) = 1
)

select *
from episode_dedup

{% if is_incremental() %}

    --SOLO INSERTo FILAS MÁS RECIENTES QUE LO ÚLTIMO CARGADO
    where ingest_timestamp > (select max(ingest_timestamp) from {{ this }})

{% endif %}
























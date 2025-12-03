{{ config(
    materialized = 'table',
    unique_key   = 'id_episode',
    tags         = ['gold', 'fact_episode_performance']
) }}

--Obtengo los datos de la última versión del snapshot
with snapshot_latest as (

    select *
    from (
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
            dbt_scd_id,
            dbt_valid_from,
            dbt_valid_to,
            row_number() over (
                partition by id_episode
                order by dbt_valid_from desc
            ) as rn
        from {{ ref('episode_meta_snapshot') }}
    )
    where rn = 1
),

--Hago injesta simulada
incremental_latest as (

    select *
    from (
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
            row_number() over (
                partition by id_episode
                order by ingest_timestamp desc
            ) as rn
        from {{ ref('INT_EPISODE_PERFORMANCE') }}
    )
    where rn = 1
),

final_metrics as (

    select
        e.id_episode,
        e.id_director,

        coalesce(i.stars, s.stars) as stars,
        coalesce(i.votes, s.votes) as votes,
        coalesce(i.us_views_millions, s.us_views_millions) as us_views_millions,

        s.dbt_valid_from as snapshot_valid_from

    from {{ ref('DIM_EPISODE') }} e
    left join snapshot_latest   s on e.id_episode = s.id_episode
    left join incremental_latest i on e.id_episode = i.id_episode
    left join {{ ref('DIM_DIRECTOR') }} d on e.id_director = d.id_director
),

--Rankings usando la macro calcular_ranking()
with_rankings as (

    select
        id_episode,
        id_director,
        stars,
        votes,
        us_views_millions,
        snapshot_valid_from,

        {{ calcular_ranking("votes") }} as rank_votes,
        {{ calcular_ranking("stars") }} as rank_stars,
        {{ calcular_ranking("us_views_millions") }} as rank_views

    from final_metrics
)

select
    id_episode,
    id_director,
    stars,
    votes,
    us_views_millions,
    snapshot_valid_from,
    rank_votes,
    rank_stars,
    rank_views
from with_rankings 



















-- ============================================================================
-- FACT TABLE: EPISODE PERFORMANCE (GOLD)
--
-- Esta fact está modelada EN ESTRELLA:
--   - Depende de DIM_EPISODE (id_episode)
--   - Depende de DIM_DIRECTOR (id_director)
--
-- Las métricas provienen de:
--   1. Snapshot SCD2  -> Datos históricos consolidados
--   2. Incremental    -> Ingestas simuladas (datos más recientes)
--
-- Reglas de combinación:
--   - Si existe dato incremental → tiene prioridad
--   - Si no existe → usar snapshot
--
-- NOTA: Esta fact ya no toma metadata del staging.
--       La metadata está en las DIMs, como debe ser.
-- ============================================================================

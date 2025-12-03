--Episodios muy vistos pero con rating bajo, ordenados por visitas
{{ config(
    materialized = 'table',
    tags = ['datamart', 'episode_performance']
) }}

with stats as (
    select
        avg(us_views_millions) as avg_views,
        avg(stars)            as avg_rating
    from {{ ref('FCT_EPISODE_PERFORMANCE') }}
),

filtered as (
    select
        e.title,
        e.season,
        e.episode_number,
        f.stars as rating,
        f.us_views_millions as views
    from {{ ref('FCT_EPISODE_PERFORMANCE') }} f
    join {{ ref('DIM_EPISODE') }} e
        on f.id_episode = e.id_episode
    cross join stats s
    where f.us_views_millions > s.avg_views
      and f.stars < s.avg_rating
)

select *
from filtered
order by views desc

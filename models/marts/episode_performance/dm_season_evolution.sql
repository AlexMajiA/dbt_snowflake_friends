--Evolución del rating por temporada
{{ config(
    materialized = 'table',
    tags = ['datamart', 'episode_performance']
) }}

with base as (
    select
        d.season,
        avg(f.stars) as avg_rating,
        avg(f.votes) as avg_votes,
        avg(f.us_views_millions) as avg_views_millions
    from {{ ref('FCT_EPISODE_PERFORMANCE') }} f
    join {{ ref('DIM_EPISODE') }} d
        on f.id_episode = d.id_episode
    group by d.season
)

select
    season,
    round(avg_rating, 2) as avg_rating,
    round(avg_votes, 2) as avg_votes,
    round(avg_views_millions, 2) as avg_views_millions,
    dense_rank() over (order by avg_rating desc) as season_rank
from base
order by season

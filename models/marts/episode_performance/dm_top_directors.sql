
--Directores con mejor rendimiento
{{ config(
    materialized = 'view',
    tags = ['datamart', 'episode_performance']
) }}

with base as (
    select
        d.director_name,
        avg(f.stars) as avg_rating,
        avg(f.votes) as avg_votes,
        avg(f.us_views_millions) as avg_views,
        count(*) as episodes_directed
    from {{ ref('FCT_EPISODE_PERFORMANCE') }} f
    join {{ ref('DIM_DIRECTOR') }} d
        on f.id_director = d.id_director
    group by d.director_name
)

select
    director_name,
    round(avg_rating, 2) as avg_rating,
    round(avg_votes, 2) as avg_votes,
    round(avg_views, 2) as avg_views,
    episodes_directed
from base
where episodes_directed >= 2
order by avg_rating desc


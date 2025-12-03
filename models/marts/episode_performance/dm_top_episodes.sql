--Top 10 episodios por rating (stars)
{{ config(
    materialized = 'view',
    tags = ['datamart', 'episode_performance']
) }}

select
    d.title as episode_title,
    d.season,
    d.episode_number,

    f.stars as rating,
    f.votes,
    f.us_views_millions,

    f.rank_stars,
    f.rank_votes,
    f.rank_views

from {{ ref('FCT_EPISODE_PERFORMANCE') }} f
join {{ ref('DIM_EPISODE') }} d
    on f.id_episode = d.id_episode
order by f.stars desc


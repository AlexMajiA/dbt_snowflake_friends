--Identifica qué personaje domina emocionalmente cada episodio.
{{ config(
    materialized = 'table',
    tags = ['datamart', 'emotions']
) }}

with counts as (
    select
        f.id_episode,
        f.season,
        f.episode,
        c.character_name,
        count(*) as emotion_events
    from {{ ref('FCT_EMOTIONS') }} f
    join {{ ref('DIM_CHARACTER') }} c
        on f.id_character = c.id_character
    group by
        f.id_episode,
        f.season,
        f.episode,
        c.character_name
),
ranked as (
    select
        *,
        dense_rank() over (
            partition by id_episode
            order by emotion_events desc
        ) as character_rank
    from counts
)

select *
from ranked
order by season, episode, character_rank

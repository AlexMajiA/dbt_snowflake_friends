--Top personajes por temporada (ranking emocional general).
{{ config(
    materialized = 'view',
    tags = ['datamart', 'emotions']
) }}

with totals as (
    select
        f.season,
        c.character_name,
        count(*) as total_emotions
    from {{ ref('FCT_EMOTIONS') }} f
    join {{ ref('DIM_CHARACTER') }} c
        on f.id_character = c.id_character
    group by
        f.season,
        c.character_name
),
ranked as (
    select
        season,
        character_name,
        total_emotions,
        rank() over (
            partition by season
            order by total_emotions desc
        ) as emotion_rank
    from totals
)

select *
from ranked
order by season, emotion_rank desc

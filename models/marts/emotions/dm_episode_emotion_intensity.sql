--Mide qué episodios son los más “tristes”, “alegres”, “enfadados”…
{{ config(
    materialized = 'table',
    tags = ['datamart', 'emotions']
) }}

select
    f.id_episode,
    f.season,
    f.episode,
    e.emotion_name,
    count(*) as emotion_count,
    dense_rank() over (
        partition by e.emotion_name
        order by count(*) desc
    ) as emotion_rank_global,
    dense_rank() over (
        partition by f.season, e.emotion_name
        order by count(*) desc
    ) as emotion_rank_in_season
from {{ ref('FCT_EMOTIONS') }} f
join {{ ref('DIM_EMOTION') }} e
    on f.id_emotion = e.id_emotion
group by
    f.id_episode,
    f.season,
    f.episode,
    e.emotion_name
order by emotion_count desc

{{ config(
    materialized = 'view',
    tags = ['datamart', 'emotions']
) }}

select
    c.character_name,
    e.emotion_name,
    count(*) as emotion_count
from {{ ref('FCT_EMOTIONS') }} f
join {{ ref('DIM_CHARACTER') }} c
    on f.id_character = c.id_character
join {{ ref('DIM_EMOTION') }} e
    on f.id_emotion    = e.id_emotion
group by
    c.character_name,
    e.emotion_name
order by emotion_count desc

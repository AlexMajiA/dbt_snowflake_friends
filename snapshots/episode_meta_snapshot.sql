
{% snapshot episode_meta_snapshot %}
{{
    config(
        database='FRIENDS_SILVER',
        schema='SNAPSHOTS',
        unique_key='id_episode',
        strategy='check',
        check_cols=['summary','stars','votes','us_views_millions']
    )
}}

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
    us_views_millions
from {{ ref('STG_RAW__EPISODE') }}

{% endsnapshot %}





































/* Check: La estrategia de verificación es útil para las tablas que no tienen una columna updated_at confiable. 
Esta estrategia funciona comparando una lista de columnas entre sus valores actuales e históricos. 
Si alguna de estas columnas ha cambiado, dbt invalidará el registro anterior y registrará el nuevo. 
Si los valores de la columna son idénticos, dbt no realizará ninguna acción.
*/

--No necesito un hard_deletes porque los episodios no se borran, solo se cambian las métricas.

--schema es la manera moderna y recomendada.
--target_schema no hereda configs.
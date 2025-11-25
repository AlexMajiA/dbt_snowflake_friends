{{ 
    config(
        materialized='incremental',
        unique_key='id_episode',
        tags=['incremental']
    ) 
}}


with episode_source as (

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

        current_timestamp() as ingest_timestamp

    from {{ ref('STG_RAW__EPISODE') }}

),

episode_enriched as (

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

        -- La marca de ingesta se propaga
        ingest_timestamp,

        -- columna derivada 
        case
            when stars >= 8 then 'High-rated'
            when stars >= 6 then 'Medium-rated'
            else 'Low-rated'
        end as rating_category

    from episode_source
),
episode_dedup as (

    select *
    from episode_enriched
    qualify row_number() over (
        partition by id_episode
        order by ingest_timestamp desc
    ) = 1
)

-- 4. SELECT FINAL (CON EL INCREMENTAL)
select *
from episode_dedup

{% if is_incremental() %}

    -- SOLO INSERTAMOS FILAS MÁS RECIENTES QUE LO ÚLTIMO CARGADO
    where ingest_timestamp > (select max(ingest_timestamp) from {{ this }})

{% endif %}








-- NOTAS:
-- INT_EPISODE_PERFORMANCE
-- Modelo incremental Silver que simula INGESTAS DIARIAS.
-- Cada ejecución añade "nuevos paquetes" con valores
-- actualizados de votes, stars y views.
--
-- Este modelo sirve como puente entre:
--   - STAGING (datos limpios)
--   - SNAPSHOT (historia)
--   - GOLD (fact table FCT_EPISODE_PERFORMANCE)

-- =======================================================
-- INT_EPISODE_PERFORMANCE
-- Simulación de ingestas: cada ejecución añade episodios
-- cuya marca ingest_timestamp sea mayor que la última 
-- ingesta registrada en este modelo.
-- =======================================================
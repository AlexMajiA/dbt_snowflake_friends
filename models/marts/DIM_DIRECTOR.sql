{{ config(materialized='table') }}

with director_base as (

    select
        id_director,
        director as director_name
    from {{ ref('STG_RAW__DIRECTOR') }}

),

dedup as (
    select distinct * from director_base
)

select * from dedup

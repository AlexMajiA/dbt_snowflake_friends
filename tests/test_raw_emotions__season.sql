--Test que valida que todas las temporadas de raw_emotions existen también en raw_dialogs.

with invalid as (
    select distinct e.season
    from {{ source('raw', 'raw_emotions') }} e
    left join {{ source('raw', 'raw_dialogs') }} d
        on e.season = d.season
    where d.season is null
)

select *
from invalid

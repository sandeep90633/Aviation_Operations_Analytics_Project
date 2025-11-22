with
    staging as (
        select
            *
        from 
            {{ source('aviation', 'flights') }}
        where estarrivalairport in ('EDDN', 'KJFK')
    )

select *
from staging

{{ 
    config(
        alias='players_profiles', 
    ) 
}}


-- For the sake of time, not bothering to perform any transformation
select
    *

from {{ source('raw', 'players_profiles') }}

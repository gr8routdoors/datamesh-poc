-- For the sake of time, not bothering to perform any typical silver layer cleanup

select
    *

from {{ source('bronze', 'chess__players_profiles') }}

{{ 
    config(
        alias='players_games', 
    ) 
}}


-- For the sake of time, not bothering to perform any typical silver layer cleanup
select
    *

from {{ ref('bronze_players_games') }}

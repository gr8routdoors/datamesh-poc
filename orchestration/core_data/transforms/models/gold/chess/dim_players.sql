-- For the sake of time, not bothering to perform any typical silver layer cleanup
{{ 
    config(
        materialized='incremental',
        unique_key = 'player_id',
    )
}}

select
    *

from {{ source('silver', 'chess_players_profiles') }}

{% if is_incremental() %}

-- TODO: make sure this is the right column for record timestamp/sequence
where last_online > (select max(last_online) from {{ this }})

{% endif %}


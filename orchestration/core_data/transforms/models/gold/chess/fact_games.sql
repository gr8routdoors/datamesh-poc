-- For the sake of time, not bothering to perform any typical silver layer cleanup
{{ 
    config(
        materialized='incremental',
        unique_key = 'uuid',
    )
}}

select
    *

from {{ source('silver', 'chess_players_games') }}

{% if is_incremental() %}

-- TODO: make sure this is the right column for record timestamp/sequence
where end_time > (select max(end_time) from {{ this }})

{% endif %}


-- For the sake of time, not bothering to perform the typical star modeling
{{ 
    config(
        alias='dim_players',
        materialized='incremental',
        unique_key = 'player_id',
    )
}}

select
    *

from {{ ref('silver_players_profiles') }}

{% if is_incremental() %}

-- TODO: make sure this is the right column for record timestamp/sequence
where last_online > (select max(last_online) from {{ this }})

{% endif %}


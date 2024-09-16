-- For the sake of time, not bothering to perform the typical star modeling
{{ 
    config(
        alias='fact_games',
        materialized='incremental',
        unique_key = 'uuid',
    )
}}

select
    *

from {{ ref('silver_players_games') }}

{% if is_incremental() %}

-- TODO: make sure this is the right column for record timestamp/sequence
where end_time > (select max(end_time) from {{ this }})

{% endif %}


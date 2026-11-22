{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

select
    opening_inventory_key,
    snapshot_at
from {{ ref('stg_r2_opening_inventory') }}
where
    timezone('UTC', snapshot_at)
    <> date_trunc('day', timezone('UTC', snapshot_at))

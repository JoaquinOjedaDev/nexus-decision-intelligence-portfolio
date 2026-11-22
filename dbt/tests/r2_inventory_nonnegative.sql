{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

select
    sku_location_key,
    sku_id,
    location_id,
    on_hand_qty
from {{ ref('gold_r2_inventory_position') }}
where on_hand_qty < 0

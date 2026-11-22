{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

select
    sku_location_key,
    opening_qty,
    receipt_qty,
    fulfilled_qty,
    adjustment_qty,
    on_hand_qty
from {{ ref('gold_r2_inventory_position') }}
where
    on_hand_qty
    <> opening_qty + receipt_qty - fulfilled_qty + adjustment_qty

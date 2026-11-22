select sku_id, location_id, on_hand_qty
from {{ ref('fct_inventory_position') }}
where on_hand_qty < 0

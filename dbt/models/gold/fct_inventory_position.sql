select
    concat(sku_id, '::', location_id) as sku_location_key,
    sku_id,
    location_id,
    cast(sum(delta_qty) as bigint) as on_hand_qty,
    cast(count(*) as bigint) as contributing_event_count,
    max(event_time) as as_of_event_time,
    max(ingested_at) as as_of_ingestion_time
from {{ ref('stg_inventory_adjustments') }}
group by sku_id, location_id

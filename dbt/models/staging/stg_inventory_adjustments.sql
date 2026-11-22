select
    cast(event_id as varchar) as event_id,
    cast(sku_id as varchar) as sku_id,
    cast(location_id as varchar) as location_id,
    cast(delta_qty as bigint) as delta_qty,
    cast(reason as varchar) as reason,
    cast(event_time as timestamptz) as event_time,
    cast(ingested_at as timestamptz) as ingested_at
from {{ ref('inventory_adjustments') }}

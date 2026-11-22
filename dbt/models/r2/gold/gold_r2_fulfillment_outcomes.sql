{{ config(enabled=var('enable_r2', false), materialized='table', tags=['r2']) }}

select
    fulfillment_id,
    demand_date,
    sku_location_key,
    sku_id,
    location_id,
    demand_qty,
    fulfilled_qty,
    cast(demand_qty - fulfilled_qty as bigint) as unfulfilled_qty,
    case
        when demand_qty = 0 then cast(1.0 as double)
        else cast(fulfilled_qty as double) / cast(demand_qty as double)
    end as fill_rate,
    available_at
from {{ ref('stg_r2_fulfillment_outcomes') }}

{{ config(enabled=var('enable_r2', false), materialized='table', tags=['r2']) }}

select
    concat(
        cast(demand_date as varchar),
        '::',
        sku_id,
        '::',
        location_id
    ) as demand_history_key,
    demand_date,
    sku_location_key,
    sku_id,
    location_id,
    cast(sum(demand_qty) as bigint) as demand_qty,
    cast(count(*) as bigint) as fulfillment_record_count,
    max(available_at) as latest_available_at
from {{ ref('stg_r2_fulfillment_outcomes') }}
group by demand_date, sku_location_key, sku_id, location_id

{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

with opening_counts as (
    select
        sku_location_key,
        count(*) as opening_count
    from {{ ref('stg_r2_opening_inventory') }}
    group by sku_location_key
)
select
    products.sku_location_key,
    coalesce(openings.opening_count, 0) as opening_count
from {{ ref('stg_r2_product_locations') }} as products
left join opening_counts as openings using (sku_location_key)
where products.active and coalesce(openings.opening_count, 0) <> 1

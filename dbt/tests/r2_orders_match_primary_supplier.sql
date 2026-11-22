{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

select
    orders.order_id,
    orders.sku_location_key,
    orders.supplier_id as order_supplier_id,
    products.supplier_id as primary_supplier_id
from {{ ref('stg_r2_purchase_orders') }} as orders
join {{ ref('stg_r2_product_locations') }} as products using (sku_location_key)
where orders.supplier_id <> products.supplier_id

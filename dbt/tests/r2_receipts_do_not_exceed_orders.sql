{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

with receipt_totals as (
    select
        order_id,
        sum(received_qty) as received_qty
    from {{ ref('stg_r2_receipts') }}
    group by order_id
)
select
    orders.order_id,
    orders.ordered_qty,
    receipts.received_qty
from {{ ref('stg_r2_purchase_orders') }} as orders
join receipt_totals as receipts using (order_id)
where receipts.received_qty > orders.ordered_qty

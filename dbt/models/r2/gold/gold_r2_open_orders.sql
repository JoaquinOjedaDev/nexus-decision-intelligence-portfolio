{{ config(enabled=var('enable_r2', false), materialized='table', tags=['r2']) }}

with receipt_totals as (
    select
        order_id,
        cast(sum(received_qty) as bigint) as received_qty,
        max(received_at) as latest_received_at,
        max(available_at) as latest_receipt_available_at
    from {{ ref('stg_r2_receipts') }}
    group by order_id
),
order_state as (
    select
        orders.order_id,
        orders.sku_location_key,
        orders.sku_id,
        orders.location_id,
        orders.supplier_id,
        orders.ordered_qty,
        coalesce(receipts.received_qty, cast(0 as bigint)) as received_qty,
        orders.ordered_qty
        - coalesce(receipts.received_qty, cast(0 as bigint)) as open_qty,
        orders.ordered_at,
        orders.expected_at,
        receipts.latest_received_at,
        greatest(
            orders.available_at,
            coalesce(receipts.latest_receipt_available_at, orders.available_at)
        ) as latest_available_at
    from {{ ref('stg_r2_purchase_orders') }} as orders
    left join receipt_totals as receipts using (order_id)
)
select *
from order_state
where open_qty > 0

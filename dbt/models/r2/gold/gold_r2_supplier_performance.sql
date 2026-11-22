{{ config(enabled=var('enable_r2', false), materialized='table', tags=['r2']) }}

with receipt_totals as (
    select
        order_id,
        cast(sum(received_qty) as bigint) as received_qty,
        max(received_at) as completed_at
    from {{ ref('stg_r2_receipts') }}
    group by order_id
),
order_outcomes as (
    select
        orders.order_id,
        orders.supplier_id,
        orders.ordered_qty,
        coalesce(receipts.received_qty, cast(0 as bigint)) as received_qty,
        receipts.completed_at,
        orders.expected_at,
        coalesce(receipts.received_qty, cast(0 as bigint))
        >= orders.ordered_qty as completed,
        coalesce(receipts.received_qty, cast(0 as bigint))
        >= orders.ordered_qty
        and receipts.completed_at <= orders.expected_at as completed_on_time
    from {{ ref('stg_r2_purchase_orders') }} as orders
    left join receipt_totals as receipts using (order_id)
),
aggregated as (
    select
        supplier_id,
        cast(count(*) as bigint) as order_count,
        cast(count(*) filter (where completed) as bigint) as completed_order_count,
        cast(
            count(*) filter (where completed_on_time) as bigint
        ) as on_time_completed_order_count,
        cast(sum(received_qty) as bigint) as received_units
    from order_outcomes
    group by supplier_id
)
select
    suppliers.supplier_id,
    suppliers.supplier_name,
    suppliers.active,
    coalesce(aggregated.order_count, cast(0 as bigint)) as order_count,
    coalesce(
        aggregated.completed_order_count, cast(0 as bigint)
    ) as completed_order_count,
    coalesce(
        aggregated.on_time_completed_order_count, cast(0 as bigint)
    ) as on_time_completed_order_count,
    coalesce(aggregated.received_units, cast(0 as bigint)) as received_units,
    case
        when coalesce(aggregated.completed_order_count, 0) = 0
            then cast(0.0 as double)
        else
            cast(aggregated.on_time_completed_order_count as double)
            / cast(aggregated.completed_order_count as double)
    end as on_time_completion_rate,
    suppliers.available_at
from {{ ref('stg_r2_suppliers') }} as suppliers
left join aggregated using (supplier_id)

{{ config(enabled=var('enable_r2', false), materialized='table', tags=['r2']) }}

{% set planning_cutoff = var('r2_planning_cutoff', '1970-01-01T00:00:00+00:00') %}

with ranked_opening as (
    select
        *,
        row_number() over (
            partition by sku_location_key
            order by snapshot_at desc, available_at desc
        ) as snapshot_rank
    from {{ ref('stg_r2_opening_inventory') }}
),
opening as (
    select
        sku_location_key,
        on_hand_qty as opening_qty,
        snapshot_at as opening_snapshot_at,
        available_at as opening_available_at
    from ranked_opening
    where snapshot_rank = 1
),
receipt_totals as (
    select
        receipts.sku_location_key,
        cast(sum(receipts.received_qty) as bigint) as receipt_qty,
        max(receipts.received_at) as latest_received_at
    from {{ ref('stg_r2_receipts') }} as receipts
    join opening using (sku_location_key)
    where receipts.received_at > opening.opening_snapshot_at
    group by receipts.sku_location_key
),
fulfillment_totals as (
    select
        fulfillment.sku_location_key,
        cast(sum(fulfillment.fulfilled_qty) as bigint) as fulfilled_qty,
        max(fulfillment.demand_date) as latest_demand_date
    from {{ ref('stg_r2_fulfillment_outcomes') }} as fulfillment
    join opening using (sku_location_key)
    where
        fulfillment.demand_date
        >= cast(opening.opening_snapshot_at as date)
    group by fulfillment.sku_location_key
),
adjustment_totals as (
    select
        adjustments.sku_location_key,
        cast(sum(adjustments.delta_qty) as bigint) as adjustment_qty,
        cast(count(*) as bigint) as effective_adjustment_count,
        max(adjustments.occurred_at) as latest_adjustment_occurred_at
    from {{ ref('int_r2_effective_inventory_adjustments') }} as adjustments
    join opening using (sku_location_key)
    where adjustments.occurred_at > opening.opening_snapshot_at
    group by adjustments.sku_location_key
),
open_order_totals as (
    select
        sku_location_key,
        cast(sum(open_qty) as bigint) as on_order_qty
    from {{ ref('gold_r2_open_orders') }}
    group by sku_location_key
),
components as (
    select
        product_locations.sku_location_key,
        product_locations.sku_id,
        product_locations.location_id,
        product_locations.supplier_id,
        product_locations.active,
        opening.opening_qty,
        coalesce(receipts.receipt_qty, cast(0 as bigint)) as receipt_qty,
        coalesce(
            fulfillment.fulfilled_qty, cast(0 as bigint)
        ) as fulfilled_qty,
        coalesce(
            adjustments.adjustment_qty, cast(0 as bigint)
        ) as adjustment_qty,
        coalesce(open_orders.on_order_qty, cast(0 as bigint)) as on_order_qty,
        coalesce(
            adjustments.effective_adjustment_count, cast(0 as bigint)
        ) as effective_adjustment_count,
        opening.opening_snapshot_at,
        receipts.latest_received_at,
        fulfillment.latest_demand_date,
        adjustments.latest_adjustment_occurred_at
    from {{ ref('stg_r2_product_locations') }} as product_locations
    join opening using (sku_location_key)
    left join receipt_totals as receipts using (sku_location_key)
    left join fulfillment_totals as fulfillment using (sku_location_key)
    left join adjustment_totals as adjustments using (sku_location_key)
    left join open_order_totals as open_orders using (sku_location_key)
    where product_locations.active
)
select
    sku_location_key,
    sku_id,
    location_id,
    supplier_id,
    active,
    opening_qty,
    receipt_qty,
    fulfilled_qty,
    adjustment_qty,
    cast(
        opening_qty + receipt_qty - fulfilled_qty + adjustment_qty as bigint
    ) as on_hand_qty,
    on_order_qty,
    effective_adjustment_count,
    opening_snapshot_at,
    latest_received_at,
    latest_demand_date,
    latest_adjustment_occurred_at,
    cast('{{ planning_cutoff }}' as timestamptz) as planning_cutoff
from components

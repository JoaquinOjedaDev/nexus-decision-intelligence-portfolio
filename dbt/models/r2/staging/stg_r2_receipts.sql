{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

{% set validated_file = var('r2_validated_files', {}).get(
    'receipts', 'artifacts/r2/validated/receipts/*.parquet'
) | replace('\\', '/') %}
{% set planning_cutoff = var('r2_planning_cutoff', '1970-01-01T00:00:00+00:00') %}

with typed as (
    select
        cast(receipt_id as varchar) as receipt_id,
        cast(order_id as varchar) as order_id,
        cast(sku_id as varchar) as sku_id,
        cast(location_id as varchar) as location_id,
        cast(supplier_id as varchar) as supplier_id,
        cast(received_qty as bigint) as received_qty,
        cast(received_at as timestamptz) as received_at,
        cast(available_at as timestamptz) as available_at
    from read_parquet('{{ validated_file }}')
)
select
    receipt_id,
    order_id,
    concat(sku_id, '::', location_id) as sku_location_key,
    sku_id,
    location_id,
    supplier_id,
    received_qty,
    received_at,
    available_at
from typed
where
    received_at <= cast('{{ planning_cutoff }}' as timestamptz)
    and available_at <= cast('{{ planning_cutoff }}' as timestamptz)

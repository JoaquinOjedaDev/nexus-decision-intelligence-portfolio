{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

{% set validated_file = var('r2_validated_files', {}).get(
    'product_locations', 'artifacts/r2/validated/product_locations/*.parquet'
) | replace('\\', '/') %}
{% set planning_cutoff = var('r2_planning_cutoff', '1970-01-01T00:00:00+00:00') %}

with typed as (
    select
        cast(sku_id as varchar) as sku_id,
        cast(location_id as varchar) as location_id,
        cast(supplier_id as varchar) as supplier_id,
        cast(active as boolean) as active,
        cast(available_at as timestamptz) as available_at
    from read_parquet('{{ validated_file }}')
),
visible as (
    select *
    from typed
    where available_at <= cast('{{ planning_cutoff }}' as timestamptz)
)
select
    concat(sku_id, '::', location_id) as sku_location_key,
    sku_id,
    location_id,
    supplier_id,
    active,
    available_at
from visible

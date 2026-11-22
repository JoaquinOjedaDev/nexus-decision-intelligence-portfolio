{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

{% set validated_file = var('r2_validated_files', {}).get(
    'fulfillment_outcomes', 'artifacts/r2/validated/fulfillment_outcomes/*.parquet'
) | replace('\\', '/') %}
{% set planning_cutoff = var('r2_planning_cutoff', '1970-01-01T00:00:00+00:00') %}

with typed as (
    select
        cast(fulfillment_id as varchar) as fulfillment_id,
        cast(demand_date as date) as demand_date,
        cast(sku_id as varchar) as sku_id,
        cast(location_id as varchar) as location_id,
        cast(demand_qty as bigint) as demand_qty,
        cast(fulfilled_qty as bigint) as fulfilled_qty,
        cast(available_at as timestamptz) as available_at
    from read_parquet('{{ validated_file }}')
)
select
    fulfillment_id,
    demand_date,
    concat(sku_id, '::', location_id) as sku_location_key,
    sku_id,
    location_id,
    demand_qty,
    fulfilled_qty,
    available_at
from typed
where
    demand_date < cast(cast('{{ planning_cutoff }}' as timestamptz) as date)
    and available_at <= cast('{{ planning_cutoff }}' as timestamptz)

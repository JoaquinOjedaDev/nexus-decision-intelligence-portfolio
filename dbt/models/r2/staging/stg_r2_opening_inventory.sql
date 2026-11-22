{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

{% set validated_file = var('r2_validated_files', {}).get(
    'opening_inventory', 'artifacts/r2/validated/opening_inventory/*.parquet'
) | replace('\\', '/') %}
{% set planning_cutoff = var('r2_planning_cutoff', '1970-01-01T00:00:00+00:00') %}

with typed as (
    select
        cast(sku_id as varchar) as sku_id,
        cast(location_id as varchar) as location_id,
        cast(on_hand_qty as bigint) as on_hand_qty,
        cast(snapshot_at as timestamptz) as snapshot_at,
        cast(available_at as timestamptz) as available_at
    from read_parquet('{{ validated_file }}')
)
select
    concat(
        sku_id,
        '::',
        location_id,
        '::',
        cast(snapshot_at as varchar)
    ) as opening_inventory_key,
    concat(sku_id, '::', location_id) as sku_location_key,
    sku_id,
    location_id,
    on_hand_qty,
    snapshot_at,
    available_at
from typed
where
    snapshot_at <= cast('{{ planning_cutoff }}' as timestamptz)
    and available_at <= cast('{{ planning_cutoff }}' as timestamptz)

{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

{% set validated_file = var('r2_validated_files', {}).get(
    'inventory_adjustments', 'artifacts/r2/validated/inventory_adjustments/*.parquet'
) | replace('\\', '/') %}

select
    cast(contract_version as varchar) as contract_version,
    cast(event_id as varchar) as event_id,
    cast(event_version as integer) as event_version,
    cast(source as varchar) as source,
    cast(logical_event_id as varchar) as logical_event_id,
    cast(revision as integer) as revision,
    cast(idempotency_key as varchar) as idempotency_key,
    cast(occurred_at as timestamptz) as occurred_at,
    cast(ingested_at as timestamptz) as ingested_at,
    concat(cast(sku_id as varchar), '::', cast(location_id as varchar)) as sku_location_key,
    cast(sku_id as varchar) as sku_id,
    cast(location_id as varchar) as location_id,
    cast(delta_qty as bigint) as delta_qty,
    cast(reason_code as varchar) as reason_code
from read_parquet('{{ validated_file }}')

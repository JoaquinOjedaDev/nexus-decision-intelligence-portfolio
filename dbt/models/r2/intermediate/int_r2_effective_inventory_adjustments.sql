{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

{% set planning_cutoff = var('r2_planning_cutoff', '1970-01-01T00:00:00+00:00') %}

with visible as (
    select
        *,
        row_number() over (
            partition by source, logical_event_id
            order by revision, ingested_at, event_id
        ) as expected_revision
    from {{ ref('stg_r2_inventory_adjustments') }}
    where
        occurred_at <= cast('{{ planning_cutoff }}' as timestamptz)
        and ingested_at <= cast('{{ planning_cutoff }}' as timestamptz)
),
contiguous_prefix as (
    select *
    from visible
    where revision = expected_revision
)
select
    contract_version,
    event_id,
    event_version,
    source,
    logical_event_id,
    revision,
    idempotency_key,
    occurred_at,
    ingested_at,
    sku_location_key,
    sku_id,
    location_id,
    delta_qty,
    reason_code
from contiguous_prefix
qualify
    row_number() over (
        partition by source, logical_event_id
        order by revision desc, ingested_at desc, event_id desc
    )
    = 1

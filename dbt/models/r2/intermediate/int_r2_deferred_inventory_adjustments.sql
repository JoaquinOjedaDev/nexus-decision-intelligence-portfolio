{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

{% set planning_cutoff = var('r2_planning_cutoff', '1970-01-01T00:00:00+00:00') %}

with all_adjustments as (
    select *
    from {{ ref('stg_r2_inventory_adjustments') }}
),
time_deferred as (
    select
        *,
        case
            when
                occurred_at > cast('{{ planning_cutoff }}' as timestamptz)
                and ingested_at > cast('{{ planning_cutoff }}' as timestamptz)
                then 'future_business_and_arrival'
            when occurred_at > cast('{{ planning_cutoff }}' as timestamptz)
                then 'future_business_time'
            else 'late_arrival'
        end as deferred_reason
    from all_adjustments
    where
        occurred_at > cast('{{ planning_cutoff }}' as timestamptz)
        or ingested_at > cast('{{ planning_cutoff }}' as timestamptz)
),
visible as (
    select
        *,
        row_number() over (
            partition by source, logical_event_id
            order by revision, ingested_at, event_id
        ) as expected_revision
    from all_adjustments
    where
        occurred_at <= cast('{{ planning_cutoff }}' as timestamptz)
        and ingested_at <= cast('{{ planning_cutoff }}' as timestamptz)
),
chain_deferred as (
    select
        * exclude (expected_revision),
        'incomplete_visible_revision_chain' as deferred_reason
    from visible
    where revision <> expected_revision
),
deferred as (
    select * from time_deferred
    union all by name
    select * from chain_deferred
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
    reason_code,
    deferred_reason
from deferred

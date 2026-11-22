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
    select * exclude (expected_revision)
    from visible
    where revision = expected_revision
)
select contiguous_prefix.*
from contiguous_prefix
left join {{ ref('int_r2_effective_inventory_adjustments') }} as effective using (event_id)
where effective.event_id is null

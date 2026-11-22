{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

with dispositions as (
    select event_id, 'effective' as disposition
    from {{ ref('int_r2_effective_inventory_adjustments') }}
    union all
    select event_id, 'superseded' as disposition
    from {{ ref('int_r2_superseded_inventory_adjustments') }}
    union all
    select event_id, 'deferred' as disposition
    from {{ ref('int_r2_deferred_inventory_adjustments') }}
),
classified as (
    select
        source.event_id,
        count(dispositions.disposition) as disposition_count
    from {{ ref('stg_r2_inventory_adjustments') }} as source
    left join dispositions using (event_id)
    group by source.event_id
)
select *
from classified
where disposition_count <> 1

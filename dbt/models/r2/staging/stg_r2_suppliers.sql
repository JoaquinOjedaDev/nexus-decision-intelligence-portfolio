{{ config(enabled=var('enable_r2', false), tags=['r2']) }}

{% set validated_file = var('r2_validated_files', {}).get(
    'suppliers', 'artifacts/r2/validated/suppliers/*.parquet'
) | replace('\\', '/') %}
{% set planning_cutoff = var('r2_planning_cutoff', '1970-01-01T00:00:00+00:00') %}

with typed as (
    select
        cast(supplier_id as varchar) as supplier_id,
        cast(supplier_name as varchar) as supplier_name,
        cast(active as boolean) as active,
        cast(available_at as timestamptz) as available_at
    from read_parquet('{{ validated_file }}')
)
select *
from typed
where available_at <= cast('{{ planning_cutoff }}' as timestamptz)

with raw_total as (
    select sum(delta_qty) as quantity
    from {{ ref('stg_inventory_adjustments') }}
),
gold_total as (
    select sum(on_hand_qty) as quantity
    from {{ ref('fct_inventory_position') }}
)
select raw_total.quantity as raw_quantity, gold_total.quantity as gold_quantity
from raw_total cross join gold_total
where raw_total.quantity <> gold_total.quantity

{% test r2_unique_combination(model, columns) %}

select
    {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %},
    count(*) as record_count
from {{ model }}
group by
    {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
having count(*) > 1

{% endtest %}

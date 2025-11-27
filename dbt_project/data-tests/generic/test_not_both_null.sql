{% test not_both_null(model, column_1, column_2) %}

select
  *
from {{ model }}
where {{ column_1 }} is null
  and {{ column_2 }} is null

{% endtest %}
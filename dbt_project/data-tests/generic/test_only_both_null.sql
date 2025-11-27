{% test only_both_null(model, column_1, column_2) %}

select
  *
from {{ model }}
where {{ column_1 }} is not null
   or {{ column_2 }} is not null

{% endtest %}
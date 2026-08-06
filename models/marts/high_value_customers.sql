-- Plain SQL model consuming the output of the upstream Python/Spark
-- model, showing SQL and Python models composing in the same DAG.

select
    customer_id,
    first_name,
    last_name,
    order_count,
    total_amount,
    most_recent_order_date

from {{ ref('customer_order_summary') }}
where customer_value_segment = 'high'

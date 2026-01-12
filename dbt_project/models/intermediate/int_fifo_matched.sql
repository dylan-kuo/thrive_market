with source_data as (
    select * from {{ source('python_layer', 'python_fifo_output') }}
)

select
    trans_id,
    trans_type,
    created_at,
    customer_id,
    amount,
    redeem_id::int as redeem_id
from source_data
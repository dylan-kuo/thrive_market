with source_data as (
    select * from "thrive"."main"."python_fifo_output"
)

select
    trans_id,
    trans_type,
    created_at,
    customer_id,
    amount,
    redeem_id::int as redeem_id
from source_data
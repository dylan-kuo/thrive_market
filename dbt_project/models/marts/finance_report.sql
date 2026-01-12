with enriched_transactions as (
    select 
        t.trans_id,
        t.customer_id,
        t.amount,
        t.trans_type,
        t.created_at,
        f.redeem_id,
        -- Sign convention: earned = +, spent/expired = -
        case 
            when t.trans_type = 'earned' then t.amount
            else -t.amount 
        end as signed_amount
    from {{ ref('stg_tc_data') }} t
    left join {{ ref('int_fifo_matched') }} f 
        on t.trans_id = f.trans_id
),

running_balances as (
    select
        trans_id,
        customer_id,
        trans_type,
        created_at,
        amount,
        redeem_id,
        signed_amount,
        -- Running balance for this customer up to this point in time
        sum(signed_amount) over (
            partition by customer_id 
            order by created_at, trans_id
            rows between unbounded preceding and current row
        ) as running_balance,
        -- Cumulative totals by transaction type
        sum(case when trans_type = 'earned' then amount else 0 end) over (
            partition by customer_id 
            order by created_at, trans_id
            rows between unbounded preceding and current row
        ) as cumulative_earned,
        sum(case when trans_type = 'spent' then amount else 0 end) over (
            partition by customer_id 
            order by created_at, trans_id
            rows between unbounded preceding and current row
        ) as cumulative_spent,
        sum(case when trans_type = 'expired' then amount else 0 end) over (
            partition by customer_id 
            order by created_at, trans_id
            rows between unbounded preceding and current row
        ) as cumulative_expired
    from enriched_transactions
)

select
    customer_id,
    created_at as transaction_date,
    amount,
    cumulative_earned,
    cumulative_spent,
    cumulative_expired,
    (cumulative_earned - cumulative_spent - cumulative_expired) as current_balance
from running_balances
order by customer_id, created_at, trans_id
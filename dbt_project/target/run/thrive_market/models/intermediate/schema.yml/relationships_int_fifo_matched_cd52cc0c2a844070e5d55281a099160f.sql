select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select redeem_id as from_field
    from "thrive"."main"."int_fifo_matched"
    where redeem_id is not null
),

parent as (
    select transaction_id as to_field
    from "thrive"."main"."stg_tc_data"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test
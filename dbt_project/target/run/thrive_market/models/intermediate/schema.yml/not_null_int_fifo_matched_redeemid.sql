select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select redeemid
from "thrive"."main"."int_fifo_matched"
where redeemid is null



      
    ) dbt_internal_test
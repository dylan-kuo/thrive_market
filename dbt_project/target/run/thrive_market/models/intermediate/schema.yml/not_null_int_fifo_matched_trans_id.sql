
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trans_id
from "thrive"."main"."int_fifo_matched"
where trans_id is null



  
  
      
    ) dbt_internal_test
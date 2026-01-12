
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    trans_id as unique_field,
    count(*) as n_records

from "thrive"."main"."int_fifo_matched"
where trans_id is not null
group by trans_id
having count(*) > 1



  
  
      
    ) dbt_internal_test
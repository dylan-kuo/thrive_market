select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    redeemid as unique_field,
    count(*) as n_records

from "thrive"."main"."int_fifo_matched"
where redeemid is not null
group by redeemid
having count(*) > 1



      
    ) dbt_internal_test
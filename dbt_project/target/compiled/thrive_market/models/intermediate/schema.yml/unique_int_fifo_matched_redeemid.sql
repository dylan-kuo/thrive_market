
    
    

select
    redeemid as unique_field,
    count(*) as n_records

from "thrive"."main"."int_fifo_matched"
where redeemid is not null
group by redeemid
having count(*) > 1



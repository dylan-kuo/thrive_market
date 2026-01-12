
    
    

select
    redeem_id as unique_field,
    count(*) as n_records

from "thrive"."main"."int_fifo_matched"
where redeem_id is not null
group by redeem_id
having count(*) > 1



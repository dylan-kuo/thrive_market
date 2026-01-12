
  
  create view "thrive"."main"."stg_tc_data__dbt_tmp" as (
    select
    "TRANS_ID"::int as trans_id,
    "TCTYPE"::varchar as trans_type,
    "CREATEDAT"::timestamp as created_at,
    "EXPIREDAT"::timestamp as expired_at,
    "CUSTOMERID"::int as customer_id,
    "ORDERID"::int as order_id,
    "AMOUNT"::decimal(10,2) as amount,
    "REASON"::varchar as reason
from "thrive"."main"."raw_tc_data"
  );

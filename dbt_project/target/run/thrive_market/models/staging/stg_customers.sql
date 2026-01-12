
  
  create view "thrive"."main"."stg_customers__dbt_tmp" as (
    SELECT
    "CUSTOMERID"::int as customer_id,
    "EMAIL"::varchar as email,
    "FIRSTNAME"::varchar as first_name,
    "BILLINGPOSTCODE"::varchar as billing_post_code
FROM "thrive"."main"."raw_customers"
  );

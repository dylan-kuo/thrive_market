
  
  create view "thrive"."main"."stg_sales__dbt_tmp" as (
    SELECT
    "ORDERID"::int as order_id,
    "CUSTOMERID"::int as customer_id,
    "PREDISCOUNTGROSSPRODUCTSALES":: decimal(10,2) as pre_discount_gross_product_sales,
    "ORDERWEIGHT"::decimal(10,2) as order_weight
FROM "thrive"."main"."raw_sales"
  );

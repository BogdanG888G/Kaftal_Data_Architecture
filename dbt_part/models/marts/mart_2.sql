{{ config(materialized = 'table') }}

select branch, region, city, address, sum(sales_units) as total_sales_units, sum(sales_rub) as total_sales_rub
from {{ ref('stg_sales') }}
group by branch, region, city, address
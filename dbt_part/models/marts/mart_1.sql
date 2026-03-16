{{ config(materialized='table') }}

select brand, grammage, flavor, sum(sales_units) as total_sales_units, sum(sales_rub) as total_sales_rub
from {{ ref ('stg_sales')}}
group by brand, grammage, flavor
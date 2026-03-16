{{ config(materialized='table') }}

select *
from {{source ('silver', 'sales')}}
where retail_chain = 'Пятёрочка'
limit 1000
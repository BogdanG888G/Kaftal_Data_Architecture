{{config(materialized='table')}}

select * from
{{source('aushan_silver', 'sales')}}

limit 100
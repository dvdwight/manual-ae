{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('manual', 'raw_customers') }}
)
select
    customer_id,
    customer_country
from source

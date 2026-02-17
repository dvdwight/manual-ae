{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('manual', 'raw_acq_orders') }}
)
select 
    customer_id, 
    taxonomy_business_category_group 
from
    source
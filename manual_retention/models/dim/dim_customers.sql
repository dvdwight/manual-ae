with customers as (
    select * from {{ ref('stg_customers') }}
),

acq_orders as (
    select * from {{ ref('stg_acq_orders') }}
)

select
    customers.customer_id,
    customers.customer_country,
    acq_orders.taxonomy_business_category_group as business_group
from customers
left join acq_orders
    on customers.customer_id = acq_orders.customer_id
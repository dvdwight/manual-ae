-- Test: Validate that each customer has exactly one business_group in acq_orders
-- This confirms the immutability assumption and validates snapshot integrity

select customer_id, count(distinct taxonomy_business_category_group) as group_count
from {{ source('manual', 'raw_acq_orders') }}
group by customer_id
having count(distinct taxonomy_business_category_group) > 1

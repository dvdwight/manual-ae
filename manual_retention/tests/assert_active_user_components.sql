-- Test to ensure active customers equal the sum of retained, reactivated, and new customers
-- This validates that the three segments are mutually exclusive and account for all active users

select *
from {{ ref('fct_monthly_metrics') }}
where active_customers != (retained_customers + reactivated_customers + new_customers)
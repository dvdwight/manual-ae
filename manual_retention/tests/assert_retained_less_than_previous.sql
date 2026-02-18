--This test checks that the number of retained customers is less than the number of active customers in the previous month.

select *from {{ ref('fct_monthly_metrics') }}
where retained_customers > previous_month_active
     and previous_month_active is not null
-- USE LIMIT TO MINIMISE ERRORS
limit 10
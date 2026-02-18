{{
    config(
        materialized='table',
        partition_by={
            "field": "activity_month",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["customer_country", "business_group"],
        partition_expiration_days=20000
    )
}}

with retention_data as (
    select * from {{ ref('fct_customer_retention') }}
),

monthly_metrics as (
    select
        activity_month,
        customer_country,
        business_group,
        
        -- Total active customers
        count(distinct customer_id) as active_customers,
        
        -- New customers (first month)
        count(distinct case when is_cohort_month = 1 then customer_id end) as new_customers,
        
        -- Retained customers
        count(distinct case when is_retained_from_previous_month = 1 then customer_id end) as retained_customers,
        
        -- Reactivated customers
        count(distinct case when is_reactivation = 1 then customer_id end) as reactivated_customers
        
    from retention_data
    group by activity_month, customer_country, business_group
),

with_previous_month as (
    select
        *,
        lag(active_customers) over (
            partition by customer_country, business_group
            order by activity_month
        ) as previous_month_active,
        
        lag(activity_month) over (
            partition by customer_country, business_group
            order by activity_month
        ) as previous_month
    from monthly_metrics
),

with_retention_metrics as (
    select
        *,
        safe_divide(retained_customers, previous_month_active) as retention_rate,
        lag(safe_divide(retained_customers, previous_month_active)) over (
            partition by customer_country, business_group
            order by activity_month
        ) as previous_month_retention_rate
    from with_previous_month
)

select
    activity_month,
    customer_country,
    business_group,
    active_customers,
    new_customers,
    retained_customers,
    reactivated_customers,
    previous_month_active,
    
    -- Calculate churned customers (previous month - retained)
    previous_month_active - retained_customers as churned_customers,
    
    -- Calculate retention rate
    retention_rate,
    
    -- Calculate previous month retention rate
    previous_month_retention_rate,
    
    -- Calculate MoM change in retention rate
    retention_rate - previous_month_retention_rate as retention_rate_mom_change,
    
    -- Calculate churn rate
    safe_divide(previous_month_active - retained_customers, previous_month_active) as churn_rate,
    
    -- Calculate growth rate
    safe_divide(
        active_customers - previous_month_active, 
        previous_month_active
    ) as growth_rate,
    
    -- Customer acquisition rate
    safe_divide(new_customers, active_customers) as acquisition_rate

from with_retention_metrics
where previous_month_active is not null
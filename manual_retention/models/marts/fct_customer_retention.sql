{{ config(
    materialized='table',
    partition_by={
        "field": "activity_month",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["cohort_month", "customer_country", "business_group"],
    partition_expiration_days=20000
    

) }}

/*
 Joins both intermediate tables and dim customers to calculate customer-level 
 retention metrics at monthly granularity.
*/


with customer_monthly_activity as (
    select * from {{ ref('int_customer_activity_monthly') }}
),

first_activity as (
    select * from {{ ref('int_customer_activity_first') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

-- Join all customer attributes
customer_base as (
    select
        cma.customer_id,
        cma.activity_month,
        fa.cohort_month,
        fa.cohort_year,
        fa.first_activity_date,
        c.customer_country,
        c.business_group
    from customer_monthly_activity cma
    left join first_activity fa
        on cma.customer_id = fa.customer_id
    left join customers c
        on cma.customer_id = c.customer_id
),

-- Calculate retention metrics at activity month level
retention_metrics as (
    select
        customer_id,
        activity_month,
        cohort_month,
        cohort_year,
        first_activity_date,
        customer_country,
        business_group,
        
        -- Calculate months since first activity
        date_diff(activity_month, cohort_month, month) as months_since_cohort,
        
        -- Flag to indicate customer is active in this month (since they have activity record)
        1 as is_active,
        
        -- Calculate if customer was active in previous month
        lag(activity_month) over (
            partition by customer_id 
            order by activity_month
        ) as previous_activity_month,
        
        -- Identify if this is the customer's first month
        case 
            when activity_month = cohort_month then 1 
            else 0 
        end as is_cohort_month
        
    from customer_base
)

select
    customer_id,
    activity_month,
    cohort_month,
    cohort_year,
    first_activity_date,
    customer_country,
    business_group,
    months_since_cohort,
    is_active,
    is_cohort_month,
    
    -- Calculate retention flags
    case 
        when previous_activity_month = date_sub(activity_month, interval 1 month) 
        then 1 
        else 0 
    end as is_retained_from_previous_month,
        
    -- Calculate if customer churned (was inactive, now active)
    case 
        when previous_activity_month is not null 
        and previous_activity_month < date_sub(activity_month, interval 1 month)
        then 1 
        else 0 
    end as is_reactivation

from retention_metrics
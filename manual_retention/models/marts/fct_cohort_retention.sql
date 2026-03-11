{{
    config(
        materialized='table',
        partition_by={
            "field": "cohort_month",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["customer_country", "business_group"],
        partition_expiration_days=20000
    )
}}

/*
 This model calculates cohort retention metrics at the cohort level, 
 including retention rates, churn rates, and cumulative active customers over time.

 It aggregates customer activity data from fct_customer_retention by cohort, country, and business group
 to provide insights into retention patterns across different cohorts, countries, and business groups. 

 The model is partitioned by cohort month for efficient querying and analysis.
*/

with retention_data as (
    select * from {{ ref('fct_customer_retention') }}
),

cohort_sizes as (
    select
        cohort_month,
        customer_country,
        business_group,
        count(distinct customer_id) as cohort_size
    from retention_data
    where is_cohort_month = 1
    group by cohort_month, customer_country, business_group
),

retention_by_period as (
    select
        cohort_month,
        customer_country,
        business_group,
        months_since_cohort,
        count(distinct customer_id) as active_customers,
        count(distinct case when is_retained_from_previous_month = 1 then customer_id end) as retained_customers,
        count(distinct case when is_reactivation = 1 then customer_id end) as reactivated_customers
    from retention_data
    group by cohort_month, customer_country, business_group, months_since_cohort
)

select
    r.cohort_month,
    r.customer_country,
    r.business_group,
    r.months_since_cohort,
    c.cohort_size,
    r.active_customers,
    r.retained_customers,
    r.reactivated_customers,
    
    -- Calculate retention rate
    safe_divide(r.active_customers, c.cohort_size) as retention_rate,
    
    -- Calculate month-over-month retention
    safe_divide(r.retained_customers, r.active_customers) as mom_retention_rate,
    
    -- Calculate churn
    1 - safe_divide(r.active_customers, c.cohort_size) as churn_rate,
    
    -- Calculate cumulative customers
    sum(r.active_customers) over (
        partition by r.cohort_month, r.customer_country, r.business_group
        order by r.months_since_cohort
    ) as cumulative_active_customers,

    lag(safe_divide(r.active_customers, c.cohort_size)) over (
         partition by r.cohort_month, r.customer_country, r.business_group order by r.months_since_cohort 
    ) as previous_retention_rate, 
    
    lag(1 - safe_divide(r.active_customers, c.cohort_size)) over 
    ( partition by r.cohort_month, r.customer_country, r.business_group order by r.months_since_cohort 
    ) as previous_churn_rate

from retention_by_period r
left join cohort_sizes c
    on r.cohort_month = c.cohort_month
    and r.customer_country = c.customer_country
    and r.business_group = c.business_group
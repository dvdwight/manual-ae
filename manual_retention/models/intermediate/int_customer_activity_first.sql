{{
    config(
        materialized='view'
    )
}}

with activity as (
    select * from {{ ref('stg_activity') }}
),
first_activity as (
    select
        customer_id,
        min(from_date) as first_activity_date,
        min(cohort_month) as cohort_month,
        min(cohort_year) as cohort_year
    from activity
    group by customer_id
)
select * from first_activity
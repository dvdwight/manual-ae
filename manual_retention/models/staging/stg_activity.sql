{{
    config(
        materialized='view'
    )
}}

with raw_activity as (
    select * from {{ source('manual', 'raw_activity') }} 
)
select 
    customer_id,
    subscription_id,
    from_date,
    to_date,
    -- Calculate subscription duration in days
    date_diff(to_date, from_date, DAY) as subscription_days,
    -- Extract month and year for cohort analysis
    date_trunc(from_date, MONTH) as cohort_month,
    date_trunc(from_date, YEAR) as cohort_year
from raw_activity
-- some validation on dates
where from_date is not null
      and to_date is not null
      and to_date >= from_date
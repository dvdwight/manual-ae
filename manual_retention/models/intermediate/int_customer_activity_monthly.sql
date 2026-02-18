{{
    config(
        materialized='view'
    )
}}

with activity as (
    select * from {{ ref('stg_activity') }}
),

/* Generate unique set of months a customer was active */
customer_months as (
    select distinct
        customer_id,
        date_trunc(month_date, month) as activity_month
    from activity
    cross join unnest(
        generate_date_array(
            date_trunc(from_date, month),
            date_trunc(to_date, month),
            interval 1 month
        )
    ) as month_date
)

select * from customer_months
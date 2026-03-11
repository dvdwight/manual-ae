{{ config(severity = 'warn') }}

-- Test to warn if activity table records jump by more than 50% from one month to the next
-- This catches potential data quality issues or unexpected spikes in activity data

with monthly_counts as (
    select
        cohort_month,
        count(*) as record_count,
        lag(count(*)) over (order by cohort_month) as previous_month_count
    from {{ ref('stg_activity') }}
    where cohort_month >= '2024-01-01'
    group by cohort_month
),

month_over_month_changes as (
    select
        cohort_month,
        record_count,
        previous_month_count,
        case
            when previous_month_count is null then null
            else round(
                ((record_count - previous_month_count) / previous_month_count) * 100, 2
            )
        end as pct_change
    from monthly_counts
)

select *
from month_over_month_changes
where pct_change > 50

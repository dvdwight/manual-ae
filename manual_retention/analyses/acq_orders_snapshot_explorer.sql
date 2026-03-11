-- Exploration model: Shows snapshot history for acq_orders
-- Use this to inspect if/when business_group changes are detected
-- Expected: All rows have dbt_valid_to = NULL (no changes detected = immutable)

select
    customer_id,
    taxonomy_business_category_group,
    dbt_valid_from,
    dbt_valid_to,
    dbt_change_type,
    dbt_sn_id
from {{ ref('scd_acq_orders') }}
order by customer_id, dbt_valid_from desc

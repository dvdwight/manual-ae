{% snapshot scd_acq_orders %}
  {{
    config(
      target_schema='manual',
      unique_key='customer_id',
      strategy='check',
      check_cols=['taxonomy_business_category_group']
    )
  }}
  select * from {{ source('manual', 'raw_acq_orders') }}
{% endsnapshot %}

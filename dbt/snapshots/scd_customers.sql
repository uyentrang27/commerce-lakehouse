{% snapshot scd_customers %}
{{
    config(
        unique_key='customer_id',
        strategy='check',
        check_cols=['city', 'segment'],
        target_schema='snapshots',
        invalidate_hard_deletes=True,
    )
}}
-- SCD Type 2 on customers: dbt tracks when `city` or `segment` change and
-- versions each customer with dbt_valid_from / dbt_valid_to.
select
    customer_id,
    full_name,
    email,
    city,
    country,
    segment,
    cast(signup_date as date) as signup_date
from {{ silver('customers') }}
{% endsnapshot %}

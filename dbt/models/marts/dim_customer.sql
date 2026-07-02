-- SCD Type 2 customer dimension sourced from the snapshot.
-- One row per customer VERSION; is_current flags the live version.
select
    md5(cast(customer_id as varchar) || '|' || coalesce(cast(dbt_valid_from as varchar), 'init')) as customer_sk,
    customer_id,
    full_name,
    email,
    city,
    country,
    segment,
    signup_date,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('scd_customers') }}

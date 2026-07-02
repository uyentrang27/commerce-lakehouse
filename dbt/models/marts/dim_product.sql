-- SCD Type 2 product dimension (price + grade history) sourced from the snapshot.
select
    md5(cast(product_id as varchar) || '|' || coalesce(cast(dbt_valid_from as varchar), 'init')) as product_sk,
    product_id,
    model_name,
    brand,
    grade,
    unit_cost,
    list_price,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('scd_products') }}

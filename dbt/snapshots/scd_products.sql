{% snapshot scd_products %}
{{
    config(
        unique_key='product_id',
        strategy='check',
        check_cols=['list_price'],
        target_schema='snapshots',
        invalidate_hard_deletes=True,
    )
}}
-- SCD Type 2 on products: price history. dbt versions a product each time
-- `list_price` changes, so facts can (optionally) join point-in-time.
select
    product_id,
    product_name,
    category,
    brand,
    supplier,
    cast(unit_cost as double) as unit_cost,
    cast(list_price as double) as list_price
from {{ silver('products') }}
{% endsnapshot %}

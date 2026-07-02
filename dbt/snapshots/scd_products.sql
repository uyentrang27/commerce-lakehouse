{% snapshot scd_products %}
{{
    config(
        unique_key='product_id',
        strategy='check',
        check_cols=['list_price', 'grade'],
        target_schema='snapshots',
        invalidate_hard_deletes=True,
    )
}}
-- SCD Type 2 on the product master: dbt opens a new version each time a
-- product's `list_price` or refurbished `grade` changes, so facts can join
-- point-in-time and history is preserved.
select
    product_id,
    model_name,
    brand,
    grade,
    cast(unit_cost as double)  as unit_cost,
    cast(list_price as double) as list_price
from {{ silver('products') }}
{% endsnapshot %}

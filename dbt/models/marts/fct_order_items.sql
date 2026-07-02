-- Incremental fact at (order, product) grain.
-- On a normal run dbt only processes order_dates newer than what's already
-- loaded (delete+insert), so refresh cost scales with the delta, not the whole
-- history — the core reason incremental models matter at scale.
{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='delete+insert',
    )
}}

with items as (
    select * from {{ ref('stg_order_items') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
    {% endif %}
)

select
    i.order_item_id,
    i.order_id,
    o.order_date,
    dc.customer_sk,
    dm.marketplace_sk,
    dp.product_sk,
    i.product_id,
    i.category,
    i.brand,
    i.quantity,
    i.gross_amount,
    i.net_amount,
    i.cost_amount,
    i.margin_amount
from items i
join orders o on i.order_id = o.order_id
left join {{ ref('dim_product') }}     dp on i.product_id   = dp.product_id     and dp.is_current
left join {{ ref('dim_customer') }}    dc on o.customer_id  = dc.customer_id    and dc.is_current
left join {{ ref('dim_marketplace') }} dm on o.marketplace_id = dm.marketplace_id

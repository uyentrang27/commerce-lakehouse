-- Incremental fact at order grain: items rolled up to the order, joined to
-- current customer/marketplace dims, with marketplace commission derived.
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),
items as (
    select
        order_id,
        count(*)            as item_count,
        sum(quantity)       as units,
        sum(net_amount)     as net_revenue,
        sum(margin_amount)  as margin_amount
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.order_date,
    dc.customer_sk,
    dm.marketplace_sk,
    o.order_status,
    o.is_returned,
    o.is_cancelled,
    o.commission_rate,
    coalesce(i.item_count, 0)    as item_count,
    coalesce(i.units, 0)         as units,
    coalesce(i.net_revenue, 0)   as net_revenue,
    coalesce(i.margin_amount, 0) as margin_amount,
    round(coalesce(i.net_revenue, 0) * o.commission_rate, 2) as marketplace_commission
from orders o
left join items i on o.order_id = i.order_id
left join {{ ref('dim_customer') }}    dc on o.customer_id   = dc.customer_id   and dc.is_current
left join {{ ref('dim_marketplace') }} dm on o.marketplace_id = dm.marketplace_id

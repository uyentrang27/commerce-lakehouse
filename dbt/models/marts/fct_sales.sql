-- Incremental sales fact at order grain (one row per sold order), conformed
-- from both channels and joined to the current product + channel dimensions.
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
    )
}}

with sales as (
    select * from {{ ref('stg_sales') }}
    {% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
    {% endif %}
)

select
    s.order_id,
    s.order_date,
    dc.channel_sk,
    dp.product_sk,
    s.source,
    s.canonical_status,
    (s.canonical_status = 'RETURNED')                       as is_returned,
    (s.canonical_status in ('DELIVERED', 'SHIPPED'))        as is_settleable,
    s.quantity,
    s.unit_price,
    s.revenue_usd
from sales s
left join {{ ref('dim_channel') }} dc on s.source = dc.channel
left join {{ ref('dim_product') }} dp on s.product_id = dp.product_id and dp.is_current

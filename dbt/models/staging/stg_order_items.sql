select
    cast(order_item_id as bigint) as order_item_id,
    cast(order_id as bigint) as order_id,
    cast(product_id as bigint) as product_id,
    category,
    brand,
    cast(quantity as integer) as quantity,
    cast(unit_price as double) as unit_price,
    cast(discount_pct as double) as discount_pct,
    cast(unit_cost as double) as unit_cost,
    cast(gross_amount as double) as gross_amount,
    cast(net_amount as double) as net_amount,
    cast(cost_amount as double) as cost_amount,
    cast(margin_amount as double) as margin_amount
from {{ silver('order_items') }}

select
    cast(product_id as bigint) as product_id,
    product_name,
    category,
    brand,
    supplier,
    cast(unit_cost as double) as unit_cost,
    cast(list_price as double) as list_price,
    cast(margin as double) as margin
from {{ silver('products') }}

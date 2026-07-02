-- Product master conformed by Silver; the snapshot versions it for SCD Type 2.
select
    cast(product_id as bigint) as product_id,
    model_name,
    brand,
    grade,
    cast(unit_cost  as double) as unit_cost,
    cast(list_price as double) as list_price
from {{ silver('products') }}

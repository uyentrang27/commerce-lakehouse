-- Reads the Silver sales lake (both channels, already conformed + unioned).
-- hive_partitioning recovers order_date from the partition folder path Spark wrote.
select
    order_id,
    source,
    cast(product_id as bigint) as product_id,
    cast(quantity as integer)  as quantity,
    cast(unit_price as double) as unit_price,
    canonical_status,
    cast(revenue_usd as double) as revenue_usd,
    cast(order_date as date)   as order_date
from {{ silver('sales', '**/*.parquet', ', hive_partitioning=1') }}

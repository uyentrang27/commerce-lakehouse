-- Reads the Silver orders lake; hive_partitioning recovers order_date from the
-- partition folder path written by Spark.
select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(marketplace_id as integer) as marketplace_id,
    marketplace_name,
    channel_type,
    cast(commission_rate as double) as commission_rate,
    order_status,
    is_returned,
    is_cancelled,
    currency,
    cast(order_date as date) as order_date
from {{ silver('orders', '**/*.parquet', ', hive_partitioning=1') }}

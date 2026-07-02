select
    cast(marketplace_id as integer) as marketplace_id,
    marketplace_name,
    channel_type,
    country,
    cast(commission_rate as double) as commission_rate
from {{ silver('marketplaces') }}

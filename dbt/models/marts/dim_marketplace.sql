select
    md5(cast(marketplace_id as varchar)) as marketplace_sk,
    marketplace_id,
    marketplace_name,
    channel_type,
    country,
    commission_rate
from {{ ref('stg_marketplaces') }}

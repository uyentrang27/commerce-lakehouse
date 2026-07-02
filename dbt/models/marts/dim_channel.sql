-- Small sales-channel dimension. Derived from the sources present in Silver,
-- with a display name and the marketplace fee rate each channel charges.
with channels as (
    select distinct source as channel from {{ ref('stg_sales') }}
)
select
    md5(channel) as channel_sk,
    channel,
    case channel
        when 'amazon'     then 'Amazon'
        when 'backmarket' then 'BackMarket'
        else channel
    end as channel_name,
    case channel
        when 'amazon'     then 0.15
        when 'backmarket' then 0.12
        else null
    end as fee_rate
from channels

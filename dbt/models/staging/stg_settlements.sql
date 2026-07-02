-- Reads the Silver settlements lake (cash remitted per order, net of fees).
-- Different grain from sales; joined back to sales in mart_reconciliation.
select
    settlement_id,
    channel,
    order_ref,
    cast(gross_usd as double) as gross_usd,
    cast(fees_usd  as double) as fees_usd,
    cast(net_usd   as double) as net_usd,
    cast(payout_date as date) as payout_date
from {{ silver('settlements') }}

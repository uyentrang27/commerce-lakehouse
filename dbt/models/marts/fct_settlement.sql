-- Incremental settlement fact at settlement grain (one row per cash remittance).
-- Kept separate from fct_sales because it is a different grain; the two are
-- reconciled by order reference in mart_reconciliation.
{{
    config(
        materialized='incremental',
        unique_key='settlement_id',
        incremental_strategy='delete+insert',
    )
}}

with settlements as (
    select * from {{ ref('stg_settlements') }}
    {% if is_incremental() %}
    where payout_date > (select max(payout_date) from {{ this }})
    {% endif %}
)

select
    s.settlement_id,
    s.order_ref,
    dc.channel_sk,
    s.channel,
    s.payout_date,
    s.gross_usd,
    s.fees_usd,
    s.net_usd
from settlements s
left join {{ ref('dim_channel') }} dc on s.channel = dc.channel

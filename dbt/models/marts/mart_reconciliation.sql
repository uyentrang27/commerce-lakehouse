-- Financial reconciliation at order grain: booked sales revenue vs cash actually
-- remitted by the OMS. The two sources record different things -- sales books
-- gross revenue at order time; settlement reports net cash after marketplace &
-- shipping fees -- so reconciling them surfaces fees and cash not yet received.
--
--   gap_usd = booked - (net_received + fees)
--     ~ 0 (USD)           -> fully reconciled: net + fees explain the booking
--     small (EUR, settled)-> FX drift, revenue booked at sale-date rate vs cash
--                            converted at payout-date rate (real FX exposure)
--     = booked (unsettled)-> order fulfilled but cash not yet received
with settleable as (
    -- only fulfilled orders are expected to settle
    select * from {{ ref('fct_sales') }} where is_settleable
),
settle as (
    select order_ref, sum(net_usd) as net_usd, sum(fees_usd) as fees_usd
    from {{ ref('fct_settlement') }}
    group by order_ref
)

select
    s.order_id,
    s.source,
    s.order_date,
    s.canonical_status,
    s.revenue_usd                                   as booked_usd,
    t.net_usd                                        as net_received_usd,
    t.fees_usd                                       as fees_usd,
    (t.order_ref is not null)                        as is_settled,
    round(s.revenue_usd - coalesce(t.net_usd, 0) - coalesce(t.fees_usd, 0), 2) as gap_usd
from settleable s
left join settle t on s.order_id = t.order_ref

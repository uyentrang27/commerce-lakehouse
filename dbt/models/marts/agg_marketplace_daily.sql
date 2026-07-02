-- Pre-aggregated serving table: daily marketplace performance.
-- Aggregation tables like this are what keep a BI report (Power BI) fast on
-- large fact tables — the dashboard queries the rollup, not the raw grain.
select
    m.marketplace_name,
    m.channel_type,
    f.order_date,
    count(*)                              as orders,
    sum(f.units)                          as units,
    round(sum(f.net_revenue), 2)          as net_revenue,
    round(sum(f.margin_amount), 2)        as margin,
    round(sum(f.marketplace_commission), 2) as commission,
    sum(case when f.is_returned then 1 else 0 end) as returned_orders,
    round(
        sum(case when f.is_returned then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2
    )                                     as return_rate_pct
from {{ ref('fct_orders') }} f
join {{ ref('dim_marketplace') }} m on f.marketplace_sk = m.marketplace_sk
group by 1, 2, 3

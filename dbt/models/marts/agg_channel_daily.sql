-- Pre-aggregated serving table: daily channel performance. Aggregation tables
-- like this keep a BI report (Power BI) fast on large facts -- the dashboard
-- queries the rollup, not the order-grain fact.
select
    c.channel_name,
    f.order_date,
    count(*)                                    as orders,
    sum(f.quantity)                             as units,
    round(sum(f.revenue_usd), 2)               as booked_usd,
    sum(case when f.is_returned then 1 else 0 end) as returned_orders,
    round(
        sum(case when f.is_returned then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2
    )                                           as return_rate_pct
from {{ ref('fct_sales') }} f
join {{ ref('dim_channel') }} c on f.channel_sk = c.channel_sk
group by 1, 2

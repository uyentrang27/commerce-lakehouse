-- Date spine covering the order window, generated from the fact date range.
with bounds as (
    select min(order_date) as mn, max(order_date) as mx from {{ ref('stg_sales') }}
),
spine as (
    select unnest(generate_series(
        cast(mn as timestamp), cast(mx as timestamp), interval '1 day'
    )) as d
    from bounds
)
select
    cast(d as date)               as date_key,
    extract('year'    from d)     as year,
    extract('quarter' from d)     as quarter,
    extract('month'   from d)     as month,
    strftime(d, '%B')             as month_name,
    extract('day'     from d)     as day_of_month,
    extract('isodow'  from d)     as iso_day_of_week,
    strftime(d, '%A')             as day_name,
    (extract('isodow' from d) >= 6) as is_weekend
from spine

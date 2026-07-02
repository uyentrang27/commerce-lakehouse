select
    cast(customer_id as bigint) as customer_id,
    full_name,
    email,
    city,
    country,
    segment,
    cast(signup_date as date) as signup_date
from {{ silver('customers') }}

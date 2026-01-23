select
    country,
    count(*) as total_customers,
    avg(monthly_spend) as avg_monthly_spend,
    sum(case when premium_subscription then 1 else 0 end) as premium_customers
from {{ ref('stg_e_commerce') }}
group by country
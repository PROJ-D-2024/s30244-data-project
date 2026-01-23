select
    user_id,
    cast(age as integer) as age,
    lower(gender) as gender,
    lower(country) as country,
    cast(monthly_spend as numeric) as monthly_spend,
    cast(weekly_purchases as integer) as weekly_purchases,
    cast(last_purchase_date as date) as last_purchase_date,
    cast(premium_subscription as boolean) as premium_subscription
from {{ source('raw', 'e_commerce_clean') }}
where
    monthly_spend >= 0
    and weekly_purchases >= 0
    and age is not null
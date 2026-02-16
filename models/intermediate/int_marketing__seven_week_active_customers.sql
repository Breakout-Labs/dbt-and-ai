select distinct
    customer_id
from {{ ref('stg_ecomm__orders') }}
where ordered_at > current_date - 49  -- Include orders from last 7 weeks
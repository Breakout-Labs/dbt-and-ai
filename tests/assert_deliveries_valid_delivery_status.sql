select
    stg_ecomm__deliveries.delivery_id,
    stg_ecomm__deliveries.delivery_status
from {{ ref('stg_ecomm__deliveries') }}
where stg_ecomm__deliveries.delivery_status not in ('delivered', 'cancelled', 'pickup')
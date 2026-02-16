with source as (
  select
    *
  from {{ source('ecomm', 'orders') }}
),

renamed as (
  select
    id as order_id,
    *,
    created_at as ordered_at,
    status as order_status
  from source
),

final as (
  select
    *
  from renamed
)

select
  *
from final
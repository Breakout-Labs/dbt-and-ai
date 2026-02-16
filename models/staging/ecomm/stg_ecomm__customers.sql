with source as (
  select
    *
  from {{ source('ecomm', 'customers') }}
),

renamed as (
  select
    id as customer_id,
    *
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
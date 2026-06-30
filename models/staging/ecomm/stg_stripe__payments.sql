with source as (
  select
    json_data
  from {{ source('stripe', 'payments') }}
),

renamed as (
  select
    json_data:id::varchar as payment_id,
    json_data:order_id::varchar as order_id,
    json_data:amount::number(38, 0) as amount_cents,
    json_data:method::varchar as payment_method,
    json_data:payment_provider::varchar as payment_provider,
    json_data:created_at::timestamp_ntz as created_at
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

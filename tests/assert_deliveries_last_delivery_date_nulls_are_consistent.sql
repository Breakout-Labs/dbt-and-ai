-- Test: last_delivery_date must be null if and only if successful_deliveries = 0.
-- A customer with no successful deliveries has no delivered_at timestamp to draw from,
-- so last_delivery_date should always be null in that case — and never null otherwise.

select
    customer_id,
    successful_deliveries,
    last_delivery_date
from {{ ref('deliveries') }}
where
    (successful_deliveries = 0 and last_delivery_date is not null)
    or (successful_deliveries > 0 and last_delivery_date is null)
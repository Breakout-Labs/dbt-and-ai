-- Test: all delivery count columns must be non-negative.
-- Negative values would indicate a data or logic error upstream.

select
    customer_id,
    total_deliveries,
    successful_deliveries,
    failed_deliveries,
    other_status_deliveries
from {{ ref('deliveries') }}
where
    total_deliveries < 0
    or successful_deliveries < 0
    or failed_deliveries < 0
    or other_status_deliveries < 0
-- Test: total_deliveries must equal the sum of its component parts for every customer.
-- A mismatch would indicate a logic error in the aggregation.

select
    customer_id,
    total_deliveries,
    successful_deliveries + failed_deliveries + other_status_deliveries as expected_total_deliveries
from {{ ref('deliveries') }}
where total_deliveries != successful_deliveries + failed_deliveries + other_status_deliveries
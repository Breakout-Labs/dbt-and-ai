-- Asserts that successful_deliveries + failed_deliveries + other_status_deliveries
-- equals total_deliveries for every customer. Any rows returned indicate a failure.
select
    customer_id,
    total_deliveries,
    successful_deliveries,
    failed_deliveries,
    other_status_deliveries
from {{ ref("deliveries") }}
where
    total_deliveries
    != successful_deliveries + failed_deliveries + other_status_deliveries

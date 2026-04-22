-- Asserts that fulfillment_ratio is between 0 and 1 (inclusive) for all rows,
-- and is null only when total_deliveries is 0.
select
    customer_id,
    total_deliveries,
    fulfillment_ratio
from {{ ref('deliveries') }}
where
    fulfillment_ratio < 0
    or fulfillment_ratio > 1
    or (total_deliveries = 0 and fulfillment_ratio is null)
    or (total_deliveries > 0 and fulfillment_ratio is not null)
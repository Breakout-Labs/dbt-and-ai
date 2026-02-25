-- Asserts that fulfillment_ratio is always between 0 and 1 inclusive.
-- Any rows returned indicate a failure.

select
    customer_id,
    fulfillment_ratio
from {{ ref('deliveries') }}
where fulfillment_ratio < 0 or fulfillment_ratio > 1
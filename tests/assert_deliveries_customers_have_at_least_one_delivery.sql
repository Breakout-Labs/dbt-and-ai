-- Test: every customer in this model must have at least one delivery.
-- The model is built from an inner join on orders and deliveries, so a customer
-- with zero total deliveries should never appear. If they do, it signals a join issue.

select
    customer_id,
    total_deliveries
from {{ ref('deliveries') }}
where total_deliveries < 1
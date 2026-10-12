-- Per-endpoint fairness: a cap on concurrent sends and an optional rate
-- limit, both enforced in the claim so every dispatcher replica shares them.
ALTER TABLE endpoints
    ADD COLUMN max_in_flight integer NOT NULL DEFAULT 4 CHECK (max_in_flight > 0),
    -- Requests per second; NULL means unlimited.
    ADD COLUMN rate_limit integer CHECK (rate_limit > 0),
    -- Rate limiter state (GCRA): the time by which every request already
    -- allowed would have been sent at exactly rate_limit per second.
    ADD COLUMN rate_tat timestamptz NOT NULL DEFAULT '-infinity',
    -- Claims go round-robin over endpoints, least recently served first.
    ADD COLUMN last_claimed_at timestamptz;

-- The claim looks up each endpoint's due work and in-flight count.
CREATE INDEX deliveries_due_by_endpoint ON deliveries (endpoint_id, next_attempt_at) WHERE state = 'pending';
CREATE INDEX deliveries_in_flight_by_endpoint ON deliveries (endpoint_id) WHERE state = 'in_flight';

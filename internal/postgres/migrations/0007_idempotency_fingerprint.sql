-- A retried endpoint create must ask for the same endpoint as the first one;
-- the endpoint itself may have been changed since, so the original request
-- is remembered as a fingerprint. Messages and applications cannot be
-- changed, so they are compared directly.
ALTER TABLE endpoints ADD COLUMN idempotency_fingerprint text;

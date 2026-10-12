-- The design doc asks for an Idempotency-Key on every create. Messages have
-- had one since 0001; applications and endpoints get the same: a retried
-- create with the same key returns the first object instead of a second one.
-- NULL keys never collide, so creates without a key are always accepted.

ALTER TABLE applications ADD COLUMN idempotency_key text UNIQUE;

ALTER TABLE endpoints ADD COLUMN idempotency_key text,
    ADD UNIQUE (app_id, idempotency_key);

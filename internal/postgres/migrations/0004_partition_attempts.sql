-- The attempt log grows with every HTTP call, so it is partitioned by day
-- (UTC) and old days are dropped whole instead of deleted row by row.
--
-- Existing attempts are copied into the new table inside this migration,
-- which holds a lock on them while it runs.

-- Partitions are named attempts_YYYYMMDD. Both functions take a transaction
-- advisory lock, so several dispatchers can call them at once.
CREATE FUNCTION create_attempt_partitions(first_day date, last_day date) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    d       date;
    created integer := 0;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('attempt_partitions'));
    FOR d IN SELECT generate_series(first_day, last_day, interval '1 day')::date LOOP
        IF to_regclass('attempts_' || to_char(d, 'YYYYMMDD')) IS NULL THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF attempts FOR VALUES FROM (%L) TO (%L)',
                'attempts_' || to_char(d, 'YYYYMMDD'),
                d::timestamp AT TIME ZONE 'UTC', (d + 1)::timestamp AT TIME ZONE 'UTC');
            created := created + 1;
        END IF;
    END LOOP;
    RETURN created;
END $$;

CREATE FUNCTION drop_attempt_partitions(before_day date) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    part    text;
    dropped integer := 0;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('attempt_partitions'));
    FOR part IN
        SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'attempts'::regclass AND c.relname ~ '^attempts_[0-9]{8}$'
          AND to_date(substr(c.relname, 10), 'YYYYMMDD') < before_day
    LOOP
        EXECUTE format('DROP TABLE %I', part);
        dropped := dropped + 1;
    END LOOP;
    RETURN dropped;
END $$;

ALTER TABLE attempts RENAME TO attempts_old;
DROP INDEX attempts_delivery_id;

-- (attempts_id_seq is the old table's identity sequence, dropped below.)
CREATE SEQUENCE attempt_ids;
CREATE TABLE attempts (
    id            bigint NOT NULL DEFAULT nextval('attempt_ids'),
    delivery_id   text NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
    attempted_at  timestamptz NOT NULL DEFAULT now(),
    status_code   integer,          -- NULL when no response arrived
    latency_ms    integer NOT NULL,
    error         text,             -- network or timeout error, if any
    response_body text,             -- first 4 KB of the response
    PRIMARY KEY (id, attempted_at)  -- a partitioned table's key must include the partition key
) PARTITION BY RANGE (attempted_at);
ALTER SEQUENCE attempt_ids OWNED BY attempts.id;
CREATE INDEX attempts_delivery_id ON attempts (delivery_id, attempted_at);

-- Partitions for every day that has attempts, through a week from now.
SELECT create_attempt_partitions(
    coalesce((SELECT min(attempted_at AT TIME ZONE 'UTC')::date FROM attempts_old),
             (now() AT TIME ZONE 'UTC')::date),
    (now() AT TIME ZONE 'UTC')::date + 7);

INSERT INTO attempts (id, delivery_id, attempted_at, status_code, latency_ms, error, response_body)
SELECT id, delivery_id, attempted_at, status_code, latency_ms, error, response_body FROM attempts_old;
SELECT setval('attempt_ids', coalesce((SELECT max(id) FROM attempts_old), 0) + 1, false);
DROP TABLE attempts_old;

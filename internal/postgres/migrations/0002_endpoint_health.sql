-- Phase 2: endpoint health (circuit breaker, auto-disable) and secret
-- rotation.

ALTER TABLE endpoints
    -- Failed attempts in a row; reset by any success.
    ADD COLUMN consecutive_failures integer NOT NULL DEFAULT 0,
    -- When the current run of failures began; NULL while healthy.
    ADD COLUMN failing_since timestamptz,
    -- Set when the circuit breaker trips. While it is in the future no
    -- deliveries are claimed for the endpoint; once it passes, one probe
    -- delivery is sent. NULL means the circuit is closed.
    ADD COLUMN circuit_open_until timestamptz,
    ADD COLUMN disabled_at timestamptz,
    -- During rotation requests are signed with both secrets until the old
    -- one expires.
    ADD COLUMN previous_secret text,
    ADD COLUMN previous_secret_expires_at timestamptz;

CREATE INDEX endpoints_circuit_open ON endpoints (circuit_open_until)
    WHERE circuit_open_until IS NOT NULL;

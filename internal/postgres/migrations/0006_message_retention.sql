-- Messages are kept for MESSAGE_RETENTION_DAYS once all their deliveries
-- have finished; the sweep finds old ones by creation time. The second index
-- serves the newest-first message list of one app.
CREATE INDEX messages_created_at ON messages (created_at);
CREATE INDEX messages_app_created ON messages (app_id, created_at DESC, id DESC);

-- Every due-work lookup is per endpoint and served by
-- deliveries_due_by_endpoint. This older index is not needed, and with a
-- large backlog the planner picked it anyway and scanned every due row once
-- per endpoint on each claim.
DROP INDEX deliveries_due;


ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS attempts
        INTEGER NOT NULL DEFAULT 0
        CHECK (attempts >= 0),

    ADD COLUMN IF NOT EXISTS started_at
        TIMESTAMPTZ,

    ADD COLUMN IF NOT EXISTS completed_at
        TIMESTAMPTZ,

    ADD COLUMN IF NOT EXISTS output_path
        TEXT,

    ADD COLUMN IF NOT EXISTS error_message
        TEXT;

-- Optimise lookup of queued jobs.
CREATE INDEX IF NOT EXISTS idx_jobs_queued_priority
ON jobs (
    priority DESC,
    created_at ASC,
    id ASC
)
WHERE status = 'queued';

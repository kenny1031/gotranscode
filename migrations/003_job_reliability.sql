
ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS max_attempts
        INTEGER NOT NULL DEFAULT 3
        CHECK (max_attempts BETWEEN 1 AND 10),

    ADD COLUMN IF NOT EXISTS available_at
        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    ADD COLUMN IF NOT EXISTS lease_token
        UUID,

    ADD COLUMN IF NOT EXISTS lease_expires_at
        TIMESTAMPTZ;

-- Recover legacy jobs left in PROCESSING by older workers.
-- Run this migration only after stopping all old workers.
UPDATE jobs
SET
    status = CASE
        WHEN attempts >= max_attempts THEN 'failed'
        ELSE 'retrying'
    END,
    available_at = NOW(),
    lease_token = NULL,
    lease_expires_at = NULL,
    error_message = 'Recovered during reliability migration'
WHERE status = 'processing'
  AND lease_token IS NULL;

-- Index used when claiming ready jobs.
CREATE INDEX IF NOT EXISTS idx_jobs_ready
ON jobs (
    priority DESC,
    available_at ASC,
    created_at ASC,
    id ASC
)
WHERE status IN ('queued', 'retrying');

-- Index used to locate expired leases.
CREATE INDEX IF NOT EXISTS idx_jobs_expired_leases
ON jobs (lease_expires_at)
WHERE status = 'processing';

-- Migration file: 003_worker_tracking.sql
-- Add worker registration and tracking capabilities

BEGIN;

-- Add worker_status table
CREATE TABLE IF NOT EXISTS worker_status (
    worker_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('Active', 'Idle', 'Disconnected')),
    current_task_id UUID REFERENCES tasks(task_id),
    last_heartbeat TIMESTAMP DEFAULT NOW(),
    registered_at TIMESTAMP DEFAULT NOW(),
    capabilities JSONB DEFAULT '{}'::jsonb
);

-- Add indices for common queries
CREATE INDEX IF NOT EXISTS idx_worker_status_current_task ON worker_status(current_task_id) WHERE current_task_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_worker_status_heartbeat ON worker_status(last_heartbeat) WHERE status != 'Disconnected';

-- Add worker_id column to tasks if not exists
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name = 'tasks' AND column_name = 'assigned_worker_id') THEN
        ALTER TABLE tasks ADD COLUMN assigned_worker_id TEXT REFERENCES worker_status(worker_id);
        CREATE INDEX idx_tasks_worker ON tasks(assigned_worker_id) WHERE assigned_worker_id IS NOT NULL;
    END IF;
END $$;

COMMIT;

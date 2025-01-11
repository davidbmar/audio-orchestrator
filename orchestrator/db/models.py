#db/models.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID

@dataclass
class Task:
    task_id: UUID
    object_key: str
    status: str
    created_at: datetime
    updated_at: datetime
    worker_id: Optional[str] = None
    failure_reason: Optional[str] = None
    retries: int = 0
    retry_at: Optional[datetime] = None

    @staticmethod
    def create_table_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id UUID PRIMARY KEY,
                object_key TEXT NOT NULL,
                worker_id TEXT,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                failure_reason TEXT,
                retries INTEGER DEFAULT 0,
                retry_at TIMESTAMP
            );
        """

# orchestrator/db/operations.py

import logging
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from ..config.settings import settings
from .models import Task
import time

logger = logging.getLogger(__name__)

class DatabaseOperations:
    def __init__(self):
        self.settings = settings

    def _get_connection(self, connect_timeout: int = None):
        """Create a new database connection."""
        try:
            host_port = self.settings.DB_HOST.split(':')
            host = host_port[0]
            port = int(host_port[1]) if len(host_port) > 1 else 5432
            
            connect_args = {
                'host': host,
                'port': port,
                'database': self.settings.DB_NAME,
                'user': self.settings.DB_USER,
                'password': self.settings.DB_PASSWORD
            }
            
            if connect_timeout is not None:
                connect_args['connect_timeout'] = connect_timeout

            return psycopg2.connect(**connect_args)
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def verify_connection(self) -> None:
        """Verify database connection with retries."""
        max_retries = 3
        retry_delay = 5  # seconds
        last_error = None

        for attempt in range(max_retries):
            try:
                conn = self._get_connection(connect_timeout=5)
                conn.close()
                return
            except psycopg2.OperationalError as e:
                last_error = e
                if attempt < max_retries - 1:
                    logger.warning(f"Database connection attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                continue
            except Exception as e:
                last_error = e
                break

        logger.critical("Failed to establish database connection")
        raise last_error

    def initialize_tables(self) -> None:
        """Initialize database tables."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # Create tasks table
                cursor.execute(Task.create_table_sql())
                conn.commit()
                logger.info("Database tables initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database tables: {e}")
            raise
        finally:
            conn.close()

    def execute(self, query: str, params: tuple = None) -> None:
        """Execute a database query."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            conn.close()

    def fetch_one(self, query: str, params: tuple = None) -> Optional[tuple]:
        """Fetch a single row from the database."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchone()
        except Exception as e:
            logger.error(f"Error fetching row: {e}")
            raise
        finally:
            conn.close()

    def fetch_all(self, query: str, params: tuple = None) -> List[tuple]:
        """Fetch all rows from the database."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching rows: {e}")
            raise
        finally:
            conn.close()

    def create_task(self, task_id: str, object_key: str) -> None:
        """Create a new task in the database."""
        query = """
            INSERT INTO tasks (task_id, object_key, status)
            VALUES (%s, %s, 'Pending')
        """
        self.execute(query, (task_id, object_key))
        logger.info(f"Created task {task_id} for object {object_key}")

    def get_pending_tasks(self, limit: int = 10) -> List[Task]:
        """Get pending tasks that are ready for processing."""
        query = """
            SELECT task_id, object_key, status, created_at, updated_at, 
                   worker_id, failure_reason, retries, retry_at
            FROM tasks 
            WHERE status = 'Pending'
            AND (retry_at IS NULL OR retry_at <= NOW())
            ORDER BY created_at ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """
        rows = self.fetch_all(query, (limit,))
        return [Task(*row) for row in rows]

    def get_pending_tasks_count(self) -> int:
        """Get count of pending tasks."""
        query = """
            SELECT COUNT(*) 
            FROM tasks 
            WHERE status = 'Pending'
            AND (retry_at IS NULL OR retry_at <= NOW())
        """
        result = self.fetch_one(query)
        return result[0] if result else 0

    def get_next_queued_task(self) -> Optional[Task]:
        """Get the next queued task for processing."""
        query = """
            SELECT task_id, object_key, status, created_at, updated_at, 
                   worker_id, failure_reason, retries, retry_at
            FROM tasks 
            WHERE status = 'Queued'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """
        row = self.fetch_one(query)
        return Task(*row) if row else None

    def update_task_status(self, task_id: str, status: str) -> None:
        """Update the status of a task."""
        query = """
            UPDATE tasks 
            SET status = %s, updated_at = NOW()
            WHERE task_id = %s
        """
        self.execute(query, (status, task_id))
        logger.info(f"Updated task {task_id} status to {status}")

    def mark_task_failed(self, task_id: str, failure_reason: str, retry_interval_minutes: int = 30) -> None:
        """Mark a task as failed with retry information."""
        query = """
            UPDATE tasks 
            SET status = 'Failed',
                failure_reason = %s,
                retries = COALESCE(retries, 0) + 1,
                retry_at = NOW() + INTERVAL '%s minutes',
                updated_at = NOW()
            WHERE task_id = %s
        """
        self.execute(query, (failure_reason, retry_interval_minutes, task_id))
        logger.info(f"Marked task {task_id} as failed: {failure_reason}")

    def reset_stuck_tasks(self, minutes: int = 10) -> None:
        """Reset tasks that have been stuck in progress."""
        query = """
            UPDATE tasks 
            SET status = 'Pending',
                updated_at = NOW()
            WHERE status = 'In-Progress'
            AND updated_at < NOW() - INTERVAL '%s minutes'
        """
        self.execute(query, (minutes,))
        logger.info("Reset stuck tasks to Pending")

    def update_task_worker(self, task_id: str, worker_id: str) -> None:
        """Assign a worker to a task."""
        query = """
            UPDATE tasks 
            SET worker_id = %s,
                updated_at = NOW()
            WHERE task_id = %s
        """
        self.execute(query, (worker_id, task_id))
        logger.info(f"Assigned worker {worker_id} to task {task_id}")

    def get_task_by_id(self, task_id: str) -> Optional[Task]:
        """Get a task by its ID."""
        query = """
            SELECT task_id, object_key, status, created_at, updated_at, 
                   worker_id, failure_reason, retries, retry_at
            FROM tasks 
            WHERE task_id = %s
        """
        row = self.fetch_one(query, (task_id,))
        return Task(*row) if row else None

    def get_tasks_by_status(self, status: str, limit: int = 100) -> List[Task]:
        """Get tasks by their status."""
        query = """
            SELECT task_id, object_key, status, created_at, updated_at, 
                   worker_id, failure_reason, retries, retry_at
            FROM tasks 
            WHERE status = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        rows = self.fetch_all(query, (status, limit))
        return [Task(*row) for row in rows]

    def cleanup_old_tasks(self, days: int = 30) -> None:
        """Clean up old completed tasks."""
        query = """
            DELETE FROM tasks 
            WHERE status = 'Completed'
            AND updated_at < NOW() - INTERVAL '%s days'
        """
        self.execute(query, (days,))
        logger.info(f"Cleaned up completed tasks older than {days} days")

    def get_task_stats(self) -> Dict[str, Any]:
        """Get statistics about tasks."""
        query = """
            SELECT 
                status,
                COUNT(*) as count,
                AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_duration
            FROM tasks
            GROUP BY status
        """
        rows = self.fetch_all(query)
        return {
            row[0]: {
                'count': row[1],
                'avg_duration_seconds': float(row[2]) if row[2] is not None else None
            }
            for row in rows
        }

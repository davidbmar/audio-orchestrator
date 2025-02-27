# orchestrator/services/task_service.py

import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from ..db.operations import DatabaseOperations
from ..services.queue_service import QueueService
from ..utils.s3 import S3Utils
from ..config.settings import settings

logger = logging.getLogger(__name__)

class TaskService:
    def __init__(self):
        self.db = DatabaseOperations()
        self.queue_service = QueueService()
        self.s3_utils = S3Utils()

    def process_s3_event(self, event: dict) -> None:
        try:
            for record in event.get('Records', []):
                if not record.get('eventName', '').startswith('ObjectCreated:'):
                    continue
    
                bucket = record['s3']['bucket']['name']
                encoded_key = record['s3']['object']['key']
                logger.info(f"Received S3 event for key: {encoded_key}")
    
                # Normalize and verify the key using S3Utils
                normalized_key = self.s3_utils.normalize_and_verify_key(encoded_key)
                if not normalized_key:
                    logger.error(f"Could not verify existence of key: {encoded_key}")
                    return
    
                # Use the normalized key as the task identifier
                task_id = normalized_key
                self.db.create_task(task_id, normalized_key)
                logger.info(f"Created task {task_id} for object {normalized_key}")
        except Exception as e:
            logger.error(f"Error processing S3 event: {e}")
            raise

    def _should_process_file(self, bucket: str, key: str) -> bool:
        """
        Determine if a file should be processed based on criteria.
        
        Args:
            bucket (str): The S3 bucket name
            key (str): The S3 object key
            
        Returns:
            bool: True if the file should be processed, False otherwise
        """
        try:
            # Skip if not in input bucket
            if bucket != settings.INPUT_BUCKET:
                return False

            # Skip if in transcriptions folder
            if key.startswith('transcriptions/'):
                return False

            # Get file metadata
            metadata = self.s3_utils.get_object_metadata(bucket, key)
            if not metadata:
                return False

            # Check file size (skip if > 100MB)
            if metadata['size'] > 100 * 1024 * 1024:
                logger.warning(f"File {key} exceeds size limit")
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking if file should be processed: {e}")
            return False

    def process_pending_tasks(self) -> None:
        """Process pending tasks and queue them for workers."""
        try:
            pending_tasks = self.db.get_pending_tasks(limit=10)
            
            for task in pending_tasks:
                logger.info(f"Processing task {task.task_id} with key: {task.object_key}")
                
                # Verify the object still exists
                if not self.s3_utils.verify_object_exists(settings.INPUT_BUCKET, task.object_key):
                    self.db.mark_task_failed(task.task_id, "Input file no longer exists")
                    continue

                # Try to queue the task
                if self.queue_service.send_task_to_queue(task.task_id, task.object_key):
                    self.db.update_task_status(task.task_id, 'Queued')
                    logger.info(f"Task {task.task_id} successfully queued")
                else:
                    self.db.mark_task_failed(task.task_id, 'Failed to queue task')
                    logger.error(f"Failed to queue task {task.task_id}")

        except Exception as e:
            logger.error(f"Error in process_pending_tasks: {e}")
            raise

    def get_task_for_worker(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a queued task and prepare it for a worker.
        
        Args:
            worker_id (str): The ID of the requesting worker
            
        Returns:
            Optional[Dict[str, Any]]: Task details if available, None otherwise
        """
        try:
            task = self.db.get_next_queued_task()
            if not task:
                return None
    
            # Generate presigned URLs
            try:
                get_url, put_url = self.s3_utils.generate_presigned_urls(task.object_key)
            except Exception as e:
                self.db.mark_task_failed(task.task_id, f'Failed to generate presigned URLs: {str(e)}')
                return None
    
            # Update task status and worker - use the actual worker_id passed in
            self.db.update_task_status(task.task_id, 'In-Progress')
            self.db.update_task_worker(task.task_id, worker_id)  # Use the actual worker_id
    
            return {
                'task_id': str(task.task_id),
                'object_key': task.object_key,
                'presigned_get_url': get_url,
                'presigned_put_url': put_url
            }
    
        except Exception as e:
            logger.error(f"Error getting task for worker: {e}")
            return None


    def handle_status_update(self, update: Dict[str, Any]) -> bool:
        """
        Handle a status update from a worker.
        
        Args:
            update (Dict[str, Any]): The status update data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            task_id = update.get('task_id')
            status = update.get('status')
            failure_reason = update.get('failure_reason')
            
            if not task_id or not status:
                logger.error("Invalid status update: missing required fields")
                return False

            task = self.db.get_task_by_id(task_id)
            if not task:
                logger.error(f"Task not found for status update: {task_id}")
                return False

            if status == 'Failed':
                # Handle failed task
                self.db.mark_task_failed(task_id, failure_reason or 'Unknown error')
                # Clean up any partial uploads
                self.s3_utils.clean_up_failed_uploads(task.object_key)
            else:
                # Update task status
                self.db.update_task_status(task_id, status)

            return True

        except Exception as e:
            logger.error(f"Error handling status update: {e}")
            return False

    def cleanup_stuck_tasks(self, timeout_minutes: int = 10) -> None:
        """
        Reset tasks that have been stuck in progress.
        
        Args:
            timeout_minutes (int): Minutes after which to consider a task stuck
        """
        try:
            self.db.reset_stuck_tasks(minutes=timeout_minutes)
            logger.info("Successfully reset stuck tasks")
        except Exception as e:
            logger.error(f"Error cleaning up stuck tasks: {e}")
            raise

    def get_task_stats(self) -> Dict[str, Any]:
        """
        Get statistics about tasks.
        
        Returns:
            Dict[str, Any]: Statistics about tasks
        """
        try:
            stats = self.db.get_task_stats()
            return {
                'stats': stats,
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting task stats: {e}")
            return {}

    def retry_failed_tasks(self) -> None:
        """Retry failed tasks that are eligible for retry."""
        try:
            failed_tasks = self.db.get_tasks_by_status('Failed')
            
            for task in failed_tasks:
                # Skip if not ready for retry
                if task.retry_at and task.retry_at > datetime.utcnow():
                    continue
                    
                # Skip if too many retries
                if task.retries >= 3:
                    logger.warning(f"Task {task.task_id} has exceeded retry limit")
                    continue

                # Reset task to pending
                self.db.update_task_status(task.task_id, 'Pending')
                logger.info(f"Reset failed task {task.task_id} for retry")

        except Exception as e:
            logger.error(f"Error retrying failed tasks: {e}")
            raise

    def cleanup_completed_tasks(self, days: int = 30) -> None:
        """
        Clean up old completed tasks.
        
        Args:
            days (int): Age in days after which to clean up completed tasks
        """
        try:
            self.db.cleanup_old_tasks(days)
            logger.info(f"Cleaned up completed tasks older than {days} days")
        except Exception as e:
            logger.error(f"Error cleaning up completed tasks: {e}")
            raise


    def cleanup_disconnected_workers(self) -> None:
        """Identify and cleanup disconnected workers based on heartbeat timeout."""
        try:
            with self.db._get_connection() as conn:
                with conn.cursor() as cur:
                    # Find workers who haven't sent heartbeat in 2 minutes
                    cur.execute("""
                        UPDATE worker_status
                        SET status = 'Disconnected',
                            current_task_id = NULL
                        WHERE status != 'Disconnected'
                        AND last_heartbeat < NOW() - INTERVAL '2 minutes'
                        RETURNING worker_id, current_task_id
                    """)
    
                    disconnected_workers = cur.fetchall()
    
                    # Reset tasks from disconnected workers
                    for worker_id, task_id in disconnected_workers:
                        if task_id:
                            cur.execute("""
                                UPDATE tasks
                                SET status = 'Pending',
                                    assigned_worker_id = NULL,
                                    updated_at = NOW()
                                WHERE task_id = %s
                                AND status = 'In-Progress'
                            """, (task_id,))
    
                            logger.warning(f"Reset task {task_id} from disconnected worker {worker_id}")
    
                    conn.commit()
    
                    if disconnected_workers:
                        logger.info(f"Cleaned up {len(disconnected_workers)} disconnected workers")
    
        except Exception as e:
            logger.error(f"Error cleaning up disconnected workers: {e}")
            raise
    
    def handle_task_progress_update(self, update: Dict[str, Any]) -> bool:
        """Handle task progress updates from worker heartbeats."""
        try:
            task_id = update.get('task_id')
            if not task_id:
                return True  # No task update needed
    
            # For now we just update the timestamp to show the task is still active
            with self.db._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE tasks
                        SET updated_at = NOW()
                        WHERE task_id = %s
                    """, (task_id,))
                    conn.commit()
            return True
    
        except Exception as e:
            logger.error(f"Error handling task progress update: {e}")
            return False
    
    def reset_task(self, task_id: str) -> None:
        """Reset a task to pending state."""
        try:
            with self.db._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE tasks
                        SET status = 'Pending',
                            assigned_worker_id = NULL,
                            updated_at = NOW()
                        WHERE task_id = %s
                        AND status = 'In-Progress'
                    """, (task_id,))
                    conn.commit()
    
        except Exception as e:
            logger.error(f"Error resetting task {task_id}: {e}")
            raise
    


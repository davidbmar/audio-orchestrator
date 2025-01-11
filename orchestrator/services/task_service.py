#services/task_service.py
import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from ..db.operations import DatabaseOperations
from ..services.queue_service import QueueService
from urllib.parse import unquote

logger = logging.getLogger(__name__)

class TaskService:
    def __init__(self):
        self.db = DatabaseOperations()
        self.queue_service = QueueService()

    def process_s3_event(self, event: Dict[str, Any]) -> None:
        """Process an S3 event and create a task."""
        try:
            for record in event.get('Records', []):
                if record.get('eventName', '').startswith('ObjectCreated:'):
                    encoded_key = record['s3']['object']['key']
                    task_id = str(uuid.uuid4())
                    
                    self.db.create_task(task_id, encoded_key)
                    logger.info(f"Created task {task_id} for object {encoded_key}")

        except Exception as e:
            logger.error(f"Error processing S3 event: {e}")

    def process_pending_tasks(self) -> None:
        """Process pending tasks and queue them for workers."""
        try:
            pending_tasks = self.db.get_pending_tasks(limit=10)
            
            for task in pending_tasks:
                decoded_key = unquote(unquote(task.object_key))
                logger.info(f"Processing task {task.task_id} with key: {decoded_key}")
                
                if self.queue_service.send_task_to_queue(task.task_id, decoded_key):
                    self.db.update_task_status(task.task_id, 'Queued')
                    logger.info(f"Task {task.task_id} successfully queued")
                else:
                    self.db.mark_task_failed(task.task_id, 'Failed to queue task')
                    logger.error(f"Failed to queue task {task.task_id}")

        except Exception as e:
            logger.error(f"Error in process_pending_tasks: {e}")

    def get_task_for_worker(self) -> Optional[Dict[str, Any]]:
        """Get a queued task and prepare it for a worker."""
        try:
            task = self.db.get_next_queued_task()
            if not task:
                return None

            # Generate presigned URLs for the task
            if self.queue_service.send_task_to_queue(task.task_id, task.object_key):
                self.db.update_task_status(task.task_id, 'In-Progress')
                return {
                    'task_id': str(task.task_id),
                    'object_key': task.object_key
                }
            else:
                self.db.mark_task_failed(task.task_id, 'Failed to generate presigned URLs')
                return None

        except Exception as e:
            logger.error(f"Error getting task for worker: {e}")
            return None

    def update_task_status(self, task_id: str, status: str, failure_reason: Optional[str] = None) -> bool:
        """Update the status of a task."""
        try:
            if status == 'Failed':
                self.db.mark_task_failed(task_id, failure_reason)
            else:
                self.db.update_task_status(task_id, status)
            return True

        except Exception as e:
            logger.error(f"Error updating task status: {e}")
            return False

    def cleanup_stuck_tasks(self) -> None:
        """Reset tasks that have been stuck in progress."""
        try:
            self.db.reset_stuck_tasks(minutes=10)
            logger.info("Successfully reset stuck tasks")
        except Exception as e:
            logger.error(f"Error cleaning up stuck tasks: {e}")

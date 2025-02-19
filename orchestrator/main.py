#!/usr/bin/env python3
# orchestrator/main.py

import logging
import sys
import threading
import traceback
from flask import Flask
from typing import Optional
import time

from .api.routes import setup_routes
from .socket_manager import socketio, active_clients  # ✅ Import from new file

from .services.task_service import TaskService
from .services.queue_service import QueueService
from .db.operations import DatabaseOperations
from .config.settings import settings

# Initialize logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('orchestrator.log'),
        logging.StreamHandler()
    ]
)

# Configure specific loggers
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

class AudioTranscriptionOrchestrator:
    def __init__(self):
        self.app: Optional[Flask] = None
        self.db = DatabaseOperations()
        self.task_service = TaskService()
        self.queue_service = QueueService()
        
    def verify_database(self) -> None:
        """Verify database connection before startup."""
        try:
            logger.info("Verifying database connection...")
            self.db.verify_connection()
            logger.info("Database connection verified successfully")
        except Exception as e:
            logger.critical("=" * 50)
            logger.critical("Database connection failed!")
            logger.critical("Please ensure RDS instance is running.")
            logger.critical(f"Current RDS endpoint: {settings.DB_HOST}")
            logger.critical(f"Error: {str(e)}")
            logger.critical("=" * 50)
            raise SystemExit(1)

    def initialize_database(self) -> None:
        """Initialize database tables and perform cleanup."""
        try:
            logger.info("Initializing database...")
            self.db.initialize_tables()
            self.task_service.cleanup_stuck_tasks()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database initialization failed: {str(e)}")
            raise

    def setup_flask_app(self) -> Flask:
        """Initialize and configure Flask application."""
        app = Flask(__name__)
        setup_routes(app, self.task_service)
        return app

    def start_background_threads(self) -> None:
        """Start all background processing threads."""
        threads = [
            # Thread for processing S3 events
            threading.Thread(
                target=self._run_s3_event_processor,
                daemon=True,
                name="s3-event-processor"
            ),
            # Thread for processing tasks
            threading.Thread(
                target=self._run_task_processor,
                daemon=True,
                name="task-processor"
            ),
            # Thread for processing status updates
            threading.Thread(
                target=self._run_status_update_processor,
                daemon=True,
                name="status-update-processor"
            ),
            # Worker cleanup thread
            threading.Thread(
                target=self._run_worker_cleanup,
                daemon=True,
                name="worker-cleanup"
            )
        ]

        for thread in threads:
            thread.start()
            logger.info(f"Started background thread: {thread.name}")

    def _run_s3_event_processor(self) -> None:
        """Background thread for processing S3 events."""
        while True:
            try:
                event = self.queue_service.poll_s3_events()
                if event:
                    self.task_service.process_s3_event(event)
            except Exception as e:
                logger.error(f"Error in S3 event processor: {e}")
            time.sleep(settings.POLL_INTERVAL)

    def _run_task_processor(self) -> None:
        """Background thread for processing pending tasks."""
        last_log_time = time.time()
        while True:
            try:
                pending_tasks = self.db.get_pending_tasks_count()
                current_time = time.time()
                
                if pending_tasks > 0:
                    logger.info(f"Processing {pending_tasks} pending tasks")
                    self.task_service.process_pending_tasks()
                elif current_time - last_log_time >= 300:  # Log every 5 minutes if no tasks
                    logger.debug("No pending tasks to process")
                    last_log_time = current_time
                    
            except Exception as e:
                logger.error(f"Error in task processor: {e}")
            time.sleep(settings.POLL_INTERVAL)

    def _run_status_update_processor(self) -> None:
        """Background thread for processing status updates."""
        while True:
            try:
                update = self.queue_service.poll_status_updates()
                if update:
                    self.task_service.handle_status_update(update)
            except Exception as e:
                logger.error(f"Error in status update processor: {e}")
            time.sleep(settings.POLL_INTERVAL)

    def _run_worker_cleanup(self) -> None:
        """Background thread for cleaning up disconnected workers."""
        while True:
            try:
                self.task_service.cleanup_disconnected_workers()
            except Exception as e:
                logger.error(f"Error in worker cleanup: {e}")
            time.sleep(60)  # Run cleanup every minute


    def run(self) -> None:
        """Main method to start the orchestrator."""
        try:
            logger.info("Starting Audio Transcription Orchestrator")
            
            # Verify and initialize database
            self.verify_database()
            self.initialize_database()
            
            # Start background processing threads
            self.start_background_threads()
            
            # Initialize Flask app
            self.app = self.setup_flask_app()
            
            # Start Flask application
            logger.info("Starting Flask application on port 6000")
            socketio.run(self.app, host='0.0.0.0', port=6000)  
            
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Shutting down...")
        except Exception as e:
            logger.critical("Critical error: %s", str(e))
            logger.critical(traceback.format_exc())
            sys.exit(1)

def main():
    orchestrator = AudioTranscriptionOrchestrator()
    orchestrator.run()

if __name__ == "__main__":
    main()

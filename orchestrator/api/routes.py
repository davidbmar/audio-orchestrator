# orchestrator/api/routes.py

from flask import Blueprint, request, jsonify, Flask
from flask_socketio import emit

from functools import wraps
import json
import logging
from typing import Callable, Any, Dict
from ..db.operations import DatabaseOperations
from ..config.settings import settings
from ..utils.s3 import S3Utils
from ..socket_manager import socketio, active_clients  # ✅ Fix import
from ..services.task_service import TaskService
s3_utils = S3Utils()  # Create an instance of S3Utils


logger = logging.getLogger(__name__)
task_service = TaskService()
api = Blueprint('api', __name__)

def authenticate(f: Callable) -> Callable:
    """Authentication decorator for routes."""
    @wraps(f)
    def decorated(*args: Any, **kwargs: Any) -> Any:
        auth_header = request.headers.get('Authorization')
        
        if not auth_header:
            logger.warning("Request missing authorization header")
            return jsonify({'error': 'No authorization header'}), 401
        
        try:
            # Expected format: "Bearer <token>"
            scheme, token = auth_header.split()
            if scheme.lower() != 'bearer':
                logger.warning("Invalid authentication scheme used")
                return jsonify({'error': 'Invalid authentication scheme'}), 401
            
            if token != settings.API_TOKEN:
                logger.warning("Invalid API token used")
                return jsonify({'error': 'Invalid token'}), 401
                
        except ValueError:
            logger.warning("Malformed authorization header")
            return jsonify({'error': 'Invalid authorization header format'}), 401
            
        return f(*args, **kwargs)
    return decorated

def validate_json(*required_fields: str) -> Callable:
    """Decorator to validate required JSON fields in request."""
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated(*args: Any, **kwargs: Any) -> Any:
            data = request.get_json()
            if not data:
                logger.warning("Request missing JSON body")
                return jsonify({'error': 'No JSON data provided'}), 400
                
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                logger.warning(f"Request missing required fields: {missing_fields}")
                return jsonify({'error': f'Missing required fields: {missing_fields}'}), 400
                
            return f(*args, **kwargs)
        return decorated
    return decorator

def setup_routes(app: Flask, task_service: TaskService) -> None:
    """Setup all routes with the Flask app."""
    
    @app.route('/health', methods=['GET'])
    def health_check() -> tuple:
        """Health check endpoint."""
        return jsonify({
            'status': 'healthy',
            'service': 'audio-transcription-orchestrator'
        }), 200

    @app.route('/verify-token', methods=['POST'])
    @authenticate
    def verify_token() -> tuple:
        """Verify API token endpoint."""
        return jsonify({'message': 'Token is valid'}), 200

    @app.route('/get-task', methods=['GET'])
    @authenticate
    def get_task() -> tuple:
        """Get a task for processing."""
        try:
            # Get worker ID from header - REQUIRED
            worker_id = request.headers.get('X-Worker-ID')
            if not worker_id:
                logger.error("No worker ID provided in request headers")
                return jsonify({'error': 'Worker ID required'}), 400
            
            task = task_service.get_task_for_worker(worker_id)
            if not task:
                return jsonify({'message': 'No tasks available'}), 204
                
            logger.info(f"Assigned task {task['task_id']} to worker {worker_id}")
            return jsonify(task), 200
            
        except Exception as e:
            logger.error(f"Error in get-task: {e}")
            return jsonify({'error': 'Internal server error'}), 500

    @app.route("/worker/transcription-result", methods=["POST"])
    @authenticate
    @validate_json("task_id", "transcription")
    def receive_transcription():
        """Receive a transcription result from a worker and forward it to the client."""
        data = request.get_json()
        task_id = data["task_id"]
        transcription = data["transcription"]

        logging.info(f"Received transcription for task {task_id}")

        # 1️⃣ Send transcription to WebSocket clients
        if task_id in active_clients:
            socketio.emit(
                "transcription_complete",
                {"task_id": task_id, "transcription": transcription},
                room=task_id
            )
            logging.info(f"Sent real-time update to client {task_id}")

        # 2️⃣ Store transcription in S3 for later access
        s3_utils.store_transcription_in_s3(task_id, transcription)


        return jsonify({"message": "Transcription received and processed"}), 200
   
    # Provide API Backup for Clients Who Are Offline
    # If a client was offline when the transcription was completed, they should be able to fetch it from S3.
    @api.route('/task/<task_id>/transcription', methods=['GET'])
    @authenticate
    def get_transcription_url(task_id: str) -> tuple:
        """Fetch a pre-signed S3 URL for the transcription."""
        s3_url = generate_presigned_s3_url(task_id)
        if not s3_url:
            return jsonify({'error': 'Transcription not found'}), 404
        
        return jsonify({'transcription_url': s3_url}), 200
    


    @app.route('/update-task-status', methods=['POST'])
    @authenticate
    @validate_json('task_id', 'status')
    def update_task_status() -> tuple:
        """Update task status endpoint."""
        try:
            data = request.get_json()
            success = task_service.handle_status_update(data)
            
            if success:
                logger.info(f"Updated status for task {data['task_id']} to {data['status']}")
                return jsonify({'message': 'Status updated successfully'}), 200
            else:
                logger.error(f"Failed to update status for task {data['task_id']}")
                return jsonify({'error': 'Failed to update task status'}), 400
                
        except Exception as e:
            logger.error(f"Error in update-task-status: {e}")
            return jsonify({'error': 'Internal server error'}), 500

    @app.route('/task/<task_id>', methods=['GET'])
    @authenticate
    def get_task_status(task_id: str) -> tuple:
        """Get status of a specific task."""
        try:
            task = task_service.get_task_by_id(task_id)
            if not task:
                return jsonify({'error': 'Task not found'}), 404
                
            return jsonify({
                'task_id': str(task.task_id),
                'status': task.status,
                'created_at': task.created_at.isoformat(),
                'updated_at': task.updated_at.isoformat(),
                'failure_reason': task.failure_reason
            }), 200
            
        except Exception as e:
            logger.error(f"Error getting task status: {e}")
            return jsonify({'error': 'Internal server error'}), 500

    @app.route('/stats', methods=['GET'])
    @authenticate
    def get_stats() -> tuple:
        """Get task processing statistics."""
        try:
            stats = task_service.get_task_stats()
            return jsonify(stats), 200
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return jsonify({'error': 'Internal server error'}), 500

    @app.route('/retry-task/<task_id>', methods=['POST'])
    @authenticate
    def retry_task(task_id: str) -> tuple:
        """Manually retry a failed task."""
        try:
            task = task_service.get_task_by_id(task_id)
            if not task:
                return jsonify({'error': 'Task not found'}), 404
                
            if task.status != 'Failed':
                return jsonify({'error': 'Can only retry failed tasks'}), 400
                
            task_service.retry_failed_tasks([task_id])
            return jsonify({'message': 'Task queued for retry'}), 200
            
        except Exception as e:
            logger.error(f"Error retrying task: {e}")
            return jsonify({'error': 'Internal server error'}), 500

    @app.route('/worker/register', methods=['POST'])
    @authenticate
    @validate_json('worker_id', 'capabilities')
    def register_worker() -> tuple:
        """Register a new worker or update existing registration."""
        try:
            data = request.get_json()
            worker_id = data['worker_id']
            capabilities = data['capabilities']
            
            db = DatabaseOperations()
            with db._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO worker_status (worker_id, status, capabilities) 
                        VALUES (%s, 'Idle', %s)
                        ON CONFLICT (worker_id) DO UPDATE 
                        SET status = 'Idle',
                            last_heartbeat = NOW(),
                            capabilities = %s
                        RETURNING worker_id
                    """, (worker_id, json.dumps(capabilities), json.dumps(capabilities)))
                    conn.commit()
            
            logger.info(f"Worker {worker_id} registered successfully")
            return jsonify({'status': 'registered'}), 200
            
        except Exception as e:
            logger.error(f"Error registering worker: {e}")
            return jsonify({'error': 'Registration failed'}), 500

    @app.route('/worker/heartbeat', methods=['POST'])
    @authenticate
    @validate_json('worker_id')  # Remove task_status from required fields
    def worker_heartbeat() -> tuple:
        """Update worker heartbeat and task status."""
        try:
            data = request.get_json()
            worker_id = data['worker_id']
            task_status = data.get('task_status', {})  # Default to empty dict if not present
            
            if task_status is None:  # Handle None case explicitly
                task_status = {}
            
            db = DatabaseOperations()
            with db._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE worker_status 
                        SET last_heartbeat = NOW(),
                            status = CASE 
                                WHEN %s::jsonb ? 'task_id' THEN 'Active'
                                ELSE 'Idle'
                            END
                        WHERE worker_id = %s
                        RETURNING worker_id
                    """, (json.dumps(task_status), worker_id))
                    
                    if not cur.fetchone():
                        return jsonify({'error': 'Worker not found'}), 404
                    
                    if task_status and task_status.get('task_id'):
                        task_service.handle_task_progress_update(task_status)
                    
                    conn.commit()
            
            return jsonify({'status': 'updated'}), 200
            
        except Exception as e:
            logger.error(f"Error updating worker heartbeat: {e}")
            return jsonify({'error': 'Heartbeat update failed'}), 500
    
    
    @app.route('/worker/disconnect', methods=['POST'])
    @authenticate
    @validate_json('worker_id')
    def disconnect_worker() -> tuple:
        """Gracefully disconnect a worker."""
        try:
            data = request.get_json()
            worker_id = data['worker_id']
            
            db = DatabaseOperations()
            with db._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE worker_status 
                        SET status = 'Disconnected',
                            current_task_id = NULL 
                        WHERE worker_id = %s
                        RETURNING current_task_id
                    """, (worker_id,))
                    
                    result = cur.fetchone()
                    if not result:
                        return jsonify({'error': 'Worker not found'}), 404
                    
                    if result[0]:
                        task_service.reset_task(result[0])
                    
                    conn.commit()
            
            return jsonify({'status': 'disconnected'}), 200
            
        except Exception as e:
            logger.error(f"Error disconnecting worker: {e}")
            return jsonify({'error': 'Disconnect failed'}), 500



    @app.errorhandler(404)
    def not_found(e: Any) -> tuple:
        """Handle 404 errors."""
        return jsonify({'error': 'Not found'}), 404

    @app.errorhandler(500)
    def internal_error(e: Any) -> tuple:
        """Handle 500 errors."""
        logger.error(f"Internal server error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

    # Register the blueprint
    app.register_blueprint(api, url_prefix='/api/v1')

    logger.info("API routes initialized successfully")

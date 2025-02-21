from flask_socketio import SocketIO, emit, join_room, disconnect
from flask import request  # Correct import for request context

import logging
import uuid

logger = logging.getLogger(__name__)

class SocketManager:
    def __init__(self):
        self.socketio = None
        self.active_clients = {}

    def init_app(self, app):
        self.socketio = SocketIO(app, cors_allowed_origins="*")
       
        # handle_connect function 
        # [BACKEND] Generate UUID when the client connects (in socket_manager.py).
        @self.socketio.on('connect')
        def handle_connect(auth):
            """Handle client connection and assign a UUID"""
            new_uuid = str(uuid.uuid4())
            client_session_id = request.sid  # Unique session ID for each connection
        
            # Store UUID associated with this client
            self.active_clients[client_session_id] = new_uuid
            logger.info(f"Client connected: {client_session_id} | UUID: {new_uuid}")
        
            emit('connection_response', {'status': 'connected', 'uuid': new_uuid})
        
        @self.socketio.on('register_for_updates')
        def handle_registration(data):
            """Register client for updates about specific tasks"""
            task_id = data.get('task_id')
            if task_id:
                join_room(task_id)
                emit('registration_complete', {'task_id': task_id})
                logger.info(f"Client registered for updates on task {task_id}")

        @self.socketio.on('test_transcription_request')
        def handle_test_request(data):
            """Handle test transcription request"""
            logger.info("Received test transcription request")
            self.socketio.emit('test_transcription', {
                'task_id': 'test-123',
                'transcription': 'This is a test transcription message.'
            })

    def emit_transcription_complete(self, task_id, transcription):
        """Emit transcription result to clients"""
        if self.socketio:
            self.socketio.emit('transcription_complete', {
                'task_id': task_id,
                'transcription': transcription
            }, room=task_id)

# Create singleton instance
socket_manager = SocketManager()

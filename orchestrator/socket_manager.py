from flask_socketio import SocketIO, emit, join_room
import logging

logger = logging.getLogger(__name__)

class SocketManager:
    def __init__(self):
        self.socketio = None
        self.active_clients = {}

    def init_app(self, app):
        self.socketio = SocketIO(app, cors_allowed_origins="*")
        
        @self.socketio.on('connect')
        def handle_connect():
            """Handle client connection"""
            logger.info("Client connected")
            emit('connection_response', {'status': 'connected'})

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

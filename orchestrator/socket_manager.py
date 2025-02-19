# In orchestrator/socket_manager.py
from flask_socketio import SocketIO, emit


# Create WebSocket server instance
socketio = SocketIO(cors_allowed_origins="*")

# Keep track of active clients
active_clients = {}

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    emit('connection_response', {'status': 'connected'})

@socketio.on('register_for_updates')
def handle_registration(data):
    """Register client for updates about specific tasks"""
    task_id = data.get('task_id')
    if task_id:
        # Join a room specific to this task
        join_room(task_id)
        emit('registration_complete', {'task_id': task_id})

# In your worker completion handler
def handle_transcription_complete(task_id, transcription):
    """Emit transcription result to clients"""
    socketio.emit('transcription_complete', {
        'task_id': task_id,
        'transcription': transcription
    }, room=task_id)


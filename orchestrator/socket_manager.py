from flask_socketio import SocketIO

# Create WebSocket server instance
socketio = SocketIO(cors_allowed_origins="*")

# Keep track of active clients
active_clients = {}


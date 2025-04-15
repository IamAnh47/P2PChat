import socket
import json
import threading
import logging
from datetime import datetime

class Tracker:
    def __init__(self, host='0.0.0.0', port=8000):
        self.host = host
        self.port = port
        self.server_socket = None
        self.peers = {}  # Format: {peer_id: {'ip': ip, 'port': port, 'username': username, 'session_id': session_id}}
        self.lock = threading.Lock()  # For thread safety
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            filename='tracker.log',
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('Tracker')
        
    def save_peers(self):
        """Save current peer list to peers.json"""
        with open('peers.json', 'w') as f:
            json.dump(self.peers, f)
        self.logger.info(f"Saved {len(self.peers)} peers to peers.json")
            
    def load_peers(self):
        """Load peers from peers.json if exists"""
        try:
            with open('peers.json', 'r') as f:
                self.peers = json.load(f)
            self.logger.info(f"Loaded {len(self.peers)} peers from peers.json")
        except FileNotFoundError:
            self.logger.info("No existing peers.json found, starting with empty peer list")
            
    def start(self):
        """Start the tracker server"""
        self.load_peers()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.logger.info(f"Tracker started on {self.host}:{self.port}")
            
            while True:
                client_socket, addr = self.server_socket.accept()
                self.logger.info(f"New connection from {addr[0]}:{addr[1]}")
                
                # Handle client connection in a new thread
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, addr)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            self.logger.error(f"Server error: {str(e)}")
        finally:
            if self.server_socket:
                self.server_socket.close()
                
    def handle_client(self, client_socket, addr):
        """Handle client connection and commands"""
        try:
            # Receive data from client
            data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                
                # Check if we've received a complete JSON object
                try:
                    decoded_data = data.decode('utf-8')
                    json.loads(decoded_data)  # Test if valid JSON
                    break  # If we can parse it, we've received the full message
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue  # Not complete JSON, wait for more data
                    
            if not data:
                return
                
            # Parse the command
            try:
                command_data = json.loads(data.decode('utf-8'))
                command = command_data.get('command')
                
                if command == 'submit_info':
                    response = self.handle_submit_info(command_data, addr)
                elif command == 'get_list':
                    response = self.handle_get_list(command_data)
                else:
                    response = {"status": "error", "message": "Unknown command"}
                    
                # Send response
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                self.logger.info(f"Sent response for command {command}")
                
            except json.JSONDecodeError:
                error_msg = {"status": "error", "message": "Invalid JSON format"}
                client_socket.sendall(json.dumps(error_msg).encode('utf-8'))
                self.logger.error("Received invalid JSON data")
                
        except Exception as e:
            self.logger.error(f"Error handling client: {str(e)}")
        finally:
            client_socket.close()
            
    def handle_submit_info(self, data, addr):
        """Handle submit_info command to register a peer"""
        try:
            peer_port = data.get('port')
            username = data.get('username')
            session_id = data.get('session_id', datetime.now().strftime("%Y%m%d%H%M%S"))
            
            # Generate unique peer ID
            peer_id = f"{addr[0]}:{peer_port}:{username}:{session_id}"
            
            # Add to peers list with thread safety
            with self.lock:
                self.peers[peer_id] = {
                    'ip': addr[0],
                    'port': peer_port,
                    'username': username,
                    'session_id': session_id,
                    'last_seen': datetime.now().isoformat()
                }
                self.save_peers()  # Save to file
                
            self.logger.info(f"Registered new peer: {peer_id}")
            return {"status": "success", "peer_id": peer_id}
            
        except KeyError as e:
            self.logger.error(f"Missing required field in submit_info: {str(e)}")
            return {"status": "error", "message": f"Missing required field: {str(e)}"}
            
    def handle_get_list(self, data):
        """Handle get_list command to return peer list"""
        # Optional filtering criteria could be added here
        with self.lock:
            return {
                "status": "success",
                "peers": self.peers
            }
            
    def stop(self):
        """Stop the tracker server"""
        if self.server_socket:
            self.server_socket.close()
            self.logger.info("Tracker server stopped")
            
if __name__ == "__main__":
    tracker = Tracker()
    try:
        tracker.start()
    except KeyboardInterrupt:
        tracker.stop()
        print("Tracker stopped")
import socket
import threading
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(filename='tracker_log.txt', level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

class Tracker:
    def __init__(self, host='localhost', port=9000):
        """
        Initialize the Tracker (centralized server)
        
        Parameters:
        - host: Server host address
        - port: Server port number
        """
        self.host = host
        self.port = port
        self.peers = {}  # Dictionary to store peer information: {peer_id: {"ip": ip, "port": port, "username": username, "session_id": session_id, "status": status, "last_seen": timestamp}}
        self.channels = {}  # Dictionary to store channel information: {channel_id: {"name": name, "owner": owner_id, "messages": [messages], "access_control": access_control}}
        self.users = {}  # Dictionary to store user information: {username: {"password": password, "status": status}}
        self.lock = threading.Lock()  # Lock for thread safety
        self.server_socket = None
        
        # Add some default users for testing
        self.users = {
            "admin": {"password": "admin123", "status": "offline"},
            "user1": {"password": "user123", "status": "offline"},
            "user2": {"password": "user123", "status": "offline"}
        }
        
        # Add some default channels for testing
        self.channels = {
            "general": {"name": "General", "owner": "admin", "messages": [], "access_control": {"visitors_allowed": True}},
            "random": {"name": "Random", "owner": "admin", "messages": [], "access_control": {"visitors_allowed": True}}
        }
    
    def start(self):
        """Start the tracker server"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            
            logging.info(f"Tracker started on {self.host}:{self.port}")
            print(f"Tracker started on {self.host}:{self.port}")
            
            # Start the cleanup thread for inactive peers
            cleanup_thread = threading.Thread(target=self.cleanup_inactive_peers)
            cleanup_thread.daemon = True
            cleanup_thread.start()
            
            while True:
                client_socket, address = self.server_socket.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, address))
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            logging.error(f"Error starting tracker: {e}")
            print(f"Error starting tracker: {e}")
            
        finally:
            if self.server_socket:
                self.server_socket.close()
    
    def handle_client(self, client_socket, address):
        """
        Handle client connections and requests
        
        Parameters:
        - client_socket: Socket object for client connection
        - address: Client address tuple (ip, port)
        """
        try:
            logging.info(f"New connection from {address}")
            
            while True:
                # Receive data from client
                data = client_socket.recv(4096)
                if not data:
                    break
                
                # Parse the received JSON data
                request = json.loads(data.decode('utf-8'))
                logging.info(f"Received request from {address}: {request}")
                
                # Process the request based on its type
                if request.get('type') == 'submit_info':
                    response = self.handle_submit_info(request, address)
                elif request.get('type') == 'get_list':
                    response = self.handle_get_list(request)
                elif request.get('type') == 'authenticate':
                    response = self.handle_authenticate(request)
                elif request.get('type') == 'register_visitor':
                    response = self.handle_register_visitor(request, address)
                elif request.get('type') == 'get_channels':
                    response = self.handle_get_channels(request)
                elif request.get('type') == 'get_channel_messages':
                    response = self.handle_get_channel_messages(request)
                elif request.get('type') == 'add_message':
                    response = self.handle_add_message(request)
                elif request.get('type') == 'sync_channel':
                    response = self.handle_sync_channel(request)
                elif request.get('type') == 'update_status':
                    response = self.handle_update_status(request)
                elif request.get('type') == 'ping':
                    response = {'status': 'success', 'message': 'pong'}
                else:
                    response = {'status': 'error', 'message': 'Unknown request type'}
                
                # Send response back to client
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                logging.info(f"Sent response to {address}: {response}")
                
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON received from {address}")
            client_socket.sendall(json.dumps({'status': 'error', 'message': 'Invalid JSON'}).encode('utf-8'))
            
        except Exception as e:
            logging.error(f"Error handling client {address}: {e}")
            try:
                client_socket.sendall(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8'))
            except:
                pass
                
        finally:
            client_socket.close()
            logging.info(f"Connection closed with {address}")
    
    def handle_submit_info(self, request, address):
        """
        Handle submit_info request from a peer
        
        Parameters:
        - request: The submit_info request data
        - address: The client's address tuple
        
        Returns:
        - Response dictionary
        """
        with self.lock:
            # Generate a unique peer_id
            peer_id = f"{request.get('username', 'anonymous')}_{address[0]}_{request.get('port')}_{int(time.time())}"
            
            # Add peer to the tracking list
            self.peers[peer_id] = {
                "ip": address[0],
                "port": request.get('port'),
                "username": request.get('username', 'anonymous'),
                "session_id": request.get('session_id', ''),
                "status": request.get('status', 'online'),
                "last_seen": time.time()
            }
            
            logging.info(f"Added peer {peer_id} to tracking list")
            
            return {
                'status': 'success',
                'peer_id': peer_id,
                'message': 'Peer information submitted successfully'
            }
    
    def handle_get_list(self, request):
        """
        Handle get_list request from a peer
        
        Parameters:
        - request: The get_list request data
        
        Returns:
        - Response dictionary with peer list
        """
        with self.lock:
            # Filter peers based on request parameters
            filtered_peers = {}
            for peer_id, peer_info in self.peers.items():
                # Skip peers that don't match the filter criteria
                if request.get('filter_status') and peer_info['status'] != request.get('filter_status'):
                    continue
                if request.get('filter_username') and peer_info['username'] != request.get('filter_username'):
                    continue
                
                # Add to filtered list
                filtered_peers[peer_id] = peer_info
            
            return {
                'status': 'success',
                'peers': filtered_peers,
                'message': 'Peer list retrieved successfully'
            }
    
    def handle_authenticate(self, request):
        """
        Handle authentication request from a user
        
        Parameters:
        - request: The authentication request data
        
        Returns:
        - Response dictionary
        """
        username = request.get('username')
        password = request.get('password')
        
        with self.lock:
            if username in self.users and self.users[username]['password'] == password:
                # Update user status
                self.users[username]['status'] = 'online'
                
                return {
                    'status': 'success',
                    'message': 'Authentication successful',
                    'username': username,
                    'session_id': f"session_{username}_{int(time.time())}"
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Invalid username or password'
                }
    
    def handle_register_visitor(self, request, address):
        """
        Handle visitor registration request
        
        Parameters:
        - request: The visitor registration request data
        - address: The client's address tuple
        
        Returns:
        - Response dictionary
        """
        visitor_name = request.get('visitor_name', f"visitor_{address[0]}_{int(time.time())}")
        
        peer_id = f"{visitor_name}_{address[0]}_{request.get('port')}_{int(time.time())}"
        
        with self.lock:
            # Add visitor to the tracking list
            self.peers[peer_id] = {
                "ip": address[0],
                "port": request.get('port'),
                "username": visitor_name,
                "session_id": f"visitor_{int(time.time())}",
                "status": "visitor",
                "last_seen": time.time()
            }
            
            return {
                'status': 'success',
                'peer_id': peer_id,
                'visitor_name': visitor_name,
                'message': 'Visitor registered successfully'
            }
    
    def handle_get_channels(self, request):
        """
        Handle get_channels request
        
        Parameters:
        - request: The get_channels request data
        
        Returns:
        - Response dictionary with channel list
        """
        with self.lock:
            # If visitor mode, filter channels that allow visitors
            if request.get('visitor_mode', False):
                visitor_channels = {}
                for channel_id, channel_info in self.channels.items():
                    if channel_info['access_control'].get('visitors_allowed', False):
                        visitor_channels[channel_id] = {
                            'name': channel_info['name'],
                            'owner': channel_info['owner']
                        }
                
                return {
                    'status': 'success',
                    'channels': visitor_channels,
                    'message': 'Channel list retrieved successfully'
                }
            else:
                # Return all channels for authenticated users
                channel_list = {}
                for channel_id, channel_info in self.channels.items():
                    channel_list[channel_id] = {
                        'name': channel_info['name'],
                        'owner': channel_info['owner']
                    }
                
                return {
                    'status': 'success',
                    'channels': channel_list,
                    'message': 'Channel list retrieved successfully'
                }
    
    def handle_get_channel_messages(self, request):
        """
        Handle get_channel_messages request
        
        Parameters:
        - request: The get_channel_messages request data
        
        Returns:
        - Response dictionary with channel messages
        """
        channel_id = request.get('channel_id')
        
        with self.lock:
            if channel_id in self.channels:
                # Check if visitor is allowed for this channel
                if request.get('visitor_mode', False) and not self.channels[channel_id]['access_control'].get('visitors_allowed', False):
                    return {
                        'status': 'error',
                        'message': 'Visitors are not allowed in this channel'
                    }
                
                # Return channel messages
                return {
                    'status': 'success',
                    'channel_id': channel_id,
                    'channel_name': self.channels[channel_id]['name'],
                    'messages': self.channels[channel_id]['messages'],
                    'message': 'Channel messages retrieved successfully'
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Channel not found'
                }
    
    def handle_add_message(self, request):
        """
        Handle add_message request
        
        Parameters:
        - request: The add_message request data
        
        Returns:
        - Response dictionary
        """
        channel_id = request.get('channel_id')
        username = request.get('username')
        message_content = request.get('message')
        session_id = request.get('session_id', '')
        
        with self.lock:
            if channel_id in self.channels:
                # Check if it's a visitor trying to add a message
                if request.get('visitor_mode', False):
                    return {
                        'status': 'error',
                        'message': 'Visitors are not allowed to add messages'
                    }
                
                # Add message to channel
                message = {
                    'id': len(self.channels[channel_id]['messages']) + 1,
                    'username': username,
                    'content': message_content,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                self.channels[channel_id]['messages'].append(message)
                
                return {
                    'status': 'success',
                    'message_id': message['id'],
                    'message': 'Message added successfully'
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Channel not found'
                }
    
    def handle_sync_channel(self, request):
        """
        Handle sync_channel request
        
        Parameters:
        - request: The sync_channel request data
        
        Returns:
        - Response dictionary
        """
        channel_id = request.get('channel_id')
        channel_data = request.get('channel_data')
        username = request.get('username')
        session_id = request.get('session_id', '')
        
        with self.lock:
            # Check if user is authenticated and is the channel owner
            if channel_id in self.channels and self.channels[channel_id]['owner'] == username:
                # Merge messages between channel hosting and centralized server
                if 'messages' in channel_data:
                    # Get the latest messages from both sources
                    server_messages = self.channels[channel_id]['messages']
                    client_messages = channel_data['messages']
                    
                    # Create a merged list of messages
                    merged_messages = []
                    message_ids = set()
                    
                    # Add server messages first
                    for msg in server_messages:
                        message_ids.add(msg['id'])
                        merged_messages.append(msg)
                    
                    # Add client messages that don't exist on server
                    for msg in client_messages:
                        if msg['id'] not in message_ids:
                            merged_messages.append(msg)
                            message_ids.add(msg['id'])
                    
                    # Sort messages by timestamp
                    merged_messages.sort(key=lambda x: x['timestamp'])
                    
                    # Update server channel with merged messages
                    self.channels[channel_id]['messages'] = merged_messages
                
                # Update access control settings if provided
                if 'access_control' in channel_data:
                    self.channels[channel_id]['access_control'] = channel_data['access_control']
                
                return {
                    'status': 'success',
                    'channel_id': channel_id,
                    'channel_data': self.channels[channel_id],
                    'message': 'Channel synchronized successfully'
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Channel not found or you are not the owner'
                }
    
    def handle_update_status(self, request):
        """
        Handle update_status request
        
        Parameters:
        - request: The update_status request data
        
        Returns:
        - Response dictionary
        """
        username = request.get('username')
        status = request.get('status')
        peer_id = request.get('peer_id')
        
        with self.lock:
            # Update user status
            if username in self.users:
                self.users[username]['status'] = status
            
            # Update peer status
            if peer_id in self.peers:
                self.peers[peer_id]['status'] = status
                self.peers[peer_id]['last_seen'] = time.time()
            
            return {
                'status': 'success',
                'message': 'Status updated successfully'
            }
    
    def cleanup_inactive_peers(self):
        """Periodically remove inactive peers from the tracking list"""
        while True:
            time.sleep(60)  # Check every minute
            
            with self.lock:
                current_time = time.time()
                inactive_peers = []
                
                # Find inactive peers (no activity for more than 5 minutes)
                for peer_id, peer_info in self.peers.items():
                    if current_time - peer_info['last_seen'] > 300:  # 5 minutes
                        inactive_peers.append(peer_id)
                
                # Remove inactive peers
                for peer_id in inactive_peers:
                    del self.peers[peer_id]
                    logging.info(f"Removed inactive peer: {peer_id}")

if __name__ == "__main__":
    # Get host and port from command line arguments or use defaults
    import sys
    host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    
    # Create and start the tracker
    tracker = Tracker(host, port)
    tracker.start()
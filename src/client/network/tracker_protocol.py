import socket
import json
import logging
from typing import Dict, Any, Optional, Tuple, List

class TrackerProtocol:
    def __init__(self, tracker_host: str, tracker_port: int):
        """Initialize the tracker protocol handler
        
        Args:
            tracker_host (str): Hostname or IP of the tracker server
            tracker_port (int): Port number of the tracker server
        """
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.logger = logging.getLogger('TrackerProtocol')
        
    def _send_request(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request to the tracker and return the response
        
        Args:
            data (Dict[str, Any]): JSON-serializable request data
            
        Returns:
            Dict[str, Any]: Response data from the tracker
            
        Raises:
            ConnectionError: If connection to tracker fails
            TimeoutError: If tracker doesn't respond in time
            ValueError: If response parsing fails
        """
        try:
            # Create a socket for the request
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)  # 10 second timeout
            
            # Connect to the tracker
            sock.connect((self.tracker_host, self.tracker_port))
            
            # Send the request
            request_data = json.dumps(data).encode('utf-8')
            sock.sendall(request_data)
            
            # Receive the response
            response_data = b''
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response_data += chunk
                
                # Check if we've received a complete JSON object
                try:
                    decoded_data = response_data.decode('utf-8')
                    json.loads(decoded_data)  # Test if valid JSON
                    break  # If we can parse it, we've received the full message
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue  # Not complete JSON, wait for more data
            
            # Parse the response
            response = json.loads(response_data.decode('utf-8'))
            return response
            
        except socket.timeout:
            self.logger.error("Connection to tracker timed out")
            raise TimeoutError("Connection to tracker timed out")
        except socket.error as e:
            self.logger.error(f"Socket error connecting to tracker: {str(e)}")
            raise ConnectionError(f"Failed to connect to tracker: {str(e)}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse tracker response: {str(e)}")
            raise ValueError(f"Invalid response from tracker: {str(e)}")
        finally:
            sock.close()
    
    def submit_info(self, port: int, username: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """Submit peer information to the tracker
        
        Args:
            port (int): The port number this peer is listening on
            username (str): Username for this peer
            session_id (Optional[str]): Session ID for this peer instance
            
        Returns:
            Dict[str, Any]: Response from the tracker
        """
        data = {
            "command": "submit_info",
            "port": port,
            "username": username
        }
        
        if session_id:
            data["session_id"] = session_id
            
        self.logger.info(f"Submitting peer info to tracker: {username} on port {port}")
        response = self._send_request(data)
        
        if response.get("status") == "success":
            self.logger.info("Successfully registered with tracker")
        else:
            self.logger.error(f"Failed to register with tracker: {response.get('message', 'Unknown error')}")
            
        return response
    
    def get_list(self) -> List[Dict[str, Any]]:
        """Get the list of peers from the tracker
        
        Returns:
            List[Dict[str, Any]]: List of peer information dictionaries
        """
        data = {
            "command": "get_list"
        }
        
        self.logger.info("Requesting peer list from tracker")
        response = self._send_request(data)
        
        if response.get("status") == "success":
            peers = response.get("peers", {})
            peer_list = []
            
            for peer_id, peer_info in peers.items():
                # Convert the dictionary entries to a list of dictionaries
                peer_info['peer_id'] = peer_id
                peer_list.append(peer_info)
                
            self.logger.info(f"Received {len(peer_list)} peers from tracker")
            return peer_list
        else:
            self.logger.error(f"Failed to get peer list: {response.get('message', 'Unknown error')}")
            return []
            
    def parse_peer_list(self, peer_list: List[Dict[str, Any]]) -> List[Tuple[str, int, str, str]]:
        """Parse the peer list into a list of (ip, port, username, session_id) tuples
        
        Args:
            peer_list (List[Dict[str, Any]]): List of peer information dictionaries
            
        Returns:
            List[Tuple[str, int, str, str]]: List of (ip, port, username, session_id) tuples
        """
        result = []
        for peer in peer_list:
            try:
                ip = peer.get('ip')
                port = int(peer.get('port'))
                username = peer.get('username')
                session_id = peer.get('session_id')
                
                if all([ip, port, username, session_id]):
                    result.append((ip, port, username, session_id))
            except (ValueError, TypeError) as e:
                self.logger.error(f"Error parsing peer data: {str(e)}")
                
        return result
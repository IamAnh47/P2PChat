import os
import json
import threading
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

class DataManager:
    """Manages data storage and retrieval for the server"""
    
    def __init__(self, data_dir: str = 'data'):
        """Initialize the data manager
        
        Args:
            data_dir (str): Directory for data files
        """
        self.data_dir = data_dir
        self.peers_path = os.path.join(data_dir, 'peers.json')
        self.backup_messages_path = os.path.join(data_dir, 'backup_messages.json')
        self.lock = threading.Lock()  # For thread safety
        
        # Set up logging
        self.logger = logging.getLogger('DataManager')
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize data files if they don't exist
        if not os.path.exists(self.peers_path):
            self.save_peers({})
            
        if not os.path.exists(self.backup_messages_path):
            self.save_backup({})
            
    def load_peers(self) -> Dict[str, Dict[str, Any]]:
        """Load peer data from peers.json
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of peer information
        """
        try:
            with self.lock:
                with open(self.peers_path, 'r') as f:
                    return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Error loading peers: {str(e)}")
            return {}
            
    def save_peers(self, peers: Dict[str, Dict[str, Any]]) -> bool:
        """Save peer data to peers.json
        
        Args:
            peers (Dict[str, Dict[str, Any]]): Dictionary of peer information
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.lock:
                with open(self.peers_path, 'w') as f:
                    json.dump(peers, f, indent=2)
            self.logger.info(f"Saved {len(peers)} peers to {self.peers_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving peers: {str(e)}")
            return False
            
    def add_peer(self, peer_id: str, peer_info: Dict[str, Any]) -> bool:
        """Add or update a peer in peers.json
        
        Args:
            peer_id (str): Unique ID for the peer
            peer_info (Dict[str, Any]): Peer information
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            peers = self.load_peers()
            
            # Update or add the peer
            with self.lock:
                peers[peer_id] = peer_info
                return self.save_peers(peers)
        except Exception as e:
            self.logger.error(f"Error adding peer {peer_id}: {str(e)}")
            return False
            
    def remove_peer(self, peer_id: str) -> bool:
        """Remove a peer from peers.json
        
        Args:
            peer_id (str): Unique ID for the peer
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            peers = self.load_peers()
            
            with self.lock:
                if peer_id in peers:
                    del peers[peer_id]
                    return self.save_peers(peers)
                return True  # Peer not found, but not an error
        except Exception as e:
            self.logger.error(f"Error removing peer {peer_id}: {str(e)}")
            return False
            
    def load_backup_messages(self) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Load backup messages from backup_messages.json"""
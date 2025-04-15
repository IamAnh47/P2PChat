import os
import json
import requests
import logging
from typing import Dict, Any, List, Optional

class SyncManager:
    """Manages synchronization of channel content between local cache and server"""
    
    def __init__(self, server_base_url: str, cache_dir: str = 'cache'):
        """Initialize the sync manager
        
        Args:
            server_base_url (str): Base URL for the server API
            cache_dir (str): Directory for cached files
        """
        self.server_base_url = server_base_url.rstrip('/')
        self.cache_dir = cache_dir
        self.logger = logging.getLogger('SyncManager')
        
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
        os.makedirs(os.path.join(cache_dir, 'channels'), exist_ok=True)
        
    def _get_channel_cache_path(self, channel_id: str) -> str:
        """Get the path to the cache file for a channel
        
        Args:
            channel_id (str): ID of the channel
            
        Returns:
            str: Path to the cache file
        """
        return os.path.join(self.cache_dir, 'channels', f"{channel_id}.json")
        
    def _load_channel_cache(self, channel_id: str) -> List[Dict[str, Any]]:
        """Load cached messages for a channel
        
        Args:
            channel_id (str): ID of the channel
            
        Returns:
            List[Dict[str, Any]]: List of cached messages
        """
        cache_path = self._get_channel_cache_path(channel_id)
        try:
            with open(cache_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []
            
    def _save_channel_cache(self, channel_id: str, messages: List[Dict[str, Any]]):
        """Save messages to the channel cache
        
        Args:
            channel_id (str): ID of the channel
            messages (List[Dict[str, Any]]): List of messages to cache
        """
        cache_path = self._get_channel_cache_path(channel_id)
        with open(cache_path, 'w') as f:
            json.dump(messages, f)
            
    def _get_server_messages(self, user_id: str, channel_id: str) -> List[Dict[str, Any]]:
        """Get messages from the server API
        
        Args:
            user_id (str): ID of the user
            channel_id (str): ID of the channel
            
        Returns:
            List[Dict[str, Any]]: List of messages from the server
        """
        url = f"{self.server_base_url}/users/{user_id}/channels/{channel_id}/messages"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching messages from server: {str(e)}")
            return []
            
    def _post_messages_to_server(self, user_id: str, channel_id: str, 
                               messages: List[Dict[str, Any]]) -> bool:
        """Post messages to the server API
        
        Args:
            user_id (str): ID of the user
            channel_id (str): ID of the channel
            messages (List[Dict[str, Any]]): List of messages to post
            
        Returns:
            bool: True if successful, False otherwise
        """
        url = f"{self.server_base_url}/users/{user_id}/channels/{channel_id}/messages"
        try:
            response = requests.post(url, json={"messages": messages})
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error posting messages to server: {str(e)}")
            return False
            
    def sync_offline(self, user_id: str, channel_id: str) -> bool:
        """Synchronize channel content to the server when going offline
        
        This function uploads all cached messages to the server before going offline.
        
        Args:
            user_id (str): ID of the user
            channel_id (str): ID of the channel
            
        Returns:
            bool: True if synchronization was successful, False otherwise
        """
        self.logger.info(f"Syncing offline for channel {channel_id}")
        
        # Load cached messages
        cached_messages = self._load_channel_cache(channel_id)
        if not cached_messages:
            self.logger.info(f"No cached messages for channel {channel_id}")
            return True
            
        # Post cached messages to server
        success = self._post_messages_to_server(user_id, channel_id, cached_messages)
        
        if success:
            self.logger.info(f"Successfully synced {len(cached_messages)} messages to server")
        
        return success
        
    def sync_online(self, user_id: str, channel_id: str) -> List[Dict[str, Any]]:
        """Synchronize channel content from the server when going online
        
        This function downloads all messages from the server and merges them with
        any cached messages that weren't previously synced.
        
        Args:
            user_id (str): ID of the user
            channel_id (str): ID of the channel
            
        Returns:
            List[Dict[str, Any]]: The merged list of messages after synchronization
        """
        self.logger.info(f"Syncing online for channel {channel_id}")
        
        # Get messages from server
        server_messages = self._get_server_messages(user_id, channel_id)
        
        # Load cached messages
        cached_messages = self._load_channel_cache(channel_id)
        
        # Merge messages
        # Use message_id as unique identifier if available, otherwise use timestamp + sender
        merged_messages = []
        message_ids = set()
        
        # First add all server messages
        for msg in server_messages:
            identifier = msg.get('message_id', f"{msg.get('timestamp')}:{msg.get('sender_id')}")
            message_ids.add(identifier)
            merged_messages.append(msg)
            
        # Then add any cached messages not in server
        for msg in cached_messages:
            identifier = msg.get('message_id', f"{msg.get('timestamp')}:{msg.get('sender_id')}")
            if identifier not in message_ids:
                merged_messages.append(msg)
                message_ids.add(identifier)
                
        # Sort by timestamp
        merged_messages.sort(key=lambda x: x.get('timestamp', ''))
        
        # Update cache
        self._save_channel_cache(channel_id, merged_messages)
        
        self.logger.info(f"Merged {len(server_messages)} server messages with {len(cached_messages)} cached messages")
        return merged_messages
        
    def cache_message(self, channel_id: str, message: Dict[str, Any]):
        """Cache a new message locally
        
        Args:
            channel_id (str): ID of the channel
            message (Dict[str, Any]): Message to cache
        """
        messages = self._load_channel_cache(channel_id)
        messages.append(message)
        self._save_channel_cache(channel_id, messages)
        
    def clear_cache(self, channel_id: Optional[str] = None):
        """Clear the cache for a channel or all channels
        
        Args:
            channel_id (Optional[str]): ID of the channel to clear, or None for all channels
        """
        if channel_id:
            cache_path = self._get_channel_cache_path(channel_id)
            if os.path.exists(cache_path):
                os.remove(cache_path)
                self.logger.info(f"Cleared cache for channel {channel_id}")
        else:
            for filename in os.listdir(os.path.join(self.cache_dir, 'channels')):
                if filename.endswith('.json'):
                    os.remove(os.path.join(self.cache_dir, 'channels', filename))
            self.logger.info("Cleared cache for all channels")
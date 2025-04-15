import os
import logging
import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional

class RotatingFileLogger:
    """A logger with rotating file capability that's limited to 10K entries"""
    
    def __init__(self, log_dir: str = 'logs', 
                 filename: str = 'application.log',
                 max_bytes: int = 5 * 1024 * 1024,  # 5MB
                 max_entries: int = 10000):
        """Initialize the rotating file logger
        
        Args:
            log_dir (str): Directory for log files
            filename (str): Name of the log file
            max_bytes (int): Maximum size of log file before rotation
            max_entries (int): Maximum number of log entries to keep
        """
        self.log_dir = log_dir
        self.filename = filename
        self.max_bytes = max_bytes
        self.max_entries = max_entries
        self.entry_count = 0
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure logger
        self.logger = logging.getLogger('P2PChat')
        self.logger.setLevel(logging.INFO)
        
        # Set up handler
        self._setup_handler()
        
    def _setup_handler(self):
        """Set up the rotating file handler"""
        log_path = os.path.join(self.log_dir, self.filename)
        
        # Remove existing handlers if any
        if self.logger.handlers:
            for handler in self.logger.handlers:
                self.logger.removeHandler(handler)
                
        # Create a rotating file handler
        handler = RotatingFileHandler(
            log_path,
            maxBytes=self.max_bytes,
            backupCount=3  # Keep 3 backup files
        )
        
        # Set the format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        self.logger.addHandler(handler)
        
        # Log the initialization
        self.logger.info(f"Logger initialized with max {self.max_entries} entries")
        
        # Count current entries (approximate)
        try:
            with open(log_path, 'r') as f:
                self.entry_count = sum(1 for _ in f)
        except FileNotFoundError:
            self.entry_count = 0
        
    def log_event(self, event_type: str, details: Dict[str, Any], session_id: Optional[str] = None):
        """Log an event with specified type and details
        
        Args:
            event_type (str): Type of event (e.g., 'connection', 'message', 'error')
            details (Dict[str, Any]): Event details as a dictionary
            session_id (Optional[str]): Session ID for tracking purposes
        """
        # Check if we need to rotate due to entry count
        if self.entry_count >= self.max_entries:
            self._rotate_by_entries()
            
        # Add timestamp and session ID to details
        log_details = details.copy()
        log_details['timestamp'] = datetime.datetime.now().isoformat()
        
        if session_id:
            log_details['session_id'] = session_id
            
        # Format the message
        message = f"EVENT={event_type}"
        for key, value in log_details.items():
            message += f" | {key}={value}"
            
        # Log the message
        self.logger.info(message)
        self.entry_count += 1
        
    def _rotate_by_entries(self):
        """Rotate the log file when entry count exceeds the maximum"""
        log_path = os.path.join(self.log_dir, self.filename)
        backup_path = f"{log_path}.1"
        
        # Close existing handlers
        for handler in self.logger.handlers:
            handler.close()
            
        # Rename current log file to backup
        if os.path.exists(log_path):
            if os.path.exists(backup_path):
                os.remove(backup_path)
            os.rename(log_path, backup_path)
            
        # Create new log file
        open(log_path, 'w').close()
        
        # Reset entry count
        self.entry_count = 0
        
        # Re-setup handler
        self._setup_handler()
        
        # Log rotation event
        self.logger.info(f"Log file rotated due to entry count exceeding {self.max_entries}")

# Singleton instance for application-wide use
_logger_instance = None

def get_logger(log_dir: str = 'logs', filename: str = 'application.log'):
    """Get or create the singleton logger instance
    
    Args:
        log_dir (str): Directory for log files
        filename (str): Name of the log file
        
    Returns:
        RotatingFileLogger: The logger instance
    """
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = RotatingFileLogger(log_dir, filename)
    return _logger_instance
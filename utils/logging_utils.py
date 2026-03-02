"""
Logging utilities
File: utils/logging_utils.py
"""

import sys
import datetime


class Tee:
    """Redirect stdout to both console and log file"""
    
    def __init__(self, log_file_path):
        """Initialize Tee with log file path"""
        self.terminal = sys.stdout
        self.log_file = open(log_file_path, 'a', encoding='utf-8')
        self.closed = False
        
        # Write header to log file
        header = f"\n{'='*80}\nConversation Log Started: {datetime.datetime.now().isoformat()}\n{'='*80}\n"
        self.log_file.write(header)
        self.log_file.flush()
    
    def write(self, message):
        """Write message to both terminal and log file"""
        self.terminal.write(message)
        if not self.closed:
            self.log_file.write(message)
    
    def flush(self):
        """Flush both terminal and log file"""
        self.terminal.flush()
        if not self.closed:
            self.log_file.flush()
    
    def close(self):
        """Close log file and restore original stdout"""
        if hasattr(self, 'log_file') and self.log_file and not self.closed:
            footer = f"\n{'='*80}\nConversation Log Ended: {datetime.datetime.now().isoformat()}\n{'='*80}\n"
            self.log_file.write(footer)
            self.log_file.flush()
            self.log_file.close()
            self.closed = True
    
    def __del__(self):
        """Ensure file is closed on deletion"""
        self.close()
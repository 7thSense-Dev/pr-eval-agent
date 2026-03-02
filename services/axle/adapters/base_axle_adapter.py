"""
Base Axle Adapter for Conversation Pipeline
File: services/axle/adapters/base_axle_adapter.py
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pathlib import Path
import structlog

logger = structlog.get_logger()


class BaseAxleAdapter(ABC):
    """
    Base axle adapter interface - provides conversation capabilities
    Similar to BaseProviderAdapter in LLM routing service
    """
    
    def __init__(self, provider_id: str):
        self.provider_id = provider_id
        self._initialized = False
        self.uploaded_files = {}
        self.conversation_history = []
        self.turn_number = 0
        self.turn_details = []
        
        # Token tracking
        self.cumulative_tokens = {
            'input': 0,
            'output': 0,
            'total': 0
        }
        self.cumulative_cost = 0.0
    
    # ================================================================
    # LIFECYCLE MANAGEMENT
    # ================================================================
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize adapter resources"""
        pass
    
    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up adapter resources"""
        pass
    
    # ================================================================
    # FILE UPLOAD METHODS (ASYNC)
    # ================================================================
    
    @abstractmethod
    async def upload_file(self, file_path: str, **kwargs) -> Any:
        """Upload a single file to the provider"""
        pass
    
    @abstractmethod
    async def upload_multiple_files(self, file_paths: list, **kwargs) -> List[Dict[str, Any]]:
        """Upload multiple files to the provider"""
        pass
    
    def get_uploaded_files_info(self) -> Dict[str, Any]:
        """Get information about uploaded files"""
        return self.uploaded_files
    
    def reset_uploaded_files(self) -> None:
        """Reset uploaded files tracking"""
        self.uploaded_files = {}
    
    # ================================================================
    # CONVERSATION METHODS (ASYNC)
    # ================================================================
    
    @abstractmethod
    async def create_conversation(self, conversation_id: int, **kwargs) -> None:
        """Initialize a conversation session"""
        pass
    
    @abstractmethod
    async def create_message_with_files(
        self,
        file_ids: list,
        user_message: str,
        max_tokens: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a message with files"""
        pass
    
    @abstractmethod
    async def start_conversation(self, file_ids: list, prompt_path: str) -> Dict[str, Any]:
        """Start a conversation with files and prompt"""
        pass
    
    def get_conversation_summary(self) -> Dict[str, Any]:
        """Get conversation summary (sync - just returns data)"""
        print(f"\n{'=' * 80}")
        print("COMPLETE CONVERSATION SUMMARY")
        print(f"{'=' * 80}")
        print(f"\nProvider: {self.provider_id.upper()}")
        print(f"Total turns: {self.turn_number}")
        
        print(f"\n📊 TOKEN USAGE:")
        print(f"  Total input tokens:  {self.cumulative_tokens['input']:,}")
        print(f"  Total output tokens: {self.cumulative_tokens['output']:,}")
        print(f"  Total tokens:        {self.cumulative_tokens['total']:,}")
        
        print(f"\n💰 TOTAL COST:")
        print(f"  ${self.cumulative_cost:.4f}")
        
        print(f"\n{'=' * 80}\n")
        
        return {
            'provider': self.provider_id,
            'total_turns': self.turn_number,
            'cumulative_tokens': self.cumulative_tokens.copy(),
            'cumulative_cost': self.cumulative_cost,
            'turn_details': self.turn_details
        }
    
    @abstractmethod
    def save_conversation_log(self, filepath: str) -> None:
        """Save conversation log to file (sync - I/O operation)"""
        pass
    
    @abstractmethod
    def close_logging(self) -> None:
        """Close logging (sync)"""
        pass
    
    # ================================================================
    # ARTIFACT DOWNLOAD METHODS (ASYNC)
    # ================================================================
    
    @abstractmethod
    async def download_artifact(self, file_id: str, save_as: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Download a single artifact"""
        pass
    
    @abstractmethod
    async def download_multiple_artifacts(self, file_ids: list, downloads_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
        """Download multiple artifacts"""
        pass
    
    # ================================================================
    # UTILITY METHODS (SYNC - logging only)
    # ================================================================
    
    def _log_upload_start(self, file_path: str, size_kb: float):
        """Common logging for upload start"""
        print(f"\n📤 Uploading: {Path(file_path).name}")
        print(f"   Provider: {self.provider_id}")
        print(f"   Size: {size_kb:.2f} KB")
    
    def _log_upload_success(self, file_id: str):
        """Common logging for upload success"""
        print(f"   ✓ Uploaded successfully!")
        print(f"   File ID: {file_id}")
    
    def _log_upload_error(self, error: Exception):
        """Common logging for upload error"""
        print(f"   ✗ Upload failed: {error}")
    
    def _log_download_start(self, file_id: str):
        """Common logging for download start"""
        print(f"\n📥 Downloading artifact: {file_id}")
        print(f"   Provider: {self.provider_id}")
    
    def _log_download_success(self, file_path: Path, size_kb: float):
        """Common logging for download success"""
        print(f"   ✓ Downloaded: {file_path}")
        print(f"   Size: {size_kb:.2f} KB")
    
    def _log_download_error(self, error: Exception):
        """Common logging for download error"""
        print(f"   ✗ Download failed: {error}")
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(provider_id={self.provider_id})"
    
    def __repr__(self) -> str:
        return self.__str__()
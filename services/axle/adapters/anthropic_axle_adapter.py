"""
Anthropic Axle Adapter - Handles all Claude operations
File: services/axle/adapters/anthropic_axle_adapter.py
"""

import os
import sys
import json
import time
import hashlib
import datetime
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional
from anthropic import Anthropic, InternalServerError, AsyncAnthropic
from dotenv import load_dotenv
import structlog

from services.axle.adapters.base_axle_adapter import BaseAxleAdapter
from utils.logging_utils import Tee

load_dotenv()

logger = structlog.get_logger()


class AnthropicAxleAdapter(BaseAxleAdapter):
    """
    Unified Anthropic Axle Adapter - handles file uploads, conversations, and downloads
    """
    
    def __init__(self):
        super().__init__("claude")
        
        # Get API key
        self.api_key = os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY not found in environment variables.\n"
                "Please create a .env file with your API key."
            )
        
        # Initialize sync and async clients
        self.client = Anthropic(api_key=self.api_key)
        self.async_client = AsyncAnthropic(api_key=self.api_key)
        
        # Conversation-specific attributes
        self.conversation_id = None
        self.model = None
        self.max_tokens = None
        self.tee = None
        self.original_stdout = None
        
        # Pricing
        self.input_token_cost = 0.0
        self.output_token_cost = 0.0
        self.context_window = 0
        
        logger.info("Anthropic adapter initialized", provider_id=self.provider_id)
    
    # ================================================================
    # LIFECYCLE MANAGEMENT
    # ================================================================
    
    async def initialize(self) -> None:
        """Initialize Claude client"""
        try:
            self._initialized = True
            logger.info("Anthropic adapter initialized", provider_id=self.provider_id)
        except Exception as e:
            logger.error("Failed to initialize Anthropic adapter", error=str(e))
            raise
    
    async def cleanup(self) -> None:
        """Cleanup Claude resources"""
        if hasattr(self, 'tee') and self.tee:
            self.close_logging()
        if hasattr(self, 'async_client'):
            await self.async_client.close()
        self._initialized = False
        logger.info("Anthropic adapter cleaned up", provider_id=self.provider_id)
    
    # ================================================================
    # FILE UPLOAD METHODS (ASYNC)
    # ================================================================
    
    async def upload_file(self, file_path: str, **kwargs) -> Any:
        """Upload a single file to Claude using Files API (Beta)"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        size_kb = file_path.stat().st_size / 1024
        self._log_upload_start(str(file_path), size_kb)
        
        try:
            # Determine MIME type
            mime_type = self._get_mime_type(file_path)
            
            # Upload using beta.files API (run in thread pool since SDK is sync)
            loop = asyncio.get_event_loop()
            
            def _sync_upload():
                with open(file_path, 'rb') as f:
                    return self.client.beta.files.upload(
                        file=(file_path.name, f, mime_type)
                    )
            
            file_response = await loop.run_in_executor(None, _sync_upload)
            
            file_id = file_response.id
            self._log_upload_success(file_id)
            
            # Store file info
            self.uploaded_files[file_path.name] = {
                'file_id': file_id,
                'file_path': str(file_path),
                'file_name': file_path.name,
                'size_kb': round(size_kb, 2),
                'mime_type': mime_type
            }
            
            return file_response
            
        except Exception as e:
            self._log_upload_error(e)
            raise
    
    async def upload_multiple_files(self, file_paths: list, **kwargs) -> List[Dict[str, Any]]:
        """Upload multiple files to Claude"""
        results = []
        for file_path in file_paths:
            try:
                result = await self.upload_file(file_path)
                results.append({
                    'success': True,
                    'file_path': file_path,
                    'file_id': result.id
                })
            except Exception as e:
                results.append({
                    'success': False,
                    'file_path': file_path,
                    'error': str(e)
                })
        
        return results
    
    def _get_mime_type(self, file_path: Path) -> str:
        """Determine MIME type from file extension"""
        mime_types = {
            '.zip': 'application/zip',
            '.md': 'text/markdown',
            '.pdf': 'application/pdf',
            '.txt': 'text/plain',
            '.py': 'text/plain',
            '.js': 'text/plain',
            '.java': 'text/plain',
            '.cpp': 'text/plain'
        }
        return mime_types.get(file_path.suffix, 'application/octet-stream')

    def _compute_content_hash(self, content: bytes) -> str:
        """Compute MD5 hash of content for deduplication"""
        return hashlib.md5(content).hexdigest()

    # ================================================================
    # CONVERSATION METHODS (ASYNC)
    # ================================================================
    
    async def create_conversation(self, conversation_id: int, log_dir: Path = None, **kwargs) -> None:
        """
        Initialize a conversation session.

        Args:
            conversation_id: Unique conversation identifier
            log_dir: Directory for log files (defaults to metrics folder if provided)
        """
        self.conversation_id = conversation_id
        self.model = os.getenv('CLAUDE_MODEL')
        self.max_tokens = int(os.getenv('MAX_TOKENS'))

        # Reset conversation state
        self.conversation_history = []
        self.turn_number = 0
        self.turn_details = []
        self.cumulative_tokens = {'input': 0, 'output': 0, 'total': 0}
        self.cumulative_cost = 0.0

        # Set up logging - use provided log_dir (metrics folder) or fallback
        if log_dir:
            log_dir = Path(log_dir)
        else:
            # Fallback to default metrics location
            log_dir = Path("tmp/poc/default/metrics")
        # Only create if doesn't exist - use existing folder structure
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"claude_conversation_{timestamp}_{conversation_id}.log"
        self.tee = Tee(log_file)
        self.original_stdout = sys.stdout
        sys.stdout = self.tee

        # Set pricing
        self._set_pricing()
    
    def _set_pricing(self):
        """Set pricing based on model"""
        if "haiku-4-5" in self.model:
            self.input_token_cost = 1/1000000
            self.output_token_cost = 5/1000000
            self.context_window = 200000
        elif "sonnet-4-5" in self.model:
            self.input_token_cost = 3/1000000
            self.output_token_cost = 15/1000000
            self.context_window = 200000
        else:
            self.input_token_cost = 3/1000000
            self.output_token_cost = 15/1000000
            self.context_window = 200000
    
    async def create_message_with_files(
        self,
        file_ids: list,
        user_message: str,
        max_tokens: Optional[int] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        max_continuation_turns: int = 10,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a message with streaming and automatic pause_turn handling"""
        
        self.turn_number += 1
        turn_start_time = time.time()

        # Build content
        content = [{"type": "text", "text": user_message}]
        for file_id in file_ids:
            content.append({"type": "container_upload", "file_id": file_id})

        try:
            # Add to conversation history
            self.conversation_history.append({
                "role": "user",
                "content": content
            })
            
            message_params = {
                "model": self.model,
                "max_tokens": max_tokens or self.max_tokens,
                "betas": ["code-execution-2025-08-25", "files-api-2025-04-14"],
                "tools": [{"type": "code_execution_20250825", "name": "code_execution"}],
                "messages": self.conversation_history
            }
            
            continuation_count = 0
            final_message = None
            stop_reason = None
            new_container_id = None
            turn_input_tokens = 0
            turn_output_tokens = 0

            while continuation_count < max_continuation_turns:
                for attempt in range(max_retries):
                    try:
                        # Run streaming in thread pool (SDK is sync)
                        loop = asyncio.get_event_loop()
                        
                        def _sync_stream():
                            with self.client.beta.messages.stream(**message_params) as stream:
                                current_block_type = None

                                for event in stream:
                                    # Handle different event types for comprehensive logging
                                    if event.type == "content_block_start":
                                        block = event.content_block
                                        current_block_type = block.type

                                        if block.type == "tool_use":
                                            tool_name = getattr(block, 'name', 'unknown')
                                            print(f"\n\n🔧 [Tool Call: {tool_name}]", flush=True)
                                        elif block.type == "code_execution_tool_result":
                                            print(f"\n\n📤 [Code Execution Result]", flush=True)
                                        elif block.type == "text":
                                            pass  # Text will be printed via deltas

                                    elif event.type == "content_block_delta":
                                        delta = event.delta

                                        if delta.type == "text_delta":
                                            print(delta.text, end="", flush=True)
                                        elif delta.type == "input_json_delta":
                                            # Tool input JSON being streamed
                                            print(delta.partial_json, end="", flush=True)

                                    elif event.type == "content_block_stop":
                                        if current_block_type in ["tool_use", "code_execution_tool_result"]:
                                            print("\n", flush=True)
                                        current_block_type = None

                                print("\n")
                                return stream.get_final_message()
                        
                        final_message = await loop.run_in_executor(None, _sync_stream)
                        stop_reason = final_message.stop_reason
                        
                        # Track tokens
                        turn_input_tokens = final_message.usage.input_tokens
                        turn_output_tokens = final_message.usage.output_tokens
                        
                        if hasattr(final_message, 'container') and hasattr(final_message.container, 'id'):
                            new_container_id = final_message.container.id

                        if stop_reason == "pause_turn":
                            self.conversation_history.append({
                                "role": "assistant",
                                "content": final_message.content
                            })
                            message_params["messages"] = self.conversation_history
                            continuation_count += 1
                            break
                        elif stop_reason in ["end_turn", "max_tokens"]:
                            self.conversation_history.append({
                                "role": "assistant",
                                "content": final_message.content
                            })
                            continuation_count = max_continuation_turns
                            break
                        else:
                            self.conversation_history.append({
                                "role": "assistant",
                                "content": final_message.content
                            })
                            continuation_count = max_continuation_turns
                            break

                    except InternalServerError as e:
                        if attempt == max_retries - 1:
                            raise
                        delay = retry_delay * (2 ** attempt)
                        await asyncio.sleep(delay)
                        continue
                    break

                if stop_reason == "pause_turn" and continuation_count < max_continuation_turns:
                    continue
                else:
                    break

            # Calculate metrics
            turn_duration = time.time() - turn_start_time
            
            response_text = ""
            for block in final_message.content:
                if hasattr(block, 'text'):
                    response_text += block.text

            input_cost = (turn_input_tokens / 1_000_000) * self.input_token_cost * 1_000_000
            output_cost = (turn_output_tokens / 1_000_000) * self.output_token_cost * 1_000_000
            total_cost = input_cost + output_cost
            
            self.cumulative_tokens['input'] += turn_input_tokens
            self.cumulative_tokens['output'] += turn_output_tokens
            self.cumulative_tokens['total'] += (turn_input_tokens + turn_output_tokens)
            self.cumulative_cost += total_cost

            return {
                'success': True,
                'message': final_message,
                'response_text': response_text,
                'token_usage': {
                    'input_tokens': turn_input_tokens,
                    'output_tokens': turn_output_tokens,
                    'total_tokens': turn_input_tokens + turn_output_tokens
                },
                'estimated_cost': {
                    'input': input_cost,
                    'output': output_cost,
                    'total': total_cost
                },
                'cumulative_tokens': self.cumulative_tokens.copy(),
                'cumulative_cost': self.cumulative_cost,
                'container_id': new_container_id
            }

        except Exception as e:
            import traceback
            return {
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }
    
    async def start_conversation(self, file_ids: list, prompt_path: str) -> Dict[str, Any]:
        """Start a conversation with files and prompt"""
        prompt_file = Path(prompt_path)
        
        if not prompt_file.exists():
            raise FileNotFoundError(f"Prompt file not found: {prompt_file}")
        
        with open(prompt_file, 'r', encoding='utf-8') as f:
            prompt = f.read()

        result = await self.create_message_with_files(file_ids, prompt, 32*1000)
        
        if result.get('message'):
            result_message = result["message"].model_dump(mode="json")
            result["message"] = result_message
        
        return result
    
    def save_conversation_log(self, filepath: str) -> None:
        """Save detailed conversation log to file"""
        log_data = {
            'conversation_id': self.conversation_id,
            'model': self.model,
            'timestamp': datetime.datetime.now().isoformat(),
            'total_turns': self.turn_number,
            'cumulative_tokens': self.cumulative_tokens,
            'cumulative_cost': self.cumulative_cost,
            'turn_details': self.turn_details
        }
        
        with open(filepath, 'w') as f:
            json.dump(log_data, f, indent=2)
    
    def close_logging(self) -> None:
        """Close logging and restore stdout"""
        if hasattr(self, 'tee') and self.tee:
            sys.stdout = self.original_stdout
            self.tee.close()
    
    # ================================================================
    # ARTIFACT DOWNLOAD METHODS (ASYNC)
    # ================================================================
    
    async def download_artifact(self, file_id: str, save_as: Optional[str] = None, downloads_dir: Optional[Path] = None) -> Dict[str, Any]:
        """Download an artifact file (eval report) using its file ID to artifacts folder"""
        if downloads_dir is None:
            downloads_dir = Path('artifacts')
        # Only create if doesn't exist - use existing folder structure
        downloads_dir.mkdir(parents=True, exist_ok=True)
        
        self._log_download_start(file_id)
        
        try:
            loop = asyncio.get_event_loop()
            
            # Get metadata (sync call in thread pool)
            def _get_metadata():
                return self.client.beta.files.retrieve_metadata(file_id)
            
            file_metadata_ = await loop.run_in_executor(None, _get_metadata)
            file_metadata = file_metadata_.model_dump(mode="json")
            save_as = file_metadata_.filename
            
            # Download content (sync call in thread pool)
            def _download():
                return self.client.beta.files.download(file_id)
            
            response = await loop.run_in_executor(None, _download)
            
            # Extract bytes
            if hasattr(response, 'content'):
                file_content = response.content
            elif hasattr(response, 'read'):
                file_content = response.read()
            else:
                file_content = bytes(response)
            
            # Save file with clean filename (no file_id prefix) for artifacts
            filename = save_as if save_as else f"{file_id}.txt"
            file_path = downloads_dir / filename

            with open(file_path, "w") as f:
                f.write(file_content.decode('utf-8'))

            file_size_kb = file_path.stat().st_size / 1024
            self._log_download_success(file_path, file_size_kb)

            file_metadata["local_file_path"] = str(file_path)
            file_metadata["file_id"] = file_id
            return file_metadata
            
        except Exception as e:
            self._log_download_error(e)
            raise
    
    async def download_multiple_artifacts(self, file_ids: list, downloads_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
        """Download multiple artifacts (eval reports) with deduplication by content hash.

        Files are saved to the artifacts folder with format: {filename}
        Example: file1_eval_report.md, summary_eval_report.md
        """
        if downloads_dir is None:
            downloads_dir = Path('artifacts')
        # Only create if doesn't exist - use existing folder structure
        downloads_dir.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_event_loop()
        all_files = []

        for file_id in file_ids:
            try:
                def _get_metadata(fid=file_id):
                    return self.client.beta.files.retrieve_metadata(fid)

                metadata = await loop.run_in_executor(None, _get_metadata)
                filename = metadata.filename

                def _download(fid=file_id):
                    return self.client.beta.files.download(fid)

                response = await loop.run_in_executor(None, _download)

                if hasattr(response, 'content'):
                    file_content = response.content
                elif hasattr(response, 'read'):
                    file_content = response.read()
                else:
                    file_content = bytes(response)

                content_hash = self._compute_content_hash(file_content)

                all_files.append({
                    'file_id': file_id,
                    'filename': filename,
                    'content': file_content,
                    'metadata': metadata,
                    'content_hash': content_hash
                })
            except Exception:
                continue

        # Deduplicate by content hash (keep last occurrence for each unique content)
        unique_by_hash = {}
        for file_info in all_files:
            unique_by_hash[file_info['content_hash']] = file_info

        # Save only unique files to artifacts folder
        downloaded = {}

        for content_hash, file_info in unique_by_hash.items():
            try:
                file_id = file_info['file_id']
                filename = file_info['filename']
                file_content = file_info['content']
                metadata = file_info['metadata']

                # Save with clean filename (no file_id prefix) for artifacts
                # e.g., file1_eval_report.md, summary_eval_report.md
                file_path = downloads_dir / filename

                with open(file_path, "w") as f:
                    f.write(file_content.decode('utf-8'))

                file_metadata_dict = metadata.model_dump(mode="json")
                file_metadata_dict["local_file_path"] = str(file_path)
                file_metadata_dict["content_hash"] = content_hash
                file_metadata_dict["file_id"] = file_id
                downloaded[filename] = file_metadata_dict

                self._log_download_success(file_path, file_path.stat().st_size / 1024)
            except Exception as e:
                logger.warning(f"Failed to download artifact {file_info.get('file_id')}: {e}")
                continue

        return downloaded
    
    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, 'tee') and self.tee:
            self.close_logging()
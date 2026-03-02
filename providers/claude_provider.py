"""
Claude Provider - Handles all Claude operations
File: providers/claude_provider.py
"""

import os
import sys
import json
import time
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from anthropic import Anthropic, InternalServerError
from dotenv import load_dotenv

from .base_provider import BaseConversationProvider
from utils.logging_utils import Tee

load_dotenv()


class ClaudeProvider(BaseConversationProvider):
    """
    Unified Claude Provider - handles file uploads, conversations, and downloads
    Similar to AnthropicAdapter in your LLM routing service
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
        
        # Initialize client
        self.client = Anthropic(api_key=self.api_key)
        
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
    
    # ================================================================
    # LIFECYCLE MANAGEMENT
    # ================================================================
    
    async def initialize(self) -> None:
        """Initialize Claude client"""
        try:
            self._initialized = True
            logger.info("Claude provider initialized", provider_id=self.provider_id)
        except Exception as e:
            logger.error("Failed to initialize Claude provider", error=str(e))
            raise
    
    async def cleanup(self) -> None:
        """Cleanup Claude resources"""
        if hasattr(self, 'tee') and self.tee:
            self.close_logging()
        self._initialized = False
        logger.info("Claude provider cleaned up", provider_id=self.provider_id)
    
    # ================================================================
    # FILE UPLOAD METHODS
    # ================================================================
    
    def upload_file(self, file_path: str, **kwargs) -> Any:
        """Upload a single file to Claude using Files API (Beta)"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        size_kb = file_path.stat().st_size / 1024
        self._log_upload_start(str(file_path), size_kb)
        
        try:
            # Determine MIME type
            mime_type = self._get_mime_type(file_path)
            
            # Upload using beta.files API
            with open(file_path, 'rb') as f:
                file_response = self.client.beta.files.upload(
                    file=(file_path.name, f, mime_type)
                )
            
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
    
    def upload_multiple_files(self, file_paths: list, **kwargs) -> List[Dict[str, Any]]:
        """Upload multiple files to Claude"""
        print(f"\n{'=' * 60}")
        print(f"UPLOADING {len(file_paths)} FILE(S) TO CLAUDE")
        print(f"{'=' * 60}")
        
        results = []
        for file_path in file_paths:
            try:
                result = self.upload_file(file_path)
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
    
    # ================================================================
    # CONVERSATION METHODS
    # ================================================================
    
    def create_conversation(self, conversation_id: int, **kwargs) -> None:
        """Initialize a conversation session"""
        self.conversation_id = conversation_id
        self.model = os.getenv('CLAUDE_MODEL')
        self.max_tokens = int(os.getenv('MAX_TOKENS'))
        
        # Reset conversation state
        self.conversation_history = []
        self.turn_number = 0
        self.turn_details = []
        self.cumulative_tokens = {'input': 0, 'output': 0, 'total': 0}
        self.cumulative_cost = 0.0
        
        # Set up logging
        log_dir = Path("create_conversation_log")
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"claude_conversation_{timestamp}_{conversation_id}.log"
        self.tee = Tee(log_file)
        self.original_stdout = sys.stdout
        sys.stdout = self.tee
        
        # Set pricing
        self._set_pricing()
        
        print(f"\n{'='*80}")
        print(f"Claude Conversation Initialized")
        print(f"{'='*80}")
        print(f"Conversation ID: {self.conversation_id}")
        print(f"Model: {self.model}")
        print(f"Max Tokens: {self.max_tokens}")
        print(f"Log file: {log_file}")
        print(f"{'='*80}\n")
    
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
    
    def create_message_with_files(
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
        
        # ============================================================
        # TURN HEADER
        # ============================================================
        print(f"\n{'=' * 80}")
        print(f"TURN {self.turn_number} - MESSAGE TO CLAUDE")
        print(f"{'=' * 80}")
        print(f"Conversation ID: {self.conversation_id}")
        print(f"Model: {self.model}")
        print(f"Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\n{'─' * 80}")
        print("CONVERSATION STATE:")
        print(f"  History messages: {len(self.conversation_history)}")
        print(f"  Cumulative input tokens: {self.cumulative_tokens['input']:,}")
        print(f"  Cumulative output tokens: {self.cumulative_tokens['output']:,}")
        print(f"  Cumulative total tokens: {self.cumulative_tokens['total']:,}")
        print(f"  Context window used: {self.cumulative_tokens['total']:,} / {self.context_window:,} ({(self.cumulative_tokens['total']/self.context_window*100):.1f}%)")
        
        # ============================================================
        # FILE REFERENCES
        # ============================================================
        print(f"\n{'─' * 80}")
        print("FILE REFERENCES:")
        if file_ids:
            print(f"  File IDs count: {len(file_ids)}")
            for idx, file_id in enumerate(file_ids, 1):
                print(f"    {idx}. {file_id}")
        else:
            print(f"  No files referenced in this turn")
        
        # ============================================================
        # USER MESSAGE
        # ============================================================
        print(f"\n{'─' * 80}")
        print("USER MESSAGE:")
        print(f"  Message length: {len(user_message)} characters")
        print(f"  Message preview (first 500 chars):")
        print(f"  {'-' * 76}")
        preview = user_message[:500].replace('\n', '\n  ')
        print(f"  {preview}")
        if len(user_message) > 500:
            print(f"  ... (truncated, {len(user_message) - 500} more characters)")
        print(f"  {'-' * 76}")

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
            
            # ============================================================
            # MESSAGE PARAMETERS
            # ============================================================
            print(f"\n{'─' * 80}")
            print("MESSAGE PARAMETERS:")
            print(f"  Max tokens: {max_tokens or self.max_tokens:,}")
            print(f"  Max retries: {max_retries}")
            print(f"  Max continuation turns: {max_continuation_turns}")
            print(f"  Messages in history: {len(self.conversation_history)}")
            
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

            # ============================================================
            # STREAMING RESPONSE
            # ============================================================
            print(f"\n{'=' * 80}")
            print("STREAMING RESPONSE FROM CLAUDE")
            print(f"{'=' * 80}\n")

            while continuation_count < max_continuation_turns:
                for attempt in range(max_retries):
                    try:
                        if continuation_count > 0:
                            print(f"\n{'─' * 80}")
                            print(f"CONTINUATION {continuation_count + 1}/{max_continuation_turns}")
                            print(f"{'─' * 80}\n")
                        
                        with self.client.beta.messages.stream(**message_params) as stream:
                            for text in stream.text_stream:
                                print(text, end="", flush=True)

                            print("\n")
                            final_message = stream.get_final_message()
                            stop_reason = final_message.stop_reason
                            
                            # Track tokens for this continuation
                            turn_input_tokens = final_message.usage.input_tokens
                            turn_output_tokens = final_message.usage.output_tokens
                            
                            print(f"\n{'─' * 80}")
                            print(f"CONTINUATION {continuation_count + 1} COMPLETE")
                            print(f"  Stop reason: {stop_reason}")
                            print(f"  Input tokens: {turn_input_tokens:,}")
                            print(f"  Output tokens: {turn_output_tokens:,}")
                            print(f"  Total tokens: {turn_input_tokens + turn_output_tokens:,}")

                            if hasattr(final_message, 'container') and hasattr(final_message.container, 'id'):
                                new_container_id = final_message.container.id
                                print(f"  Container ID: {new_container_id}")

                            if stop_reason == "pause_turn":
                                print(f"\n⏸️  Claude paused. Continuing conversation...")
                                
                                # Add assistant's response to history
                                self.conversation_history.append({
                                    "role": "assistant",
                                    "content": final_message.content
                                })
                                
                                # Update message_params with updated history
                                message_params["messages"] = self.conversation_history
                                
                                print(f"  Updated history: {len(self.conversation_history)} messages")
                                
                                has_tool_use = any(hasattr(block, 'type') and 'tool_use' in block.type for block in final_message.content)
                                if has_tool_use:
                                    print(f"  Tool execution detected")
                                
                                continuation_count += 1
                                break
                            elif stop_reason == "end_turn":
                                print(f"\n✓ Conversation turn completed successfully")
                                
                                # Add final assistant response to history
                                self.conversation_history.append({
                                    "role": "assistant",
                                    "content": final_message.content
                                })
                                
                                print(f"  Updated history: {len(self.conversation_history)} messages")
                                
                                continuation_count = max_continuation_turns
                                break
                            elif stop_reason == "max_tokens":
                                print(f"\n⚠️  Warning: Response hit max_tokens limit")
                                
                                # Add assistant response
                                self.conversation_history.append({
                                    "role": "assistant",
                                    "content": final_message.content
                                })
                                
                                continuation_count = max_continuation_turns
                                break
                            else:
                                print(f"\nℹ️  Stopped with reason: {stop_reason}")
                                
                                # Add assistant response
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
                        print(f"\n⚠️  API returned 500 error. Retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(delay)
                        continue
                    break

                if stop_reason == "pause_turn" and continuation_count < max_continuation_turns:
                    continue
                else:
                    break

            if continuation_count >= max_continuation_turns and stop_reason == "pause_turn":
                print(f"\n⚠️  Warning: Reached max continuation turns ({max_continuation_turns})")

            # ============================================================
            # TURN SUMMARY
            # ============================================================
            turn_duration = time.time() - turn_start_time
            
            response_text = ""
            for block in final_message.content:
                if hasattr(block, 'text'):
                    response_text += block.text

            # Calculate costs
            input_cost = (turn_input_tokens / 1_000_000) * self.input_token_cost * 1_000_000
            output_cost = (turn_output_tokens / 1_000_000) * self.output_token_cost * 1_000_000
            total_cost = input_cost + output_cost
            
            # Update cumulative totals
            self.cumulative_tokens['input'] += turn_input_tokens
            self.cumulative_tokens['output'] += turn_output_tokens
            self.cumulative_tokens['total'] += (turn_input_tokens + turn_output_tokens)
            self.cumulative_cost += total_cost

            print(f"\n{'=' * 80}")
            print(f"TURN {self.turn_number} SUMMARY")
            print(f"{'=' * 80}")
            print(f"\n📊 TOKEN USAGE (This Turn):")
            print(f"  Input tokens:  {turn_input_tokens:,}")
            print(f"  Output tokens: {turn_output_tokens:,}")
            print(f"  Total tokens:  {turn_input_tokens + turn_output_tokens:,}")

            print(f"\n💰 COST (This Turn):")
            print(f"  Input:  ${input_cost:.4f}")
            print(f"  Output: ${output_cost:.4f}")
            print(f"  Total:  ${total_cost:.4f}")
            
            print(f"\n📈 CUMULATIVE TOTALS:")
            print(f"  Input tokens:  {self.cumulative_tokens['input']:,}")
            print(f"  Output tokens: {self.cumulative_tokens['output']:,}")
            print(f"  Total tokens:  {self.cumulative_tokens['total']:,}")
            print(f"  Total cost:    ${self.cumulative_cost:.4f}")
            
            print(f"\n📊 CONTEXT WINDOW:")
            context_used_pct = (self.cumulative_tokens['total'] / self.context_window) * 100
            context_remaining = self.context_window - self.cumulative_tokens['total']
            print(f"  Window size:   {self.context_window:,} tokens")
            print(f"  Used:          {self.cumulative_tokens['total']:,} tokens ({context_used_pct:.1f}%)")
            print(f"  Remaining:     {context_remaining:,} tokens ({100-context_used_pct:.1f}%)")
            
            # Visual progress bar
            bar_width = 50
            filled = int(bar_width * context_used_pct / 100)
            bar = '█' * filled + '░' * (bar_width - filled)
            print(f"  Progress:      [{bar}]")
            
            if context_used_pct > 90:
                print(f"  ⚠️  WARNING: Context window >90% full!")
            elif context_used_pct > 75:
                print(f"  ⚠️  CAUTION: Context window >75% full")
            
            print(f"\n⏱️  TIMING:")
            print(f"  Turn duration: {turn_duration:.2f}s")
            print(f"  Continuations: {continuation_count}")
            
            print(f"\n📝 CONVERSATION STATE:")
            print(f"  Messages in history: {len(self.conversation_history)}")
            print(f"  Turns completed: {self.turn_number}")
            
            print(f"\n{'=' * 80}\n")
            
            # Store turn details for later analysis
            turn_detail = {
                'turn_number': self.turn_number,
                'timestamp': datetime.datetime.now().isoformat(),
                'file_ids_count': len(file_ids),
                'message_length': len(user_message),
                'input_tokens': turn_input_tokens,
                'output_tokens': turn_output_tokens,
                'total_tokens': turn_input_tokens + turn_output_tokens,
                'cost': total_cost,
                'duration_seconds': turn_duration,
                'continuations': continuation_count,
                'stop_reason': stop_reason,
                'cumulative_input': self.cumulative_tokens['input'],
                'cumulative_output': self.cumulative_tokens['output'],
                'cumulative_total': self.cumulative_tokens['total'],
                'cumulative_cost': self.cumulative_cost,
                'context_used_pct': context_used_pct,
                'history_messages': len(self.conversation_history)
            }
            self.turn_details.append(turn_detail)

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
                'context_window_usage': {
                    'total_tokens': self.cumulative_tokens['total'],
                    'context_window': self.context_window,
                    'used_percentage': context_used_pct,
                    'remaining_tokens': context_remaining
                },
                'turn_details': turn_detail,
                'artifacts': {},
                'container_id': new_container_id
            }

        except Exception as e:
            print(f"✗ Error creating message: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }
    
    def start_conversation(self, file_ids: list, prompt_path: str) -> Dict[str, Any]:
        """Start a conversation with files and prompt"""
        prompt_file = Path(prompt_path)
        
        if not prompt_file.exists():
            raise FileNotFoundError(f"Prompt file not found: {prompt_file}")
        
        with open(prompt_file, 'r', encoding='utf-8') as f:
            prompt = f.read()
        
        print(f"📄 Loaded prompt from: {prompt_file}")
        
        result = self.create_message_with_files(file_ids, prompt, 32*1000)
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
            'context_window': {
                'size': self.context_window,
                'used': self.cumulative_tokens['total'],
                'used_percentage': (self.cumulative_tokens['total'] / self.context_window) * 100,
                'remaining': self.context_window - self.cumulative_tokens['total']
            },
            'turn_details': self.turn_details
        }
        
        with open(filepath, 'w') as f:
            json.dump(log_data, f, indent=2)
        
        print(f"📝 Conversation log saved to: {filepath}")
    
    def close_logging(self) -> None:
        """Close logging and restore stdout"""
        if hasattr(self, 'tee') and self.tee:
            sys.stdout = self.original_stdout
            self.tee.close()
            print(f"📝 Logging closed. Log file saved.")
    
    # ================================================================
    # ARTIFACT DOWNLOAD METHODS
    # ================================================================
    
    def download_artifact(self, file_id: str, save_as: Optional[str] = None, downloads_dir: Optional[Path] = None) -> Dict[str, Any]:
        """Download an artifact file using its file ID"""
        if downloads_dir is None:
            downloads_dir = Path('downloads')
        downloads_dir.mkdir(exist_ok=True)
        
        self._log_download_start(file_id)
        
        try:
            # Get metadata
            file_metadata_ = self.client.beta.files.retrieve_metadata(file_id)
            file_metadata = file_metadata_.model_dump(mode="json")
            save_as = file_metadata_.filename
            
            # Download content
            response = self.client.beta.files.download(file_id)
            
            # Extract bytes
            if hasattr(response, 'content'):
                file_content = response.content
            elif hasattr(response, 'read'):
                file_content = response.read()
            else:
                file_content = bytes(response)
            
            # Save file
            filename = f"{file_id}_{save_as}" if save_as else f"{file_id}.txt"
            file_path = downloads_dir / filename
            
            with open(file_path, "w") as f:
                f.write(file_content.decode('utf-8'))
            
            file_size_kb = file_path.stat().st_size / 1024
            self._log_download_success(file_path, file_size_kb)
            
            file_metadata["local_file_path"] = str(file_path)
            return file_metadata
            
        except Exception as e:
            self._log_download_error(e)
            raise
    
    def download_multiple_artifacts(self, file_ids: list, downloads_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
        """Download multiple artifacts"""
        if downloads_dir is None:
            downloads_dir = Path('downloads')
        downloads_dir.mkdir(exist_ok=True)
        
        print(f"\n{'=' * 60}")
        print(f"DOWNLOADING {len(file_ids)} ARTIFACT(S)")
        print(f"{'=' * 60}")
        
        downloaded = {}
        
        for file_id in file_ids:
            try:
                file_metadata = self.download_artifact(file_id, downloads_dir=downloads_dir)
                filename = file_metadata.get("filename")
                downloaded[filename] = file_metadata
            except Exception as e:
                print(f"   ⚠ Skipped {file_id}: {e}")
        
        print(f"\n{'=' * 60}\n")
        return downloaded
    
    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, 'tee') and self.tee:
            self.close_logging()
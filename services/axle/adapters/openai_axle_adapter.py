"""
OpenAI Axle Adapter - Handles all OpenAI operations
File: services/axle/adapters/openai_axle_adapter.py
"""

import os
import sys
import json
import time
import datetime
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional
from openai import OpenAI, AsyncOpenAI
from dotenv import load_dotenv
import structlog

from services.axle.adapters.base_axle_adapter import BaseAxleAdapter
from utils.logging_utils import Tee

load_dotenv()

logger = structlog.get_logger()


class OpenAIAxleAdapter(BaseAxleAdapter):
    """
    Unified OpenAI Axle Adapter - handles file uploads, conversations, and downloads
    """
    
    def __init__(self):
        super().__init__("openai")
        
        # Get API key
        self.api_key = os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError(
                "OPENAI_API_KEY not found in environment variables.\n"
                "Please create a .env file with your API key."
            )
        
        # Initialize sync and async clients
        self.client = OpenAI(api_key=self.api_key)
        self.async_client = AsyncOpenAI(api_key=self.api_key)
        
        # Conversation-specific attributes
        self.conversation_id = None
        self.model = None
        self.max_tokens = None
        self.tee = None
        self.original_stdout = None
        self.last_response_id = None
        self.container = None
        self.container_id = None
        
        # Pricing
        self.input_token_cost = 0.0
        self.output_token_cost = 0.0
        self.context_window = 0
        
        logger.info("OpenAI adapter initialized", provider_id=self.provider_id)
    
    # ================================================================
    # LIFECYCLE MANAGEMENT
    # ================================================================
    
    async def initialize(self) -> None:
        """Initialize OpenAI client"""
        try:
            self._initialized = True
            logger.info("OpenAI adapter initialized", provider_id=self.provider_id)
        except Exception as e:
            logger.error("Failed to initialize OpenAI adapter", error=str(e))
            raise
    
    async def cleanup(self) -> None:
        """Cleanup OpenAI resources"""
        if hasattr(self, 'tee') and self.tee:
            self.close_logging()
        if hasattr(self, 'async_client'):
            await self.async_client.close()
        self._initialized = False
        logger.info("OpenAI adapter cleaned up", provider_id=self.provider_id)
    
    # ================================================================
    # FILE UPLOAD METHODS (ASYNC)
    # ================================================================
    
    async def upload_file(self, file_path: str, purpose: str = "user_data", **kwargs) -> Any:
        """Upload a file to OpenAI using Files API"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        size_kb = file_path.stat().st_size / 1024
        self._log_upload_start(str(file_path), size_kb)
        print(f"   Purpose: {purpose}")
        
        try:
            # Use async client
            with open(file_path, 'rb') as f:
                file_response = await self.async_client.files.create(
                    file=f,
                    purpose=purpose
                )
            
            file_id = file_response.id
            self._log_upload_success(file_id)
            print(f"   Status: {file_response.status}")
            
            # Store file info
            self.uploaded_files[file_path.name] = {
                'file_id': file_id,
                'file_path': str(file_path),
                'file_name': file_path.name,
                'size_bytes': file_response.bytes,
                'size_kb': round(file_response.bytes / 1024, 2),
                'purpose': purpose,
                'status': file_response.status,
                'created_at': file_response.created_at,
                'expires_at': getattr(file_response, 'expires_at', None)
            }
            
            return file_response
            
        except Exception as e:
            self._log_upload_error(e)
            raise
    
    async def upload_multiple_files(self, file_paths: list, purpose: str = "user_data", **kwargs) -> List[Dict[str, Any]]:
        """Upload multiple files to OpenAI"""
        print(f"\n{'=' * 60}")
        print(f"UPLOADING {len(file_paths)} FILE(S) TO OPENAI")
        print(f"{'=' * 60}")
        
        results = []
        for file_path in file_paths:
            try:
                result = await self.upload_file(file_path, purpose=purpose)
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
    
    # ================================================================
    # CONVERSATION METHODS (ASYNC)
    # ================================================================
    
    async def create_conversation(self, conversation_id: int, file_ids: list = None, log_dir: Path = None, **kwargs) -> None:
        """
        Initialize a conversation session with container.

        Args:
            conversation_id: Unique conversation identifier
            file_ids: List of file IDs to attach to the container
            log_dir: Directory for log files (defaults to metrics folder if provided)
        """
        self.conversation_id = conversation_id
        self.model = os.getenv('OPENAI_MODEL') or 'gpt-5.2'
        self.max_tokens = int(os.getenv('MAX_TOKENS'))

        # Reset conversation state
        self.conversation_history = []
        self.turn_number = 0
        self.turn_details = []
        self.cumulative_tokens = {'input': 0, 'output': 0, 'total': 0}
        self.cumulative_cost = 0.0
        self.last_response_id = None

        # Set up logging - use provided log_dir (metrics folder) or fallback
        if log_dir:
            log_dir = Path(log_dir)
        else:
            # Fallback to default metrics location
            log_dir = Path("tmp/poc/default/metrics")
        # Only create if doesn't exist - use existing folder structure
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"openai_conversation_{timestamp}_{conversation_id}.log"
        self.tee = Tee(log_file)
        self.original_stdout = sys.stdout
        sys.stdout = self.tee

        # Container setup (async) - only if file_ids provided
        if file_ids:
            container_memory = os.getenv('CONTAINER_MEMORY_LIMIT') or '16g'
            self.container = await self.async_client.containers.create(
                name=f"conversation-{self.conversation_id}",
                file_ids=file_ids,
                memory_limit=container_memory
            )
            self.container_id = self.container.id
        else:
            self.container = None
            self.container_id = None

        # Set pricing
        self._set_pricing()

        print(f"\n{'='*80}")
        print(f"OpenAI Conversation Initialized")
        print(f"{'='*80}")
        print(f"Conversation ID: {self.conversation_id}")
        print(f"Model: {self.model}")
        if self.container_id:
            print(f"Container ID: {self.container_id}")
        print(f"Log file: {log_file}")
        print(f"{'='*80}\n")
    
    def _set_pricing(self):
        """Set pricing based on model"""
        if "gpt-4.5" in self.model or "gpt-5.2" in self.model:
            self.input_token_cost = 2.50 / 1_000_000
            self.output_token_cost = 10.00 / 1_000_000
            self.context_window = 128000
        elif "gpt-4-turbo" in self.model:
            self.input_token_cost = 10.00 / 1_000_000
            self.output_token_cost = 30.00 / 1_000_000
            self.context_window = 128000
        else:
            self.input_token_cost = 3.00 / 1_000_000
            self.output_token_cost = 15.00 / 1_000_000
            self.context_window = 128000
    
    async def create_message_with_files(
        self,
        file_ids: list,
        user_message: str,
        max_tokens: Optional[int] = None,
        max_retries: int = 1,
        retry_delay: float = 1.0,
        max_continuation_turns: int = 10,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a message with streaming and multi-turn support - COMPLETE IMPLEMENTATION"""
        
        self.turn_number += 1
        turn_start_time = time.time()
        
        # ============================================================
        # TURN HEADER
        # ============================================================
        print(f"\n{'=' * 80}")
        print(f"TURN {self.turn_number} - MESSAGE TO OPENAI")
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

        try:
            # Add to conversation history
            self.conversation_history.append({
                "role": "user",
                "content": user_message
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
            if self.container_id:
                print(f"  Container ID: {self.container_id}")
            
            api_params = {
                "model": self.model,
                "input": self.conversation_history,
                "stream": True,
                "max_output_tokens": max_tokens or self.max_tokens
            }
            
            # Add tools if container exists
            if self.container_id:
                api_params["tools"] = [{
                    "type": "code_interpreter",
                    "container": self.container_id
                }]
                api_params["tool_choice"] = "auto"
            
            # Link to previous response for multi-turn context
            if self.last_response_id:
                api_params["previous_response_id"] = self.last_response_id
                print(f"  Linking to previous response: {self.last_response_id}")
            
            continuation_count = 0
            final_response = None
            full_response_text = ""
            turn_input_tokens = 0
            turn_output_tokens = 0
            turn_total_tokens = 0
            cached_tokens = 0
            reasoning_tokens = 0
            response_status = "failed"
            incomplete_details = None
            annotations = []

            # ============================================================
            # STREAMING RESPONSE
            # ============================================================
            print(f"\n{'=' * 80}")
            print("STREAMING RESPONSE FROM OPENAI")
            print(f"{'=' * 80}\n")

            while continuation_count < max_continuation_turns:
                for attempt in range(max_retries):
                    try:
                        if continuation_count > 0:
                            print(f"\n{'─' * 80}")
                            print(f"CONTINUATION {continuation_count + 1}/{max_continuation_turns}")
                            print(f"{'─' * 80}\n")
                        
                        # Check container status if exists
                        if self.container:
                            print(f"Container Status: {self.container.status}")
                        
                        # Create streaming response (async)
                        stream = await self.async_client.responses.create(**api_params)
                        
                        # Process stream events
                        async for event in stream:
                            # Handle different event types based on OpenAI Responses API
                            if hasattr(event, 'type'):
                                if event.type == 'response.output_text.delta':
                                    # Text chunk
                                    text = getattr(event, 'delta', '')
                                    print(text, end="", flush=True)
                                    full_response_text += text

                                elif event.type == 'response.output_text.annotation.added':
                                    annotation = event.annotation
                                    text = getattr(annotation, 'text', '')
                                    print(text, end="", flush=True)
                                    full_response_text += text

                                elif event.type == 'response.output_text.done':
                                    # Final response with complete data
                                    full_response_text = event.text

                                elif event.type == 'response.content_part.added':
                                    pass  # Can log if needed

                                elif event.type == 'response.content_part.done':
                                    pass  # Can log if needed

                                elif event.type == 'response.output_item.done':
                                    pass  # Can log if needed
                                    
                                elif event.type == 'response.completed':
                                    final_response = event.response
                                    
                                    # Extract response ID for next turn
                                    if final_response.id:
                                        self.last_response_id = final_response.id
                                    
                                    # Parse output array properly
                                    if final_response.output:
                                        for output_item in final_response.output:
                                            if output_item.type == "message":
                                                for content_item in output_item.content:
                                                    if content_item.type == "output_text":
                                                        # This is the actual response text
                                                        if not full_response_text:  # Only if streaming didn't capture it
                                                            full_response_text = content_item.text
                                                    
                                    # Extract usage with detailed tracking
                                    usage = final_response.usage
                                    turn_total_tokens = usage.total_tokens
                                    
                                    # Track cached tokens (cost savings!)
                                    if hasattr(usage, 'input_tokens_details'):
                                        cached_tokens = getattr(usage.input_tokens_details, 'cached_tokens', 0)
                                    if hasattr(usage, 'output_tokens_details'):
                                        reasoning_tokens = getattr(usage.output_tokens_details, 'reasoning_tokens', 0)
                                    
                                    turn_input_tokens = usage.input_tokens
                                    turn_output_tokens = usage.output_tokens
                                    response_status = final_response.status
                                    break
                                    
                                elif event.type == 'response.failed':
                                    final_response = event.response
                                    if final_response.id:
                                        self.last_response_id = final_response.id
                                    response_status = final_response.status
                                    error = final_response.error
                                    full_response_text += "\nresponse failed: " + "\nerror code: " + error.code + "\nerror message: " + error.message
                                    break

                                elif event.type == 'response.incomplete':
                                    final_response = event.response
                                    if final_response.id:
                                        self.last_response_id = final_response.id
                                    response_status = final_response.status
                                    incomplete_details = final_response.incomplete_details
                                    if incomplete_details.reason == 'max_tokens':
                                        turn_input_tokens = self.context_window
                                    full_response_text += "response incomplete: " + incomplete_details.reason
                                    break

                        print("\n")
                        
                        print(f"\n{'─' * 80}")
                        print(f"RESPONSE STATUS: {response_status}")
                        print(f"  Input tokens: {turn_input_tokens:,}")
                        # Display cached tokens if any
                        if cached_tokens > 0:
                            print(f"  💾 Cached tokens: {cached_tokens:,} (cost savings!)")
                        print(f"  Output tokens: {turn_output_tokens:,}")
                        if reasoning_tokens > 0:
                            print(f"  🧠 Reasoning tokens: {reasoning_tokens:,}")
                        print(f"  Total tokens: {turn_total_tokens:,}")
                        
                        # Handle different statuses for multi-turn conversations
                        if response_status == 'incomplete' and incomplete_details and incomplete_details.reason == 'max_tokens':
                            print(f"  ⚠️  Max tokens reached. Stopping continuation.")
                            break

                        if response_status == 'incomplete':
                            # Check incomplete details
                            if hasattr(final_response, 'incomplete_details') and incomplete_details:
                                print(f"  ⚠️  Incomplete reason: {incomplete_details.reason}")
                            
                            # Need continuation - add assistant response and continue
                            self.conversation_history.append({
                                "role": "assistant",
                                "content": full_response_text
                            })
                            api_params["input"] = self.conversation_history
                            continuation_count += 1
                            
                            print(f"  ↻ Continuing conversation (turn {continuation_count}/{max_continuation_turns})...")
                            continue  # Continue the while loop

                        elif response_status == 'failed':
                            print(f"  ❌ Response failed: {final_response.error}")
                            break
                            
                        elif response_status == 'completed':
                            print(f"  ✓ Response completed successfully")
                            break  # Exit continuation loop
                            
                        else:
                            print(f"  ℹ️  Unknown status: {response_status}")
                            break
                        
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        delay = retry_delay * (2 ** attempt)
                        print(f"\n⚠️  API error. Retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})...")
                        await asyncio.sleep(delay)
                        continue
                    break
                
                # Exit continuation loop if not incomplete
                if response_status != 'incomplete' or (incomplete_details and incomplete_details.reason == 'max_tokens'):
                    break

            # Add assistant response to history (if not already added in continuation)
            if response_status != 'incomplete':
                self.conversation_history.append({
                    "role": "assistant",
                    "content": full_response_text
                })

            # ============================================================
            # TURN SUMMARY
            # ============================================================
            turn_duration = time.time() - turn_start_time
            
            # Calculate costs
            input_cost = (turn_input_tokens / 1_000_000) * self.input_token_cost * 1_000_000
            output_cost = (turn_output_tokens / 1_000_000) * self.output_token_cost * 1_000_000
            total_cost = input_cost + output_cost
            
            # Update cumulative totals
            self.cumulative_tokens['input'] += turn_input_tokens
            self.cumulative_tokens['output'] += turn_output_tokens
            self.cumulative_tokens['total'] += turn_total_tokens
            self.cumulative_cost += total_cost

            print(f"\n{'=' * 80}")
            print(f"TURN {self.turn_number} SUMMARY")
            print(f"{'=' * 80}")
            print(f"\n📊 TOKEN USAGE (This Turn):")
            print(f"  Input tokens:  {turn_input_tokens:,}")
            print(f"  Output tokens: {turn_output_tokens:,}")
            print(f"  Total tokens:  {turn_total_tokens:,}")

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
                'total_tokens': turn_total_tokens,
                'cached_tokens': cached_tokens,
                'reasoning_tokens': reasoning_tokens,
                'cost': total_cost,
                'duration_seconds': turn_duration,
                'continuations': continuation_count,
                'response_id': self.last_response_id,
                'response_status': response_status,
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
                'message': final_response,
                'response_text': full_response_text,
                'token_usage': {
                    'input_tokens': turn_input_tokens,
                    'output_tokens': turn_output_tokens,
                    'total_tokens': turn_total_tokens
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
                'container_id': self.container_id
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
    
    async def start_conversation(self, file_ids: list, prompt_path: str) -> Dict[str, Any]:
        """Start a conversation with files and prompt"""
        prompt_file = Path(prompt_path)
        
        if not prompt_file.exists():
            raise FileNotFoundError(f"Prompt file not found: {prompt_file}")
        
        with open(prompt_file, 'r', encoding='utf-8') as f:
            prompt = f.read()
        
        print(f"📄 Loaded prompt from: {prompt_file}")
        
        result = await self.create_message_with_files(file_ids, prompt, 32*1000)
        
        if result.get("message") and hasattr(result["message"], 'model_dump'):
            result_message = result["message"].model_dump(mode="json")
            result["message"] = result_message
        
        return result
    
    def save_conversation_log(self, filepath: str) -> None:
        """Save conversation log to file"""
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
            'turn_details': self.turn_details,
            'container_id': self.container_id
        }
        
        with open(filepath, 'w') as f:
            json.dump(log_data, f, indent=2)
        
        print(f"📝 Conversation log saved to: {filepath}")
    
    def close_logging(self) -> None:
        """Close logging and restore stdout"""
        if hasattr(self, 'tee') and self.tee:
            sys.stdout = self.original_stdout
            self.tee.close()
    
    # ================================================================
    # ARTIFACT DOWNLOAD METHODS (ASYNC)
    # ================================================================
    
    async def download_artifact(self, file_id: str, save_as: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Not used for OpenAI - uses container downloads"""
        raise NotImplementedError("OpenAI uses download_all_container_files")
    
    async def download_multiple_artifacts(self, file_ids: list, downloads_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
        """Not used for OpenAI - uses container downloads"""
        raise NotImplementedError("OpenAI uses download_all_container_files")
    
    async def download_all_container_files(self, container_id: str, downloads_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
        """Download all files (eval reports) from an OpenAI container to artifacts folder.

        Files are saved with format: {filename}
        Example: file1_eval_report.md, summary_eval_report.md
        """
        if downloads_dir is None:
            downloads_dir = Path('artifacts')
        # Only create if doesn't exist - use existing folder structure
        downloads_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n{'=' * 60}")
        print(f"DOWNLOADING FILES FROM CONTAINER: {container_id}")
        print(f"{'=' * 60}")
        
        try:
            # List files (async)
            files = await self.async_client.containers.files.list(container_id)
            download_files = [f for f in files.data if f.source == 'assistant']
            
            if not download_files:
                print("\nNo files found in container.")
                return {}
            
            print(f"   Found {len(download_files)} file(s)")
            
            downloaded = {}
            for file_info in download_files:
                try:
                    print(f"\n📥 Downloading: {file_info.id}")
                    
                    # Download file content (async)
                    file_response = await self.async_client.containers.files.content.retrieve(
                        container_id=container_id,
                        file_id=file_info.id
                    )
                    
                    # Extract bytes
                    if isinstance(file_response, bytes):
                        content_bytes = file_response
                    elif hasattr(file_response, 'content'):
                        content_bytes = file_response.content
                    elif hasattr(file_response, 'read'):
                        content_bytes = file_response.read()
                    else:
                        content_bytes = bytes(file_response)
                    
                    # Save file with clean filename for artifacts folder
                    # Try to get original filename from file_info, fallback to file_id based name
                    if hasattr(file_info, 'filename') and file_info.filename:
                        filename = file_info.filename
                    elif hasattr(file_info, 'path') and file_info.path:
                        filename = Path(file_info.path).name
                    else:
                        # Fallback: use file_id with .json extension
                        filename = file_info.id.replace('cfile_', 'output_')
                        if not filename.endswith('.json') and not filename.endswith('.md'):
                            filename += '.json'

                    local_path = downloads_dir / filename

                    with open(local_path, 'wb') as f:
                        f.write(content_bytes)

                    file_size_kb = local_path.stat().st_size / 1024
                    self._log_download_success(local_path, file_size_kb)

                    downloaded[filename] = {
                        'file_id': file_info.id,
                        'local_path': str(local_path),
                        'filename': filename,
                        'size_kb': file_size_kb,
                        'size_bytes': file_info.bytes if hasattr(file_info, 'bytes') else None
                    }
                except Exception as e:
                    print(f"   ⚠ Skipped {file_info.id}: {e}")
            
            print(f"\n{'=' * 60}")
            print(f"✓ Downloaded {len(downloaded)}/{len(download_files)} file(s)")
            print(f"{'=' * 60}\n")
            
            return downloaded
            
        except Exception as e:
            print(f"\n✗ Failed to download container files: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, 'tee') and self.tee:
            self.close_logging()
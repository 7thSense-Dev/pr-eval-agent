"""
Anthropic Per-File Adapter — one simple text API call per source file.
File: services/axle/adapters/anthropic_per_file_adapter.py
"""

import os
import time
from typing import Dict, Any, Optional

from anthropic import Anthropic, AsyncAnthropic
from dotenv import load_dotenv
import structlog

from services.axle.adapters.per_file_axle_adapter import PerFileAxleAdapter

load_dotenv()
logger = structlog.get_logger()


class AnthropicPerFileAdapter(PerFileAxleAdapter):

    def __init__(self):
        super().__init__("claude_llm")
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not set in environment")
        self.model = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
        self.max_tokens = int(os.getenv("MAX_TOKENS", "64000"))
        self.sync_client = Anthropic(api_key=api_key)
        self.async_client = AsyncAnthropic(api_key=api_key)
        logger.info("Anthropic per-file adapter initialized", model=self.model)

    async def cleanup(self) -> None:
        if self.tee:
            self.close_logging()
        if hasattr(self, "async_client"):
            await self.async_client.close()
        if hasattr(self, "sync_client"):
            self.sync_client.close()
        self._initialized = False

    async def create_message_with_files(
        self,
        file_ids: list,
        user_message: str,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        self.turn_number += 1
        start = time.time()
        import asyncio

        # Anthropic requires streaming for requests that may exceed 10 minutes.
        # Run the synchronous streaming client in a thread pool to avoid blocking.
        def _stream():
            with self.sync_client.messages.stream(
                model=self.model,
                max_tokens=max_tokens or self.max_tokens,
                messages=[{"role": "user", "content": user_message}],
            ) as stream:
                text = stream.get_final_text()
                final = stream.get_final_message()
                return text, final

        loop = asyncio.get_event_loop()
        response_text, final_message = await loop.run_in_executor(None, _stream)

        duration = round(time.time() - start, 2)
        usage = {
            "prompt_tokens": final_message.usage.input_tokens,
            "completion_tokens": final_message.usage.output_tokens,
            "total_tokens": final_message.usage.input_tokens + final_message.usage.output_tokens,
            "cached_tokens": getattr(final_message.usage, "cache_read_input_tokens", 0) or 0,
        }

        self.cumulative_tokens["input"] += usage["prompt_tokens"]
        self.cumulative_tokens["output"] += usage["completion_tokens"]
        self.cumulative_tokens["total"] += usage["total_tokens"]

        print(f"  Duration: {duration}s | Tokens: {usage}")

        return {
            "success": True,
            "response_text": response_text,
            "token_usage": usage,
            "estimated_cost": {"total": 0.0},
            "duration_seconds": duration,
            "model": self.model,
            "container_id": None,
        }

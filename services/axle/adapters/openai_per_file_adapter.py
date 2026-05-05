"""
OpenAI Per-File Adapter — one simple text API call per source file.
File: services/axle/adapters/openai_per_file_adapter.py
"""

import os
import time
from typing import Dict, Any, Optional

from openai import AsyncOpenAI
from dotenv import load_dotenv
import structlog

from services.axle.adapters.per_file_axle_adapter import PerFileAxleAdapter

load_dotenv()
logger = structlog.get_logger()


class OpenAIPerFileAdapter(PerFileAxleAdapter):

    def __init__(self):
        super().__init__("openai_llm")
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY not set in environment")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o")
        self.max_tokens = int(os.getenv("MAX_TOKENS", "64000"))
        self.async_client = AsyncOpenAI(api_key=api_key)
        logger.info("OpenAI per-file adapter initialized", model=self.model)

    async def cleanup(self) -> None:
        if self.tee:
            self.close_logging()
        if hasattr(self, "async_client"):
            await self.async_client.close()
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

        response = await self.async_client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens or self.max_tokens,
            messages=[{"role": "user", "content": user_message}],
        )

        duration = round(time.time() - start, 2)
        usage_obj = response.usage
        cached = 0
        if hasattr(usage_obj, "prompt_tokens_details") and usage_obj.prompt_tokens_details:
            cached = getattr(usage_obj.prompt_tokens_details, "cached_tokens", 0) or 0

        usage = {
            "prompt_tokens": usage_obj.prompt_tokens,
            "completion_tokens": usage_obj.completion_tokens,
            "total_tokens": usage_obj.total_tokens,
            "cached_tokens": cached,
        }

        self.cumulative_tokens["input"] += usage["prompt_tokens"]
        self.cumulative_tokens["output"] += usage["completion_tokens"]
        self.cumulative_tokens["total"] += usage["total_tokens"]

        response_text = response.choices[0].message.content or ""

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

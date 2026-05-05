"""
Gemini Axle Adapter
File: services/axle/adapters/gemini_axle_adapter.py
"""

import os
import time
from typing import Dict, Any, Optional

from google import genai
from google.genai import types
from dotenv import load_dotenv
import structlog

from services.axle.adapters.per_file_axle_adapter import PerFileAxleAdapter

load_dotenv()
logger = structlog.get_logger()


class GeminiAxleAdapter(PerFileAxleAdapter):

    def __init__(self):
        super().__init__("gemini")
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not set in environment")
        self.model = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
        self.client = genai.Client(api_key=api_key)
        logger.info("Gemini axle adapter initialized", model=self.model)

    async def create_message_with_files(
        self,
        file_ids: list,
        user_message: str,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        self.turn_number += 1
        start = time.time()

        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=user_message,
            config=types.GenerateContentConfig(
                max_output_tokens=65536,
                temperature=0.2,
                thinking_config=types.ThinkingConfig(thinking_budget=8192),
            ),
        )

        duration = round(time.time() - start, 2)
        usage = {}
        if response.usage_metadata:
            usage = {
                "prompt_tokens": response.usage_metadata.prompt_token_count or 0,
                "completion_tokens": response.usage_metadata.candidates_token_count or 0,
                "total_tokens": response.usage_metadata.total_token_count or 0,
                "thinking_tokens": (
                    getattr(response.usage_metadata, "thoughts_token_count", 0)
                    or getattr(response.usage_metadata, "thinking_token_count", 0)
                    or 0
                ),
                "cached_tokens": response.usage_metadata.cached_content_token_count or 0,
            }

        self.cumulative_tokens["input"] += usage.get("prompt_tokens", 0)
        self.cumulative_tokens["output"] += usage.get("completion_tokens", 0)
        self.cumulative_tokens["total"] += usage.get("total_tokens", 0)

        print(f"  Duration: {duration}s | Tokens: {usage}")

        return {
            "success": True,
            "response_text": response.text,
            "token_usage": usage,
            "estimated_cost": {"total": 0.0},
            "duration_seconds": duration,
            "model": self.model,
            "container_id": None,
        }

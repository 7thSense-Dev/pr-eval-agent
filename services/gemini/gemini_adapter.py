"""
Gemini Adapter for single-prompt evaluation.
Sends a self-contained text prompt to the Gemini API and returns the response.
"""

import os
import time
import json
from pathlib import Path
from google import genai
from google.genai import types


GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")


class GeminiAdapter:
    """Lightweight adapter for Gemini text-only evaluation calls."""

    def __init__(self, model: str | None = None):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not set in environment")
        self.client = genai.Client(api_key=api_key)
        self.model = model or GEMINI_MODEL

    async def evaluate(self, prompt_text: str) -> dict:
        """
        Send a single evaluation prompt to Gemini and return the response
        along with usage metrics.

        Returns:
            dict with keys: response_text, usage, duration_seconds, model
        """
        start = time.time()

        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt_text,
            config=types.GenerateContentConfig(
                max_output_tokens=65536,
                temperature=0.2,
                thinking_config=types.ThinkingConfig(
                    thinking_budget=8192
                ),
            ),
        )

        duration = time.time() - start

        # Extract usage metrics
        usage = {}
        if response.usage_metadata:
            usage = {
                "prompt_tokens": response.usage_metadata.prompt_token_count or 0,
                "completion_tokens": response.usage_metadata.candidates_token_count or 0,
                "total_tokens": response.usage_metadata.total_token_count or 0,
                "thinking_tokens": getattr(response.usage_metadata, 'thoughts_token_count', None) or getattr(response.usage_metadata, 'thinking_token_count', 0) or 0,
                "cached_tokens": response.usage_metadata.cached_content_token_count or 0,
            }

        return {
            "response_text": response.text,
            "usage": usage,
            "duration_seconds": round(duration, 2),
            "model": self.model,
        }

"""
OpenAI Adapter for single-prompt evaluation.
Sends a self-contained text prompt to the OpenAI API and returns the response.
"""

import os
import time
from openai import OpenAI

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")


class OpenAIAdapter:
    """Lightweight adapter for OpenAI text-only evaluation calls."""

    def __init__(self, model: str | None = None):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY not set in environment")
        self.client = OpenAI(api_key=api_key)
        self.model = model or OPENAI_MODEL

    async def evaluate(self, prompt_text: str) -> dict:
        """
        Send a single evaluation prompt to OpenAI and return the response
        along with usage metrics.

        Returns:
            dict with keys: response_text, usage, duration_seconds, model
        """
        start = time.time()

        # Call OpenAI API
        response = self.client.chat.completions.create(
            model=self.model,
            max_tokens=4096,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt_text}],
        )

        duration = time.time() - start

        response_text = response.choices[0].message.content or ""
        usage = {
            "prompt_tokens": response.usage.prompt_tokens or 0,
            "completion_tokens": response.usage.completion_tokens or 0,
            "total_tokens": response.usage.total_tokens or 0,
        }

        return {
            "response_text": response_text,
            "usage": usage,
            "duration_seconds": round(duration, 2),
            "model": self.model,
        }

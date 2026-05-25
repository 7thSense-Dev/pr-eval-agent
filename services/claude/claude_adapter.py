"""
Claude Adapter for single-prompt evaluation.
Sends a self-contained text prompt to the Claude API and returns the response.
"""

import os
import time
from anthropic import Anthropic

CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-3-5-sonnet-latest")


class ClaudeAdapter:
    """Lightweight adapter for Claude text-only evaluation calls."""

    def __init__(self, model: str | None = None):
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not set in environment")
        self.client = Anthropic(api_key=api_key)
        self.model = model or CLAUDE_MODEL

    async def evaluate(self, prompt_text: str) -> dict:
        """
        Send a single evaluation prompt to Claude and return the response
        along with usage metrics.

        Returns:
            dict with keys: response_text, usage, duration_seconds, model
        """
        start = time.time()

        # Call Anthropic API
        response = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt_text}],
        )

        duration = time.time() - start

        # Extract response text
        response_text = ""
        for block in response.content:
            if hasattr(block, "text"):
                response_text += block.text

        usage = {
            "prompt_tokens": response.usage.input_tokens or 0,
            "completion_tokens": response.usage.output_tokens or 0,
            "total_tokens": (response.usage.input_tokens or 0) + (response.usage.output_tokens or 0),
        }

        return {
            "response_text": response_text,
            "usage": usage,
            "duration_seconds": round(duration, 2),
            "model": self.model,
        }

"""
Promptfoo Python provider for pr-eval-agent.

Receives a DB-record-shaped dict in `context.vars.record`, runs build_prompt()
against the production templates, and dispatches to the configured LLM adapter
(Gemini for now; Claude/OpenAI can be added the same way).

Returns the JSON shape promptfoo expects:
    {"output": str, "tokenUsage": {...}, "cost": float}
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

# Make pr-eval-agent's top-level modules importable when promptfoo invokes us.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from utils.prompt_builder import build_prompt


def call_api(prompt: str, options: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    cfg = (options or {}).get("config", {}) or {}
    vars_ = (context or {}).get("vars", {}) or {}

    record = vars_.get("record")
    if not isinstance(record, dict):
        return {"output": "", "error": "test var `record` must be a dict (use file:// to load a JSON fixture)"}

    review_approach = cfg.get("review_approach") or vars_.get("review_approach") or "axle"
    project_context = vars_.get("project_context")
    provider_name = (cfg.get("provider") or "gemini").lower()
    model_name = cfg.get("model")

    prompt_text = build_prompt(
        record,
        review_approach=review_approach,
        project_context=project_context,
    )

    try:
        result = asyncio.run(_dispatch(provider_name, model_name, prompt_text))
    except Exception as exc:
        return {"output": "", "error": f"{type(exc).__name__}: {exc}"}

    usage = result.get("usage") or {}
    return {
        "output": result.get("response_text", ""),
        "tokenUsage": {
            "prompt": usage.get("input_tokens") or usage.get("prompt_tokens"),
            "completion": usage.get("output_tokens") or usage.get("completion_tokens"),
            "total": usage.get("total_tokens"),
        },
        "metadata": {
            "model": result.get("model"),
            "provider": provider_name,
            "review_approach": review_approach,
            "duration_seconds": result.get("duration_seconds"),
        },
    }


async def _dispatch(provider_name: str, model_name: str | None, prompt_text: str) -> dict[str, Any]:
    if provider_name == "gemini":
        from services.gemini.gemini_adapter import GeminiAdapter

        adapter = GeminiAdapter(model=model_name) if model_name else GeminiAdapter()
        return await adapter.evaluate(prompt_text)

    elif provider_name in ("claude", "anthropic"):
        from services.claude.claude_adapter import ClaudeAdapter

        adapter = ClaudeAdapter(model=model_name) if model_name else ClaudeAdapter()
        return await adapter.evaluate(prompt_text)

    elif provider_name == "openai":
        from services.openai.openai_adapter import OpenAIAdapter

        adapter = OpenAIAdapter(model=model_name) if model_name else OpenAIAdapter()
        return await adapter.evaluate(prompt_text)

    raise NotImplementedError(
        f"provider={provider_name!r} not wired yet. Add a branch in _dispatch() "
        f"that calls the appropriate adapter (claude / openai / etc.)."
    )

"""
FastAPI server for PR Evaluation Pipeline
File: api.py

Run with:
    uvicorn api:app --reload --port 8000

Swagger UI:
    http://localhost:8000/docs
"""

import asyncio
import traceback
from enum import Enum
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from db import get_extractor
from services.axle import AxleService
from flow_evaluation import (
    copy_templates,
    build_file_paths,
    PipelineOrchestrator,
    PROJECT_ROOT,
    DEFAULT_AXLE_INPUT,
    DEFAULT_LLM_INPUT,
)

load_dotenv()

app = FastAPI(
    title="PR Evaluation API",
    description=(
        "Extract PR data from the database and optionally run a review pipeline.\n\n"
        "**Approaches**\n"
        "- `extract_only` – DB extraction only, no review\n"
        "- `axle` – DB extraction + Axle review engine\n"
        "- `llm` – DB extraction + LLM review (claude or openai)"
    ),
    version="1.0.0",
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch any unhandled exception and return its message instead of a bare 500."""
    tb = traceback.format_exc()
    print(f"\n[UNHANDLED EXCEPTION] {type(exc).__name__}: {exc}\n{tb}")
    return JSONResponse(
        status_code=500,
        content={"error": type(exc).__name__, "detail": str(exc), "traceback": tb},
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    database: str
    message: str


@app.get("/health", response_model=HealthResponse, summary="Health check")
def health_check() -> HealthResponse:
    """Check that the API is running and the database is reachable."""
    try:
        extractor = get_extractor()
        extractor.connect()
        extractor.disconnect()
        db_status = "ok"
        message = "API and database are healthy"
    except Exception as exc:
        db_status = "unreachable"
        message = f"API is running but database connection failed: {exc}"

    return HealthResponse(
        status="ok",
        database=db_status,
        message=message,
    )


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Approach(str, Enum):
    extract_only = "extract_only"
    axle = "axle"
    llm = "llm"


class Provider(str, Enum):
    openai = "openai"
    claude = "claude"


# ---------------------------------------------------------------------------
# Response model
# ---------------------------------------------------------------------------

class EvalResponse(BaseModel):
    pr_num: str
    repo_name: str
    review_id: Optional[str] = None
    output_path: Optional[str] = None
    message: str


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@app.get(
    "/run",
    response_model=EvalResponse,
    summary="Run PR evaluation pipeline",
    description=(
        "Extracts PR data from the database and, depending on `approach`, "
        "runs the review pipeline.\n\n"
        "| approach | what happens |\n"
        "|---|---|\n"
        "| `extract_only` | DB extraction only – no review |\n"
        "| `axle` | DB extraction → Axle review engine |\n"
        "| `llm` | DB extraction → LLM review (set `provider` to `claude` or `openai`) |"
    ),
)
async def run_evaluation(
    pr_number: str = Query(..., description="PR number, e.g. `123`"),
    repo_name: str = Query(..., description="Repository in `owner/repo` format, e.g. `myorg/myrepo`"),
    approach: Approach = Query(..., description="Review approach: `extract_only` | `axle` | `llm`"),
    provider: Provider = Query(Provider.openai, description="LLM provider: `openai` or `claude`. Optional — defaults to `openai`. Only used when approach is `llm`."),
    review_id: Optional[str] = Query(None, description="Optional review_id to narrow the database extraction"),
) -> EvalResponse:
    pr_number = pr_number.strip()
    repo_name = repo_name.strip()
    # -----------------------------------------------------------------------
    # Step 1 – DB extraction
    # -----------------------------------------------------------------------
    try:
        extractor = get_extractor()
        export = extractor.export_specific_pr(
            repository=repo_name,
            pr_number=pr_number,
            output_dir=PROJECT_ROOT,
            cleanup_folder=True,
            review_id=review_id,
        )
    except Exception as exc:
        return EvalResponse(
            pr_num=pr_number,
            repo_name=repo_name,
            review_id=review_id,
            output_path="",
            message=f"failed: DB extraction error — {exc}",
        )

    if not export.get("success"):
        return EvalResponse(
            pr_num=pr_number,
            repo_name=repo_name,
            review_id=review_id,
            output_path="",
            message=f"failed: {export.get('message', 'No reviews found for this PR')}",
        )

    pr_result = export["pr_results"][0]
    pr_dir = Path(pr_result["pr_dir"])
    output_path = str(pr_dir)

    # -----------------------------------------------------------------------
    # extract_only – return immediately after extraction
    # -----------------------------------------------------------------------
    if approach == Approach.extract_only:
        return EvalResponse(
            pr_num=pr_number,
            repo_name=repo_name,
            review_id=review_id,
            output_path=output_path,
            message="success",
        )

    # -----------------------------------------------------------------------
    # Step 2 – Copy mode-specific templates into PR folder root
    # -----------------------------------------------------------------------
    input_dir = DEFAULT_AXLE_INPUT if approach == Approach.axle else DEFAULT_LLM_INPUT
    try:
        copy_templates(pr_dir, approach.value, input_dir)
    except Exception as exc:
        return EvalResponse(
            pr_num=pr_number,
            repo_name=repo_name,
            review_id=review_id,
            message=f"failed: Template copy error — {type(exc).__name__}: {exc}",
        )

    # -----------------------------------------------------------------------
    # Step 3 – Collect all files to upload
    # -----------------------------------------------------------------------
    uploaded_dir = pr_dir / "uploaded_to_eval_agent"
    try:
        file_paths = build_file_paths(pr_dir, uploaded_dir, approach.value)
    except Exception as exc:
        return EvalResponse(
            pr_num=pr_number,
            repo_name=repo_name,
            review_id=review_id,
            message=f"failed: File list error — {type(exc).__name__}: {exc}",
        )

    prompt_path = str(pr_dir / "code_execution_prompt.txt")
    if not Path(prompt_path).exists():
        return EvalResponse(
            pr_num=pr_number,
            repo_name=repo_name,
            review_id=review_id,
            message=f"failed: Prompt file not found at {prompt_path}",
        )

    # -----------------------------------------------------------------------
    # Step 4 – Run review pipeline
    # -----------------------------------------------------------------------
    try:
        if approach == Approach.axle:
            success = await _run_axle(pr_dir, file_paths, prompt_path, provider.value)
        else:
            success = await _run_llm(pr_dir, file_paths, prompt_path, provider.value)
    except Exception as exc:
        return EvalResponse(
            pr_num=pr_number,
            repo_name=repo_name,
            review_id=review_id,
            message=f"failed: Review pipeline error — {exc}",
        )

    return EvalResponse(
        pr_num=pr_number,
        repo_name=repo_name,
        review_id=review_id,
        output_path=output_path,
        message="success" if success else "failed: Review pipeline returned a non-zero exit code",
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

async def _run_axle(pr_dir: Path, file_paths: list, prompt_path: str, provider: str) -> bool:
    """Run Axle review engine (async)."""
    axle_service = AxleService(project_root=PROJECT_ROOT, pr_dir=str(pr_dir))
    try:
        result = await axle_service.execute_task(
            provider=provider,
            file_paths=file_paths,
            prompt_path=prompt_path,
        )
        return result.get("success", False)
    finally:
        await axle_service.cleanup()


async def _run_llm(pr_dir: Path, file_paths: list, prompt_path: str, provider: str) -> bool:
    """Run LLM pipeline (sync wrapped in thread pool to avoid blocking the event loop)."""
    reports_dir = pr_dir / "reports_generated"
    metrics_dir = pr_dir / "metrics"
    reports_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    orchestrator = PipelineOrchestrator(provider_id=provider, output_dir=str(metrics_dir))
    exit_code = await asyncio.to_thread(orchestrator.run, file_paths, prompt_path, reports_dir)
    return exit_code == 0

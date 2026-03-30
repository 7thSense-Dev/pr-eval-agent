"""
FastAPI server for PR Evaluation Pipeline
File: api.py

Run with:
    uvicorn api:app --reload --port 8000

Swagger UI:
    http://localhost:8000/docs
"""

import asyncio
import json
import traceback
import datetime as _dt
from enum import Enum
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from db import get_extractor
from services.axle import AxleService
from services.gemini import GeminiAdapter
from utils.prompt_builder import build_prompt, build_summary_prompt
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
        "Extract PR data from the database and run the review evaluation pipeline.\n\n"
        "**Required inputs:** PR number, repository, review ID, review approach\n\n"
        "**Approaches**\n"
        "- `extract_only` – DB extraction only, no review\n"
        "- `axle` – code_execution_prompt + audit YAML + review_guidelines + output_format\n"
        "- `llm` – code_execution_prompt + audit YAML (guidelines embedded in prompt)\n\n"
        "**Providers** (optional, default: gemini)\n"
        "- `gemini` – per-file prompt via Gemini API + summary report\n"
        "- `claude` / `openai` – file upload via AxleService"
    ),
    version="1.1.0",
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
    gemini = "gemini"
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
        "Extracts PR data from the database and runs the evaluation pipeline.\n\n"
        "**Required:** `pr_number`, `repo_name`, `review_id`, `approach`\n\n"
        "| approach | what happens |\n"
        "|---|---|\n"
        "| `extract_only` | DB extraction only – no review |\n"
        "| `axle` | DB extraction → evaluation (per-file + summary report) |\n"
        "| `llm` | DB extraction → evaluation (per-file + summary report) |\n\n"
        "| provider | how it evaluates |\n"
        "|---|---|\n"
        "| `gemini` (default) | Per-file prompt via Gemini API + summary report |\n"
        "| `claude` / `openai` | File upload via AxleService conversation |"
    ),
)
async def run_evaluation(
    pr_number: str = Query(..., description="PR number, e.g. `123`"),
    repo_name: str = Query(..., description="Repository in `owner/repo` format, e.g. `myorg/myrepo`"),
    review_id: str = Query(..., description="Review ID — identifies the specific review run in DB"),
    approach: Approach = Query(..., description="Review approach: `extract_only` | `axle` | `llm`"),
    provider: Provider = Query(Provider.gemini, description="LLM provider: `gemini` (default), `claude`, or `openai`"),
) -> EvalResponse:
    pr_number = pr_number.strip()
    repo_name = repo_name.strip()

    # -----------------------------------------------------------------------
    # Step 0 – Validate that review_id belongs to this PR
    # -----------------------------------------------------------------------
    try:
        extractor = get_extractor()
        belongs = extractor.validate_review_belongs_to_pr(
            repository=repo_name,
            pr_number=pr_number,
            review_id=review_id,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to validate review ownership: {exc}",
        )

    if not belongs:
        raise HTTPException(
            status_code=422,
            detail=(
                f"review_id '{review_id}' does not belong to PR #{pr_number} "
                f"in repository '{repo_name}'."
            ),
        )

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
    # Route to provider
    # -----------------------------------------------------------------------
    review_approach = approach.value
    input_dir = DEFAULT_AXLE_INPUT if approach == Approach.axle else DEFAULT_LLM_INPUT

    if provider == Provider.gemini:
        # --- Gemini: per-file prompt + summary report ---
        try:
            success = await _run_gemini(
                pr_dir=pr_dir,
                repo_name=repo_name,
                pr_number=pr_number,
                review_id=review_id,
                review_approach=review_approach,
                input_dir=input_dir,
            )
        except Exception as exc:
            return EvalResponse(
                pr_num=pr_number,
                repo_name=repo_name,
                review_id=review_id,
                message=f"failed: Gemini pipeline error — {exc}",
            )
    else:
        # --- AxleService: claude / openai ---
        try:
            copy_templates(pr_dir, review_approach, input_dir)
        except Exception as exc:
            return EvalResponse(
                pr_num=pr_number,
                repo_name=repo_name,
                review_id=review_id,
                message=f"failed: Template copy error — {type(exc).__name__}: {exc}",
            )

        uploaded_dir = pr_dir / "uploaded_to_eval_agent"
        try:
            file_paths = build_file_paths(pr_dir, uploaded_dir, review_approach)
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

        try:
            if review_approach == "axle":
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

async def _run_gemini(
    pr_dir: Path,
    repo_name: str,
    pr_number: str,
    review_id: str,
    review_approach: str,
    input_dir: Path,
) -> bool:
    """Run Gemini per-file evaluation + summary report."""
    reports_dir = pr_dir / "reports_generated"
    metrics_dir = pr_dir / "metrics"
    prompt_dir = pr_dir / "uploaded_to_eval_agent" / "file_prompt"
    reports_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    prompt_dir.mkdir(parents=True, exist_ok=True)

    # Query DB for file records
    extractor = get_extractor()
    records = extractor.get_latest_pr_reviews(
        repository=repo_name, pr_number=pr_number
    )
    records = [r for r in records if r.get("head_content")]
    if not records:
        raise ValueError(f"No file records with content found for PR #{pr_number}")

    # Fetch project context
    project_context = None
    try:
        ctx = extractor.get_context_from_review_metrics(
            repository=repo_name, pr_number=pr_number, review_id=review_id
        )
        if not ctx:
            ctx = extractor.get_repo_context(
                repository=repo_name, pr_number=pr_number
            )
        project_context = ctx or None
    except Exception:
        pass

    adapter = GeminiAdapter()
    errors = []

    # --- Per-file evaluation ---
    for i, record in enumerate(records, 1):
        fname = Path(record["file"]).name
        base = Path(record["file"]).stem
        print(f"  [{i}/{len(records)}] Evaluating: {fname}")

        prompt_text = build_prompt(
            record,
            review_approach=review_approach,
            input_dir=input_dir,
            project_context=project_context,
        )
        # Save prompt for reference
        with open(prompt_dir / f"{fname}.txt", "w", encoding="utf-8") as f:
            f.write(prompt_text)

        try:
            result = await adapter.evaluate(prompt_text)
            with open(reports_dir / f"{base}_eval_report.md", "w", encoding="utf-8") as f:
                f.write(result["response_text"])
            metrics = {
                "file": record["file"], "model": result["model"],
                "provider": "gemini", "review_approach": review_approach,
                "duration_seconds": result["duration_seconds"],
                "usage": result["usage"],
                "timestamp": _dt.datetime.now().isoformat(),
            }
            with open(metrics_dir / f"{base}_gemini_metrics.json", "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=2)
            print(f"  [{i}/{len(records)}] Saved: {base}_eval_report.md")
        except Exception as exc:
            print(f"  [{i}/{len(records)}] ERROR: {exc}")
            errors.append(str(exc))

    # --- Summary report (requires 2+ eval reports) ---
    existing_reports = list(reports_dir.glob("*_eval_report.md"))
    if len(existing_reports) >= 2:
        try:
            summary_prompt_text = build_summary_prompt(
                reports_dir=reports_dir,
                review_approach=review_approach,
                input_dir=input_dir,
            )
            with open(prompt_dir / "summary_report_prompt.txt", "w", encoding="utf-8") as f:
                f.write(summary_prompt_text)

            summary_result = await adapter.evaluate(summary_prompt_text)
            summary_filename = (
                "LLM_EVAL_META_ANALYSIS.md" if review_approach == "llm"
                else "AXLE_EVAL_META_ANALYSIS.md"
            )
            with open(reports_dir / summary_filename, "w", encoding="utf-8") as f:
                f.write(summary_result["response_text"])
            summary_metrics = {
                "type": "summary_report", "model": summary_result["model"],
                "provider": "gemini", "review_approach": review_approach,
                "duration_seconds": summary_result["duration_seconds"],
                "usage": summary_result["usage"],
                "files_summarized": len(existing_reports),
                "timestamp": _dt.datetime.now().isoformat(),
            }
            with open(metrics_dir / "summary_gemini_metrics.json", "w", encoding="utf-8") as f:
                json.dump(summary_metrics, f, indent=2)
            print(f"  Summary report saved: {summary_filename}")
        except Exception as exc:
            print(f"  [WARN] Summary report failed: {exc}")

    return len(errors) == 0


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

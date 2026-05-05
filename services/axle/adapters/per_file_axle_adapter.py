"""
PerFileAxleAdapter — shared base for per-file text-prompt evaluation.
File: services/axle/adapters/per_file_axle_adapter.py

Providers that evaluate one source file at a time (Gemini, Claude, OpenAI)
all share the same loop:
  1. Store uploaded file paths locally (no API upload).
  2. Extract source files from files.zip.
  3. Build one text prompt per file via build_prompt().
  4. Call create_message_with_files() — overridden per provider.
  5. Write eval reports directly to artifacts_dir.
  6. Build and send a summary prompt when 2+ reports exist.

Subclasses only need to override create_message_with_files() with their
provider's API call.
"""

import sys
import json
import time
import zipfile
import datetime
from abc import abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

from services.axle.adapters.base_axle_adapter import BaseAxleAdapter
from utils.logging_utils import Tee

logger = structlog.get_logger()


class PerFileAxleAdapter(BaseAxleAdapter):
    """
    Base adapter for per-file text-prompt evaluation.
    Shared by GeminiAxleAdapter, AnthropicPerFileAdapter, OpenAIPerFileAdapter.
    """

    def __init__(self, provider_id: str):
        super().__init__(provider_id)
        self.conversation_id = None
        self.artifacts_dir: Optional[Path] = None
        self.tee = None
        self.original_stdout = None
        self.all_metrics: list = []

    # ================================================================
    # LIFECYCLE MANAGEMENT
    # ================================================================

    async def initialize(self) -> None:
        self._initialized = True

    async def cleanup(self) -> None:
        if self.tee:
            self.close_logging()
        self._initialized = False

    # ================================================================
    # FILE "UPLOAD" — LOCAL STORE, NO API CALL
    # ================================================================

    async def upload_file(self, file_path: str, **kwargs) -> Any:
        fp = Path(file_path)
        if not fp.exists():
            raise FileNotFoundError(f"File not found: {fp}")
        size_kb = fp.stat().st_size / 1024
        self._log_upload_start(str(fp), size_kb)
        self.uploaded_files[fp.name] = {
            "file_id": str(fp),
            "file_path": str(fp),
            "file_name": fp.name,
            "size_kb": round(size_kb, 2),
        }
        self._log_upload_success(str(fp))
        return fp

    async def upload_multiple_files(self, file_paths: list, **kwargs) -> List[Dict[str, Any]]:
        results = []
        for fp in file_paths:
            try:
                await self.upload_file(fp)
                results.append({"success": True, "file_path": fp, "file_id": fp})
            except Exception as exc:
                results.append({"success": False, "file_path": fp, "error": str(exc)})
        return results

    # ================================================================
    # CONVERSATION MANAGEMENT
    # ================================================================

    async def create_conversation(self, conversation_id: int, log_dir: Path = None, **kwargs) -> None:
        self.conversation_id = conversation_id
        self.turn_number = 0
        self.all_metrics = []
        self.cumulative_tokens = {"input": 0, "output": 0, "total": 0}
        self.cumulative_cost = 0.0

        log_dir = Path(log_dir) if log_dir else Path("tmp/poc/default/metrics")
        log_dir.mkdir(parents=True, exist_ok=True)

        self.artifacts_dir = log_dir.parent / "reports_generated"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{self.provider_id}_conversation_{timestamp}_{conversation_id}.log"
        self.tee = Tee(log_file)
        self.original_stdout = sys.stdout
        sys.stdout = self.tee

        print(f"\n{'='*80}")
        print(f"{self.provider_id.upper()} Per-File Evaluation Initialized")
        print(f"{'='*80}")
        print(f"Conversation ID : {self.conversation_id}")
        print(f"Artifacts Dir   : {self.artifacts_dir}")
        print(f"Log file        : {log_file}")
        print(f"{'='*80}\n")

    @abstractmethod
    async def create_message_with_files(
        self,
        file_ids: list,
        user_message: str,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Send a single text prompt to the provider and return the response.

        Must return a dict with keys:
          success, response_text, token_usage, duration_seconds, model, container_id
        """

    async def start_conversation(self, file_ids: list, prompt_path: str) -> Dict[str, Any]:
        """
        Per-file evaluation loop — identical for all providers.

        file_ids are local file paths (stored by upload_multiple_files).
        prompt_path is accepted for interface compatibility but not used here —
        templates are read from source/gemini_approach_input/ via build_prompt().
        """
        from utils.prompt_builder import build_prompt, build_summary_prompt

        uploaded_paths = [Path(fid) for fid in file_ids]

        files_zip = next((p for p in uploaded_paths if p.name == "files.zip"), None)
        if not files_zip or not files_zip.exists():
            raise ValueError("files.zip not found in uploaded files")

        logs_zip = next((p for p in uploaded_paths if p.name == "log-files.zip"), None)

        project_context: Optional[str] = None
        # 1. Check uploaded files (inside uploaded_to_eval_agent/)
        for p in uploaded_paths:
            if "project_context" in p.name and p.exists():
                try:
                    project_context = p.read_text(encoding="utf-8")
                    print(f"  Project context loaded: {p.name} ({len(project_context):,} chars)")
                    break
                except Exception:
                    pass
        # 2. Fallback: DataExporter writes project_context.txt at the PR dir root
        if project_context is None:
            pr_dir = self.artifacts_dir.parent
            for fname in ("project_context.txt", "project_context.md"):
                ctx_path = pr_dir / fname
                if ctx_path.exists():
                    try:
                        project_context = ctx_path.read_text(encoding="utf-8")
                        print(f"  Project context loaded: {fname} from PR dir ({len(project_context):,} chars)")
                        break
                    except Exception:
                        pass
        if project_context is None:
            print("  Project context: not found — proceeding without it")

        uploaded_names = {p.name for p in uploaded_paths}
        review_approach = "axle" if "review_guidelines.md" in uploaded_names else "llm"
        print(f"\nReview approach : {review_approach.upper()}")
        print(f"Provider        : {self.provider_id.upper()}")

        # ----------------------------------------------------------------
        # Build a {stem -> log_data} index from log-files.zip so we can
        # populate each source-file record with its matching prompt log,
        # response log, and metrics log.
        #
        # log-files.zip naming convention (set by DataExporter):
        #   {FileStem}_prompt.txt      — full prompt sent to the review LLM
        #   {FileStem}_response.json   — review LLM's JSON response
        #   {FileStem}_metrics.json    — token usage + latency metrics
        # ----------------------------------------------------------------
        log_index: dict = {}   # stem -> {"prompt": str, "review_result": str, "metrics": str}
        if logs_zip and logs_zip.exists():
            with zipfile.ZipFile(logs_zip, "r") as lz:
                for entry in lz.namelist():
                    if entry.endswith("/"):
                        continue
                    try:
                        raw = lz.read(entry).decode("utf-8", errors="replace")
                    except Exception as exc:
                        logger.warning("Skipped log-files entry", entry=entry, error=str(exc))
                        continue

                    name = Path(entry).name
                    if name.endswith("_prompt.txt"):
                        stem = name[: -len("_prompt.txt")]
                        log_index.setdefault(stem, {})["prompt"] = raw
                    elif name.endswith("_response.json"):
                        stem = name[: -len("_response.json")]
                        log_index.setdefault(stem, {})["review_result"] = raw
                    elif name.endswith("_metrics.json"):
                        stem = name[: -len("_metrics.json")]
                        log_index.setdefault(stem, {})["llm_service_metrics"] = raw

            print(f"  Log index built: {len(log_index)} file(s) found in log-files.zip")
        else:
            print("  [WARN] log-files.zip not found — prompts will have no LLM review logs")

        records = []
        with zipfile.ZipFile(files_zip, "r") as zf:
            for entry in sorted(zf.namelist()):
                if entry.endswith("/"):
                    continue
                try:
                    content = zf.read(entry).decode("utf-8", errors="replace")
                    stem = Path(entry).stem
                    logs = log_index.get(stem, {})
                    records.append({
                        "file": entry,
                        "head_content": content,
                        "prompt": logs.get("prompt", ""),
                        "review_result": logs.get("review_result"),
                        "llm_service_metrics": logs.get("llm_service_metrics"),
                    })
                    if logs:
                        print(f"    Matched logs for: {entry} (stem={stem!r})")
                    else:
                        print(f"    [WARN] No logs matched for: {entry} (stem={stem!r})")
                except Exception as exc:
                    logger.warning("Skipped zip entry", entry=entry, error=str(exc))

        if not records:
            raise ValueError("No files extracted from files.zip")

        print(f"\nEvaluating {len(records)} file(s)...\n")

        errors: list = []
        prompt_dir = self.artifacts_dir.parent / "uploaded_to_eval_agent" / "file_prompt"
        prompt_dir.mkdir(parents=True, exist_ok=True)
        metrics_dir = self.artifacts_dir.parent / "metrics"
        metrics_dir.mkdir(parents=True, exist_ok=True)

        for i, record in enumerate(records, 1):
            fname = Path(record["file"]).name
            base = Path(record["file"]).stem
            print(f"  [{i}/{len(records)}] Evaluating: {fname}")

            prompt_text = build_prompt(
                record,
                review_approach=review_approach,
                project_context=project_context,
                provider=self.provider_id,  # routes to the correct source/<provider>_approach_input/
            )

            with open(prompt_dir / f"{fname}.txt", "w", encoding="utf-8") as f:
                f.write(prompt_text)

            try:
                result = await self.create_message_with_files([], prompt_text)

                with open(self.artifacts_dir / f"{base}_eval_report.md", "w", encoding="utf-8") as f:
                    f.write(result["response_text"])

                metrics = {
                    "file": record["file"],
                    "model": result["model"],
                    "provider": self.provider_id,
                    "review_approach": review_approach,
                    "duration_seconds": result["duration_seconds"],
                    "usage": result["token_usage"],
                    "timestamp": datetime.datetime.now().isoformat(),
                }
                with open(metrics_dir / f"{base}_{self.provider_id}_metrics.json", "w", encoding="utf-8") as f:
                    json.dump(metrics, f, indent=2)

                self.all_metrics.append(metrics)
                print(f"  [{i}/{len(records)}] Saved: {base}_eval_report.md")

            except Exception as exc:
                print(f"  [{i}/{len(records)}] ERROR: {exc}")
                errors.append(str(exc))

        existing_reports = list(self.artifacts_dir.glob("*_eval_report.md"))
        if len(existing_reports) >= 2:
            try:
                summary_prompt_text = build_summary_prompt(
                    reports_dir=self.artifacts_dir,
                    review_approach=review_approach,
                    provider=self.provider_id,  # routes to the correct source/<provider>_approach_input/
                )

                with open(prompt_dir / "summary_report_prompt.txt", "w", encoding="utf-8") as f:
                    f.write(summary_prompt_text)

                summary_result = await self.create_message_with_files([], summary_prompt_text)
                summary_filename = (
                    "LLM_EVAL_META_ANALYSIS.md"
                    if review_approach == "llm"
                    else "AXLE_EVAL_META_ANALYSIS.md"
                )
                with open(self.artifacts_dir / summary_filename, "w", encoding="utf-8") as f:
                    f.write(summary_result["response_text"])

                with open(metrics_dir / f"summary_{self.provider_id}_metrics.json", "w", encoding="utf-8") as f:
                    json.dump({
                        "type": "summary_report",
                        "model": summary_result["model"],
                        "provider": self.provider_id,
                        "review_approach": review_approach,
                        "duration_seconds": summary_result["duration_seconds"],
                        "usage": summary_result["token_usage"],
                        "files_summarized": len(existing_reports),
                        "timestamp": datetime.datetime.now().isoformat(),
                    }, f, indent=2)

                print(f"  Summary report saved: {summary_filename}")

            except Exception as exc:
                print(f"  [WARN] Summary report failed: {exc}")

        return {
            "success": len(errors) == 0,
            "response_text": "",
            "token_usage": self.cumulative_tokens.copy(),
            "estimated_cost": {"total": 0.0},
            "container_id": None,
            "errors": errors,
        }

    # ================================================================
    # LOGS
    # ================================================================

    def save_conversation_log(self, filepath: str) -> None:
        log_data = {
            "conversation_id": self.conversation_id,
            "provider": self.provider_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "total_turns": self.turn_number,
            "cumulative_tokens": self.cumulative_tokens,
            "cumulative_cost": self.cumulative_cost,
            "turn_details": self.all_metrics,
        }
        with open(filepath, "w") as f:
            json.dump(log_data, f, indent=2)

    def close_logging(self) -> None:
        if self.tee:
            sys.stdout = self.original_stdout
            self.tee.close()
            self.tee = None

    # ================================================================
    # ARTIFACT DOWNLOADS — NOT USED (reports written in start_conversation)
    # ================================================================

    async def download_artifact(self, file_id: str, save_as: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError("Per-file adapter writes reports directly in start_conversation")

    async def download_multiple_artifacts(self, file_ids: list, downloads_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
        raise NotImplementedError("Per-file adapter writes reports directly in start_conversation")

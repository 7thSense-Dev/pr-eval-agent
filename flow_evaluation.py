"""
Unified PR Evaluation Flow
File: flow_evaluation.py

Single entry-point for all PR evaluation. Changing --provider switches
between Gemini (direct prompt), Claude, or OpenAI (AxleService).

--review-approach controls which templates are included in the prompt:
  - axle: code_execution_prompt + audit YAML + review_guidelines + output_format
  - llm:  code_execution_prompt + audit YAML (guidelines already embedded in prompt)

--provider controls where the evaluation runs:
  - gemini:  Query DB -> build prompt per file -> send to Gemini API -> save report
  - claude:  Extract files -> upload to AxleService -> run conversation -> download reports
  - openai:  Extract files -> upload to AxleService -> run conversation -> download reports

Usage:
    # Gemini evaluation -- all files
    python flow_evaluation.py --pr 18 --repo owner/repo --review-approach axle --provider gemini

    # Gemini evaluation -- one file only
    python flow_evaluation.py --pr 18 --repo owner/repo --review-approach llm --provider gemini --file schema.py

    # Gemini evaluation -- specific model + review-id
    python flow_evaluation.py --pr 18 --repo owner/repo --review-approach axle --provider gemini --review-id 123456 --model gemini-2.5-flash

    # DB extraction only
    python flow_evaluation.py --pr 123 --repo owner/repo --mode extract_only

    # Axle review via AxleService + OpenAI
    python flow_evaluation.py --pr 123 --repo owner/repo --review-approach axle --provider openai

    # LLM review via AxleService + Claude
    python flow_evaluation.py --pr 123 --repo owner/repo --review-approach llm --provider claude
"""

import sys
import json
import shutil
import asyncio
import argparse
import datetime
from pathlib import Path
from dotenv import load_dotenv

from services.axle import AxleService
from db import get_extractor
from providers.provider_factory import create_provider
from utils.parser import extract_file_ids_from_response
from utils.prompt_builder import build_summary_prompt

load_dotenv()

PROJECT_ROOT = Path(__file__).parent
TMP_POC_DIR = PROJECT_ROOT / "tmp" / "poc"
TMP_POC_DIR.mkdir(parents=True, exist_ok=True)

# Default input folders sit at the PROJECT ROOT inside the source/ directory.
# Manually place the required template files in these folders once.
# Future: these can be replaced by DB-sourced folders via --input-dir.
DEFAULT_AXLE_INPUT = PROJECT_ROOT / "source" / "axle_approach_input"
DEFAULT_LLM_INPUT = PROJECT_ROOT / "source" / "llm_approach_input"


# =============================================================================
# PipelineOrchestrator -- ported from commented-out code in main.py
# Uses an injected output_dir (PR's metrics/ folder) instead of auto-generating.
# =============================================================================

class PipelineOrchestrator:
    """LLM-based review orchestrator using the provider factory pattern."""

    def __init__(self, provider_id: str, output_dir: str):
        self.provider_id = provider_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.provider = create_provider(provider_id)

        self.uploaded_files = {}
        self.uploaded_filepath = self.output_dir / "uploaded_files.json"
        self._load_existing_files()

        print(f"\n{'='*70}")
        print(f"Pipeline Orchestrator Initialized")
        print(f"{'='*70}")
        print(f"Provider  : {self.provider_id.upper()}")
        print(f"Output Dir: {self.output_dir}")
        print(f"{'='*70}\n")

    def _load_existing_files(self):
        """Load existing uploaded files (allows resuming an interrupted run)."""
        if self.uploaded_filepath.exists():
            with open(self.uploaded_filepath, "r") as f:
                self.uploaded_files = json.load(f)

    def upload_files(self, file_paths: list) -> bool:
        """Upload files to the LLM provider."""
        print("\n" + "=" * 70)
        print(f"UPLOADING FILES TO {self.provider_id.upper()}")
        print("=" * 70 + "\n")

        try:
            if not self.uploaded_files:
                results = self.provider.upload_multiple_files(file_paths)
                successful = [r for r in results if r["success"]]
                failed = [r for r in results if not r["success"]]
                all_uploaded = len(failed) == 0

                print(f"\n{'=' * 60}")
                print("UPLOAD SUMMARY")
                print(f"{'=' * 60}")
                print(f"\nSuccessful: {len(successful)}/{len(results)}")

                if successful:
                    print("\nUploaded Files:")
                    for result in successful:
                        print(f"  [OK] {Path(result['file_path']).name}")
                        print(f"    ID: {result['file_id']}")

                if failed:
                    print(f"\nFailed: {len(failed)}")
                    for result in failed:
                        print(f"  [X] {Path(result['file_path']).name}")
                        print(f"    Error: {result['error']}")

                self.uploaded_files.update(self.provider.get_uploaded_files_info())
                with open(self.uploaded_filepath, "w") as f:
                    json.dump(self.uploaded_files, f, indent=2)
                print(f"\n[*] File IDs saved to: {self.uploaded_filepath}")
            else:
                print("\n[*] Files already uploaded -- skipping upload step")
                all_uploaded = True

            return all_uploaded

        except Exception as e:
            print(f"[X] Upload failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def execute_task(self, prompt_path: str, reports_dir: Path) -> bool:
        """
        Run the LLM task and download generated reports.

        Args:
            prompt_path: Path to the code execution prompt file.
            reports_dir: Directory where downloaded artifacts (reports) are saved.
        """
        print("\n" + "=" * 70)
        print(f"TASK EXECUTION ({self.provider_id.upper()})")
        print("=" * 70 + "\n")

        try:
            file_ids = [info["file_id"] for info in self.uploaded_files.values()] if self.uploaded_files else []
            conversation_id = int(datetime.datetime.now().timestamp() * 1000)

            if self.provider_id == "openai":
                self.provider.create_conversation(conversation_id, file_ids)
            else:
                self.provider.create_conversation(conversation_id)

            result = self.provider.start_conversation(file_ids, prompt_path)
            result["conversation_id"] = conversation_id

            if result["success"]:
                downloaded = self._extract_and_download_artifacts(result, reports_dir)
                print(f"\n[OK] Downloaded {len(downloaded)} artifact(s) to {reports_dir}")

            # Save execution log and conversation log to metrics/
            exec_log_path = self.output_dir / "execution_log.json"
            with open(exec_log_path, "w") as f:
                json.dump(result, f, indent=2)
            print(f"[*] Execution log saved: {exec_log_path}")

            self.provider.save_conversation_log(str(self.output_dir / "conversation_log.json"))

            return result["success"]

        except Exception as e:
            print(f"[X] Execution failed: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            if hasattr(self.provider, 'close_logging'):
                self.provider.close_logging()

    def _extract_and_download_artifacts(self, result, reports_dir: Path) -> dict:
        """Download generated artifacts from the provider into reports_dir."""
        reports_dir.mkdir(parents=True, exist_ok=True)

        if self.provider_id == "claude":
            file_ids = extract_file_ids_from_response(result)
            if not file_ids:
                print("[WARN]  No file_ids found in response -- no artifacts to download")
                return {}
            return self.provider.download_multiple_artifacts(file_ids, reports_dir)

        elif self.provider_id == "openai":
            container_id = result.get("container_id")
            if not container_id:
                print("[WARN]  No container_id in response -- no artifacts to download")
                return {}
            return self.provider.download_all_container_files(container_id, reports_dir)

        return {}

    def run(self, file_paths: list, prompt_path: str, reports_dir: Path) -> int:
        """
        Full pipeline: upload -> run task -> download reports.

        Returns:
            0 on success, 1 on failure.
        """
        if not self.upload_files(file_paths):
            print("\n[WARN] File upload failed. Cannot proceed.")
            return 1

        if not self.execute_task(prompt_path, reports_dir):
            print("\n[WARN] Task execution failed.")
            return 1

        print("\n[OK] LLM pipeline completed successfully!")
        return 0


# =============================================================================
# DB Extraction
# =============================================================================

def extract_pr_data(args) -> dict:
    """
    Step 1: Fetch PR data from DB.

    Calls ReviewDataExtractor.export_specific_pr() which creates:
      tmp/poc/<PR_DIR>/
        uploaded_to_eval_agent/
          files.zip         <- changed file content
          log-files.zip     <- prompts, metrics, responses
          project_context.md  <- if found in DB
        reports_generated/  <- to be filled by review step
        metrics/            <- to be filled by review step

    Returns the export result dict.
    """
    print("\n" + "=" * 70)
    print(f"DB EXTRACTION -- PR: {args.pr}  REPO: {args.repo}")
    print("=" * 70)

    extractor = get_extractor()
    return extractor.export_specific_pr(
        repository=args.repo,
        pr_number=args.pr,
        output_dir=PROJECT_ROOT,
        cleanup_folder=True,
        review_id=getattr(args, "review_id", None)
    )


# =============================================================================
# Template Copy
# =============================================================================

def copy_templates(pr_dir: Path, review_approach: str, input_dir: Path) -> None:
    """
    Step 2: Copy template files from input_dir into the PR folder root.

    Templates land at tmp/poc/<PR_DIR>/ (NOT inside uploaded_to_eval_agent/).
    DB exports (zips, project_context.md) remain inside uploaded_to_eval_agent/.

    Axle: copies code_execution_prompt.txt, api_comprehensive_audit_prompt.yaml,
          output_format.md, review_guidelines.md
    LLM:  copies code_execution_prompt.txt, api_comprehensive_audit_prompt.yaml

    Args:
        pr_dir:          Path to the PR folder (tmp/poc/<PR_DIR>/).
        review_approach: 'axle' or 'llm'.
        input_dir:       Source folder (axle_input/ or llm_input/ at project root).
    """
    print(f"\n{'='*70}")
    print(f"COPYING TEMPLATE FILES ({review_approach.upper()} MODE)")
    print(f"{'='*70}")
    print(f"  From: {input_dir}")
    print(f"  To  : {pr_dir}\n")

    if not input_dir.exists():
        raise FileNotFoundError(
            f"Input folder not found: {input_dir}\n"
            f"Please manually place the required template files at the project root."
        )

    copied = 0
    for src in sorted(input_dir.iterdir()):
        if src.is_file():
            dest = pr_dir / src.name
            shutil.copy2(src, dest)
            print(f"  [OK] Copied: {src.name}")
            copied += 1

    if copied == 0:
        print(f"  [WARN]  No files found in {input_dir}")
    else:
        print(f"\n  {copied} template file(s) copied to PR folder")


# =============================================================================
# File Path Builder
# =============================================================================

def build_file_paths(pr_dir: Path, uploaded_dir: Path, review_approach: str) -> list:
    """
    Step 3: Collect all files to upload to the review API.

    Combines:
    - Template files from pr_dir/ (copied from axle_input/ or llm_input/)
    - DB exports from uploaded_to_eval_agent/ (zips + project_context.md)

    Args:
        pr_dir:          Path to the PR folder root.
        uploaded_dir:    Path to uploaded_to_eval_agent/.
        review_approach: 'axle' or 'llm' (for display).

    Returns:
        Ordered list of absolute file path strings.
    """
    file_paths = []

    # 1. Template files from PR folder root
    print(f"\nFiles to upload ({review_approach.upper()}):")
    for f in sorted(pr_dir.iterdir()):
        if f.is_file():  # only top-level files, not subfolders
            file_paths.append(str(f))
            print(f"  [OK] template: {f.name}")

    # 2. DB exports from uploaded_to_eval_agent/
    if not uploaded_dir.exists():
        raise FileNotFoundError(f"uploaded_to_eval_agent/ not found: {uploaded_dir}")
    for f in sorted(uploaded_dir.iterdir()):
        if f.is_file():
            file_paths.append(str(f))
            print(f"  [OK] db export: {f.name}")

    print(f"\nTotal files: {len(file_paths)}")
    return file_paths


# =============================================================================
# Review Modes
# =============================================================================

async def run_llm_mode(args, pr_result: dict, input_dir: Path) -> int:
    """
    LLM Review:
      - Copies templates from llm_input/ into the PR folder
      - Uploads all files (templates + DB exports) to AxleService
      - Runs AxleService (same evaluation engine as axle mode)
      - Saves reports -> reports_generated/, logs -> metrics/

    Only the INPUT files differ from axle mode (source/llm_approach_input/
    vs source/axle_approach_input/). The evaluation engine is always AxleService.
    """
    pr_dir = Path(pr_result["pr_dir"])
    uploaded_dir = pr_dir / "uploaded_to_eval_agent"

    print(f"\n{'='*70}")
    print("LLM REVIEW MODE (via AxleService)")
    print(f"{'='*70}")
    print(f"PR Dir   : {pr_dir}")

    # Step 2: Copy templates into PR folder root
    copy_templates(pr_dir, "llm", input_dir)

    # Step 3: Build file list (templates from pr_dir + db exports from uploaded_dir)
    file_paths = build_file_paths(pr_dir, uploaded_dir, "llm")

    # prompt lives at PR folder root after copy
    prompt_path = str(pr_dir / "code_execution_prompt.txt")
    if not Path(prompt_path).exists():
        print(f"[X] Prompt file not found: {prompt_path}")
        return 1

    axle_service = AxleService(
        project_root=PROJECT_ROOT,
        pr_dir=str(pr_dir)
    )
    try:
        result = await axle_service.execute_task(
            provider=args.provider,
            file_paths=file_paths,
            prompt_path=prompt_path
        )

        if result["success"]:
            print("\n[OK] LLM review (via AxleService) completed successfully!")
            print(f"  Reports: {result.get('artifacts_dir', pr_dir / 'reports_generated')}")
            print(f"  Metrics: {result.get('metrics_dir', pr_dir / 'metrics')}")
        else:
            print("\n[X] LLM review failed.")

        return 0 if result["success"] else 1

    finally:
        await axle_service.cleanup()


async def run_axle_mode(args, pr_result: dict, input_dir: Path) -> int:
    """
    Axle Review:
      - Copies templates from axle_input/ into uploaded_to_eval_agent/
      - project_context.md already there from DB extraction
      - Uploads all files from uploaded_to_eval_agent/
      - Runs AxleService
      - Saves reports -> reports_generated/, logs -> metrics/
    """
    pr_dir = Path(pr_result["pr_dir"])
    uploaded_dir = pr_dir / "uploaded_to_eval_agent"

    print(f"\n{'='*70}")
    print("AXLE REVIEW MODE")
    print(f"{'='*70}")
    print(f"PR Dir   : {pr_dir}")

    # Step 2: Copy templates into PR folder root
    copy_templates(pr_dir, "axle", input_dir)

    # Step 3: Build file list (templates from pr_dir + db exports from uploaded_dir)
    file_paths = build_file_paths(pr_dir, uploaded_dir, "axle")

    # prompt lives at PR folder root after copy
    prompt_path = str(pr_dir / "code_execution_prompt.txt")
    if not Path(prompt_path).exists():
        print(f"[X] Prompt file not found: {prompt_path}")
        return 1

    axle_service = AxleService(
        project_root=PROJECT_ROOT,
        pr_dir=str(pr_dir)
    )
    try:
        result = await axle_service.execute_task(
            provider=args.provider,
            file_paths=file_paths,
            prompt_path=prompt_path
        )

        if result["success"]:
            print("\n[OK] Axle review completed successfully!")
            print(f"  Reports: {result.get('artifacts_dir', pr_dir / 'reports_generated')}")
            print(f"  Metrics: {result.get('metrics_dir', pr_dir / 'metrics')}")
        else:
            print("\n[X] Axle review failed.")

        return 0 if result["success"] else 1

    finally:
        await axle_service.cleanup()


# =============================================================================
# Main Entry Point
# =============================================================================

async def run_gemini_mode(args, pr_result) -> int:
    """
    Gemini provider flow: query DB -> build prompt per file -> send to Gemini -> save reports.
    No file upload needed -- builds a self-contained text prompt from DB data.
    """
    from utils.prompt_builder import build_prompt
    from services.gemini import GeminiAdapter
    import datetime as _dt

    review_approach = args.review_approach
    input_dir = Path(args.input_dir) if args.input_dir else None  # prompt_builder resolves based on approach

    print(f"\nProvider        : GEMINI (LLM routing service)")
    print(f"Review Approach : {review_approach.upper()}")
    print(f"Input Dir       : {input_dir or '(auto based on review approach)'}")

    # Query DB for file records
    extractor = get_extractor()
    records = extractor.get_latest_pr_reviews(
        repository=args.repo, pr_number=str(args.pr)
    )
    records = [r for r in records if r.get("head_content")]
    if not records:
        print(f"\n[FAIL] No file records with content found for PR #{args.pr}")
        return 1

    print(f"\nFound {len(records)} file(s):")
    for r in records:
        print(f"  - {Path(r['file']).name}  [{r.get('status', '?')}]")

    # Filter files (optional -- default: all files)
    if args.file:
        records = [r for r in records if args.file.lower() in Path(r['file']).name.lower()]
        if not records:
            print(f"\n[FAIL] No file matching '{args.file}'")
            return 1

    # Fetch project context from DB
    project_context = None
    try:
        ctx = extractor.get_context_from_review_metrics(
            repository=args.repo, pr_number=str(args.pr), review_id=args.review_id
        )
        if ctx:
            project_context = ctx
        else:
            ctx = extractor.get_repo_context(
                repository=args.repo, pr_number=str(args.pr)
            )
            if ctx:
                project_context = ctx
        if project_context:
            print(f"Project context : loaded ({len(project_context):,} chars)")
    except Exception as e:
        print(f"Warning: Could not fetch project context: {e}")

    dry_run = getattr(args, 'dry_run', False)
    pr_dir = Path(pr_result["pr_dir"])
    prompt_dir = pr_dir / "uploaded_to_eval_agent" / "file_prompt"
    prompt_dir.mkdir(parents=True, exist_ok=True)

    if dry_run:
        print(f"\n[DRY RUN] Building prompts for {len(records)} file(s) -- skipping Gemini API call.")
    else:
        print(f"\nEvaluating {len(records)} file(s) sequentially...")

    reports_dir = pr_dir / "reports_generated"
    metrics_dir = pr_dir / "metrics"
    reports_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    adapter = None if dry_run else GeminiAdapter(model=args.model)
    all_metrics = []

    for i, record in enumerate(records, 1):
        fname = Path(record["file"]).name
        base = Path(record["file"]).stem
        print(f"\n{'-'*60}")
        print(f"  [{i}/{len(records)}] {'Building prompt' if dry_run else 'Evaluating'}: {fname}")
        print(f"{'-'*60}")

        prompt_text = build_prompt(
            record,
            review_approach=review_approach,
            input_dir=input_dir,
            project_context=project_context,
        )
        prompt_path = prompt_dir / f"{fname}.txt"
        with open(prompt_path, "w", encoding="utf-8") as f:
            f.write(prompt_text)
        print(f"  Prompt saved ({len(prompt_text):,} chars): {prompt_path.name}")

        if dry_run:
            continue

        print(f"  [2/3] Sending to Gemini ({adapter.model})...")
        result = await adapter.evaluate(prompt_text)
        print(f"         Duration: {result['duration_seconds']}s | Tokens: {result['usage']}")

        with open(reports_dir / f"{base}_eval_report.md", "w", encoding="utf-8") as f:
            f.write(result["response_text"])
        metrics = {
            "file": record["file"], "model": result["model"],
            "provider": "gemini",
            "review_approach": review_approach,
            "duration_seconds": result["duration_seconds"],
            "usage": result["usage"],
            "timestamp": _dt.datetime.now().isoformat(),
        }
        with open(metrics_dir / f"{base}_gemini_metrics.json", "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        print(f"  [3/3] Saved: {base}_eval_report.md")
        all_metrics.append(metrics)

    # Summary
    print(f"\n{'='*60}")
    print("DRY RUN COMPLETE" if dry_run else "EVALUATION COMPLETE")
    print(f"{'='*60}")
    print(f"Files processed  : {len(records)}")
    print(f"Review approach  : {review_approach}")
    print(f"Prompts          : {prompt_dir}")
    if not dry_run:
        print(f"Provider         : gemini ({adapter.model})")
        print(f"Reports          : {reports_dir}")
        print(f"Metrics          : {metrics_dir}")
        total_duration = sum(m.get("duration_seconds", 0) for m in all_metrics)
        print(f"Total duration   : {total_duration:.1f}s")
        errors = [m for m in all_metrics if m.get("error")]
        if errors:
            print(f"Errors           : {len(errors)}")

    # --- Generate Summary Report (or dry-run prompt) ---
    existing_reports = list(reports_dir.glob("*_eval_report.md"))
    if len(existing_reports) >= 2:
        print(f"\n{'='*60}")
        print("BUILDING SUMMARY PROMPT" if dry_run else "GENERATING SUMMARY REPORT")
        print(f"{'='*60}")
        print(f"Found {len(existing_reports)} eval reports to summarize")

        try:
            summary_prompt_text = build_summary_prompt(
                reports_dir=reports_dir,
                review_approach=review_approach,
                input_dir=input_dir,
            )
            summary_prompt_path = prompt_dir / "summary_report_prompt.txt"
            with open(summary_prompt_path, "w", encoding="utf-8") as f:
                f.write(summary_prompt_text)

            # Rough token estimate: ~4 chars per token for English text
            est_tokens = len(summary_prompt_text) // 4
            print(f"  Summary prompt saved ({len(summary_prompt_text):,} chars, ~{est_tokens:,} est. tokens): {summary_prompt_path.name}")

            if dry_run:
                print(f"  [DRY RUN] Skipping Gemini API call for summary report.")
            else:
                print(f"  Sending to Gemini ({adapter.model})...")
                summary_result = await adapter.evaluate(summary_prompt_text)
                print(f"  Duration: {summary_result['duration_seconds']}s | Tokens: {summary_result['usage']}")

                summary_filename = "LLM_EVAL_META_ANALYSIS.md" if review_approach == "llm" else "AXLE_EVAL_META_ANALYSIS.md"
                summary_report_path = reports_dir / summary_filename
                with open(summary_report_path, "w", encoding="utf-8") as f:
                    f.write(summary_result["response_text"])

                summary_metrics = {
                    "type": "summary_report",
                    "model": summary_result["model"],
                    "provider": "gemini",
                    "review_approach": review_approach,
                    "duration_seconds": summary_result["duration_seconds"],
                    "usage": summary_result["usage"],
                    "files_summarized": len(existing_reports),
                    "timestamp": _dt.datetime.now().isoformat(),
                }
                with open(metrics_dir / "summary_gemini_metrics.json", "w", encoding="utf-8") as f:
                    json.dump(summary_metrics, f, indent=2)

                print(f"  [OK] Summary report saved: {summary_report_path.name}")
        except Exception as e:
            print(f"  [WARN] Summary report generation failed: {e}")
            import traceback
            traceback.print_exc()
    elif len(existing_reports) == 1:
        print(f"\n[SKIP] Only 1 eval report — summary report requires 2+ files.")
    elif dry_run:
        print(f"\n[DRY RUN] No existing eval reports in {reports_dir} — summary prompt cannot be built.")

    if not dry_run:
        return 0 if not errors else 1
    return 0


async def main():
    """Main async entry point."""
    parser = argparse.ArgumentParser(
        description="Unified PR Evaluation Flow -- single CLI for all providers"
    )
    parser.add_argument("--pr", required=True, help="PR number to evaluate")
    parser.add_argument("--repo", required=True, help="Repository (owner/repo)")
    parser.add_argument(
        "--mode",
        choices=["extract_only"],
        default=None,
        help="extract_only: DB extraction only, no evaluation"
    )
    parser.add_argument(
        "--review-approach",
        choices=["llm", "axle"],
        default=None,
        help="Review approach: 'axle' (includes review_guidelines + output_format) or 'llm' (guidelines embedded in prompt). Required unless --mode extract_only."
    )
    parser.add_argument(
        "--provider",
        choices=["gemini", "claude", "openai"],
        default="gemini",
        help="Evaluation provider: gemini (direct prompt), claude/openai (AxleService). Default: gemini"
    )
    parser.add_argument(
        "--review-id",
        default=None,
        dest="review_id",
        help="Review ID used for fetching repo context from eval metrics. Required unless --mode extract_only."
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Override input template directory. Auto-resolved from review-approach if not set."
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Evaluate only this file (partial match). Omit to evaluate all files. Used with --provider gemini."
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Override model name (e.g., gemini-2.5-flash). Used with --provider gemini."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Build and save prompts only, skip sending to LLM. Useful for testing prompt generation."
    )

    args = parser.parse_args()

    # Validate: --review-approach and --review-id are required unless --mode extract_only
    if args.mode != "extract_only" and not args.review_approach:
        parser.error("--review-approach is required unless --mode extract_only is set.")
    if args.mode != "extract_only" and not args.review_id:
        parser.error("--review-id is required unless --mode extract_only is set.")

    # -------------------------------------------------------------------------
    # Step 1: DB Extraction
    # -------------------------------------------------------------------------
    export = extract_pr_data(args)
    if not export.get("success"):
        print("\n[FAIL] DB Extraction failed. Aborting.")
        return 1

    pr_result = export["pr_results"][0]
    print(f"\n[OK] Extraction complete: {pr_result['pr_dir']}")

    if args.mode == "extract_only":
        print("Mode: extract_only -- skipping review step.")
        return 0

    # -------------------------------------------------------------------------
    # Route to provider
    # -------------------------------------------------------------------------
    if args.provider == "gemini":
        return await run_gemini_mode(args, pr_result)

    # -------------------------------------------------------------------------
    # AxleService flow (claude / openai)
    # -------------------------------------------------------------------------
    if args.input_dir:
        input_dir = Path(args.input_dir)
    else:
        input_dir = DEFAULT_AXLE_INPUT if args.review_approach == "axle" else DEFAULT_LLM_INPUT

    print(f"\nReview Approach : {args.review_approach.upper()}")
    print(f"Provider        : {args.provider.upper()}")
    print(f"Input Dir       : {input_dir}")

    if args.review_approach == "llm":
        return await run_llm_mode(args, pr_result, input_dir)
    else:
        return await run_axle_mode(args, pr_result, input_dir)


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

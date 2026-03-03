"""
Unified PR Evaluation Flow
File: flow_evaluation.py

Single entry-point that:
  1. Extracts PR data from DB (files, logs, repo context) into tmp/poc/<PR_DIR>/
  2. Copies mode-specific template files from axle_input/ or llm_input/ (project root)
     into the PR's uploaded_to_eval_agent/ folder — making each run self-contained.
  3. Routes to either the LLM PipelineOrchestrator or the AxleService review engine.

Usage:
    # DB extraction only
    python flow_evaluation.py --pr 123 --repo owner/repo --mode extract_only

    # Axle review
    python flow_evaluation.py --pr 123 --repo owner/repo --review-approach axle

    # LLM review
    python flow_evaluation.py --pr 123 --repo owner/repo --review-approach llm --provider claude

    # Custom input folder (future: DB-sourced)
    python flow_evaluation.py --pr 123 --repo owner/repo --review-approach axle --input-dir path/to/dir/
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
# PipelineOrchestrator — ported from commented-out code in main.py
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
                        print(f"  ✓ {Path(result['file_path']).name}")
                        print(f"    ID: {result['file_id']}")

                if failed:
                    print(f"\nFailed: {len(failed)}")
                    for result in failed:
                        print(f"  ✗ {Path(result['file_path']).name}")
                        print(f"    Error: {result['error']}")

                self.uploaded_files.update(self.provider.get_uploaded_files_info())
                with open(self.uploaded_filepath, "w") as f:
                    json.dump(self.uploaded_files, f, indent=2)
                print(f"\n📝 File IDs saved to: {self.uploaded_filepath}")
            else:
                print("\n📝 Files already uploaded — skipping upload step")
                all_uploaded = True

            return all_uploaded

        except Exception as e:
            print(f"✗ Upload failed: {e}")
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
                print(f"\n✓ Downloaded {len(downloaded)} artifact(s) to {reports_dir}")

            # Save execution log and conversation log to metrics/
            exec_log_path = self.output_dir / "execution_log.json"
            with open(exec_log_path, "w") as f:
                json.dump(result, f, indent=2)
            print(f"📝 Execution log saved: {exec_log_path}")

            self.provider.save_conversation_log(str(self.output_dir / "conversation_log.json"))

            return result["success"]

        except Exception as e:
            print(f"✗ Execution failed: {e}")
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
                print("⚠️  No file_ids found in response — no artifacts to download")
                return {}
            return self.provider.download_multiple_artifacts(file_ids, reports_dir)

        elif self.provider_id == "openai":
            container_id = result.get("container_id")
            if not container_id:
                print("⚠️  No container_id in response — no artifacts to download")
                return {}
            return self.provider.download_all_container_files(container_id, reports_dir)

        return {}

    def run(self, file_paths: list, prompt_path: str, reports_dir: Path) -> int:
        """
        Full pipeline: upload → run task → download reports.

        Returns:
            0 on success, 1 on failure.
        """
        if not self.upload_files(file_paths):
            print("\n⚠ File upload failed. Cannot proceed.")
            return 1

        if not self.execute_task(prompt_path, reports_dir):
            print("\n⚠ Task execution failed.")
            return 1

        print("\n✓ LLM pipeline completed successfully!")
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
          files.zip         ← changed file content
          log-files.zip     ← prompts, metrics, responses
          repo_context.txt  ← if found in DB
        reports_generated/  ← to be filled by review step
        metrics/            ← to be filled by review step

    Returns the export result dict.
    """
    print("\n" + "=" * 70)
    print(f"DB EXTRACTION — PR: {args.pr}  REPO: {args.repo}")
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
    DB exports (zips, repo_context) remain inside uploaded_to_eval_agent/.

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
            print(f"  ✓ Copied: {src.name}")
            copied += 1

    if copied == 0:
        print(f"  ⚠️  No files found in {input_dir}")
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
    - DB exports from uploaded_to_eval_agent/ (zips + repo_context.txt)

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
            print(f"  ✓ template: {f.name}")

    # 2. DB exports from uploaded_to_eval_agent/
    if not uploaded_dir.exists():
        raise FileNotFoundError(f"uploaded_to_eval_agent/ not found: {uploaded_dir}")
    for f in sorted(uploaded_dir.iterdir()):
        if f.is_file():
            file_paths.append(str(f))
            print(f"  ✓ db export: {f.name}")

    print(f"\nTotal files: {len(file_paths)}")
    return file_paths


# =============================================================================
# Review Modes
# =============================================================================

async def run_llm_mode(args, pr_result: dict, input_dir: Path) -> int:
    """
    LLM Review:
      - Copies templates from llm_input/ into uploaded_to_eval_agent/
      - Uploads all files from uploaded_to_eval_agent/
      - Runs PipelineOrchestrator
      - Saves reports → reports_generated/, logs → metrics/
    """
    pr_dir = Path(pr_result["pr_dir"])
    uploaded_dir = pr_dir / "uploaded_to_eval_agent"
    reports_dir = pr_dir / "reports_generated"
    metrics_dir = pr_dir / "metrics"
    reports_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print("LLM REVIEW MODE")
    print(f"{'='*70}")
    print(f"PR Dir      : {pr_dir}")
    print(f"Reports Dir : {reports_dir}")
    print(f"Metrics Dir : {metrics_dir}")

    # Step 2: Copy templates into PR folder root
    copy_templates(pr_dir, "llm", input_dir)

    # Step 3: Build file list (templates from pr_dir + db exports from uploaded_dir)
    file_paths = build_file_paths(pr_dir, uploaded_dir, "llm")

    # prompt lives at PR folder root after copy
    prompt_path = str(pr_dir / "code_execution_prompt.txt")
    if not Path(prompt_path).exists():
        print(f"✗ Prompt file not found: {prompt_path}")
        return 1

    orchestrator = PipelineOrchestrator(
        provider_id=args.provider,
        output_dir=str(metrics_dir)
    )
    return orchestrator.run(file_paths, prompt_path, reports_dir=reports_dir)


async def run_axle_mode(args, pr_result: dict, input_dir: Path) -> int:
    """
    Axle Review:
      - Copies templates from axle_input/ into uploaded_to_eval_agent/
      - repo_context.txt already there from DB extraction
      - Uploads all files from uploaded_to_eval_agent/
      - Runs AxleService
      - Saves reports → reports_generated/, logs → metrics/
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
        print(f"✗ Prompt file not found: {prompt_path}")
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
            print("\n✓ Axle review completed successfully!")
            print(f"  Reports: {result.get('artifacts_dir', pr_dir / 'reports_generated')}")
            print(f"  Metrics: {result.get('metrics_dir', pr_dir / 'metrics')}")
        else:
            print("\n✗ Axle review failed.")

        return 0 if result["success"] else 1

    finally:
        await axle_service.cleanup()


# =============================================================================
# Main Entry Point
# =============================================================================

async def main():
    """Main async entry point."""
    parser = argparse.ArgumentParser(
        description="Unified PR Evaluation Flow — DB extraction + LLM or Axle review"
    )
    parser.add_argument("--pr", required=True, help="PR number to evaluate")
    parser.add_argument("--repo", required=True, help="Repository (owner/repo)")
    parser.add_argument(
        "--mode",
        choices=["extract_only"],
        default=None,
        help="extract_only: DB extraction only, no review"
    )
    parser.add_argument(
        "--review-approach",
        choices=["llm", "axle"],
        default=None,
        help="Review engine: 'axle' or 'llm'. Required unless --mode extract_only."
    )
    parser.add_argument(
        "--provider",
        choices=["claude", "openai"],
        default="openai",
        help="LLM provider (default: openai)"
    )
    parser.add_argument(
        "--review-id",
        default=None,
        dest="review_id",
        help="Optional review ID used for fetching repo context from eval metrics"
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help=(
            "Override input files folder. "
            "Defaults to source/axle_approach_input/ or source/llm_approach_input/ at project root. "
            "Use this to point to a future DB-sourced folder."
        )
    )

    args = parser.parse_args()

    # Validate: --review-approach is required unless --mode extract_only
    if args.mode != "extract_only" and not args.review_approach:
        parser.error("--review-approach is required unless --mode extract_only is set.")

    # -------------------------------------------------------------------------
    # Step 1: DB Extraction
    # -------------------------------------------------------------------------
    export = extract_pr_data(args)
    if not export.get("success"):
        print("\n❌ DB Extraction failed. Aborting.")
        return 1

    pr_result = export["pr_results"][0]
    print(f"\n✅ Extraction complete: {pr_result['pr_dir']}")

    if args.mode == "extract_only":
        print("Mode: extract_only — skipping review step.")
        return 0

    # -------------------------------------------------------------------------
    # Resolve input directory
    # -------------------------------------------------------------------------
    if args.input_dir:
        input_dir = Path(args.input_dir)
    else:
        input_dir = DEFAULT_AXLE_INPUT if args.review_approach == "axle" else DEFAULT_LLM_INPUT

    print(f"\nReview Approach : {args.review_approach.upper()}")
    print(f"Provider        : {args.provider.upper()}")
    print(f"Input Dir       : {input_dir}")

    # -------------------------------------------------------------------------
    # Step 2–5: Copy templates, upload, run review, save outputs
    # -------------------------------------------------------------------------
    if args.review_approach == "llm":
        return await run_llm_mode(args, pr_result, input_dir)
    else:
        return await run_axle_mode(args, pr_result, input_dir)


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

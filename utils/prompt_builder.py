"""
Prompt Builder — builds self-contained evaluation prompts
directly from DB records (no zip extraction needed).

build_prompt():
  Produces a single-file evaluation prompt:
    1. Base code_execution_prompt.txt (already single-file ready)
    2. Separator
    3. Audit guidelines YAML
    4. Review guidelines (axle only — from source/axle_approach_input/)
    5. Output format (axle only — from source/axle_approach_input/)
    6. Project context
    7. Target source file content (head_content)
    8. Previous LLM review logs (prompt, response, metrics)

build_summary_prompt():
  Produces a summary/meta-analysis prompt:
    1. Summary report prompt template
    2. All per-file evaluation reports appended

review_approach controls which templates are included:
  - "axle": code_execution_prompt + audit YAML + review_guidelines + output_format
  - "llm":  code_execution_prompt + audit YAML (guidelines already embedded in prompt)
"""

import json
import datetime
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_AXLE_INPUT = PROJECT_ROOT / "source" / "axle_approach_input"


def _read_file_safe(filepath: Path) -> str:
    if not filepath.exists():
        return ""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        print(f"Warning: Error reading {filepath}: {e}")
        return ""


def _format_json(data) -> str:
    """Format a JSON field (dict or string) as pretty-printed JSON text."""
    if data is None:
        return ""
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return data
    return json.dumps(data, indent=2, ensure_ascii=False)


def build_prompt(
    record: dict,
    review_approach: str = "axle",
    input_dir: Optional[Path] = None,
    project_context: Optional[str] = None,
) -> str:
    """
    Build a self-contained evaluation prompt from a single DB record.

    Args:
        record: A dict from ReviewDataExtractor.get_reviews() containing:
            - file: file path
            - head_content: source file content
            - prompt: LLM prompt log
            - review_result: LLM response (JSON)
            - llm_service_metrics: metrics (JSON)
        review_approach: "axle" or "llm". Controls which templates are included:
            - axle: includes review_guidelines.md + output_format.md
            - llm: skips them (already embedded in code_execution_prompt.txt)
        input_dir: Path to template directory. Defaults based on review_approach:
            - axle → source/axle_approach_input/
            - llm  → source/llm_approach_input/
        project_context: Optional project context string from DB.

    Returns:
        The complete prompt string ready to send to Gemini.
    """
    if input_dir is None:
        if review_approach == "llm":
            input_dir = PROJECT_ROOT / "source" / "llm_approach_input"
        else:
            input_dir = DEFAULT_AXLE_INPUT

    # --- Load template files from source ---
    base_prompt = _read_file_safe(input_dir / "code_execution_prompt.txt")
    if base_prompt:
        # Inject actual date so the LLM doesn't hallucinate it
        base_prompt = base_prompt.replace("[today's date]", datetime.date.today().isoformat())
    else:
        base_prompt = "[Base code execution prompt missing]"

    audit_yaml = _read_file_safe(input_dir / "api_comprehensive_audit_prompt.yaml")

    # Review guidelines and output format are only separate files in axle approach.
    # In llm approach, they are already embedded inside code_execution_prompt.txt.
    if review_approach == "axle":
        review_guidelines = _read_file_safe(DEFAULT_AXLE_INPUT / "review_guidelines.md")
        output_format = _read_file_safe(DEFAULT_AXLE_INPUT / "output_format.md")
    else:
        review_guidelines = ""
        output_format = ""

    # --- Extract data from DB record ---
    file_path = record.get("file", "unknown")
    filename = Path(file_path).name
    head_content = record.get("head_content") or ""
    prompt_log = record.get("prompt") or ""
    response_log = _format_json(record.get("review_result"))
    metrics_log = _format_json(record.get("llm_service_metrics"))

    # --- Assemble prompt (same structure as model prompt) ---
    sections = []

    # 1. Base prompt instructions
    sections.append(base_prompt)

    # 2. Separator
    sections.append("\n" + "=" * 50)
    sections.append("BELOW IS THE INJECTED DATA FOR THE EVALUATION")
    sections.append("=" * 50 + "\n")

    # 3. Audit guidelines YAML
    sections.append("=== AUDIT GUIDELINES AND FORMAT Schema (YAML) ===")
    sections.append(audit_yaml or "[Audit prompt template missing]")

    # 4. Review guidelines (axle only)
    if review_guidelines:
        sections.append("\n=== REVIEW GUIDELINES (review_guidelines.md) ===")
        sections.append(review_guidelines)

    # 5. Output format (axle only)
    if output_format:
        sections.append("\n=== OUTPUT FORMAT (output_format.md) ===")
        sections.append(output_format)

    # 6. Project context
    if project_context:
        sections.append("\n=== PROJECT CONTEXT ===")
        sections.append(project_context)

    # 7. Target source file
    sections.append("\n=== FILE IN REVIEW DETAILS ===")
    sections.append(f"Filename: {filename}")
    sections.append("=== FILE CONTENTS ===")
    sections.append("```")
    sections.append(head_content)
    sections.append("```")

    # 8. Previous LLM review logs
    if prompt_log or response_log or metrics_log:
        sections.append("\n=== PREVIOUS LLM REVIEW LOGS (TO EVALUATE) ===")
        if prompt_log:
            sections.append(f"\n-- Prompt Log --\n{prompt_log}")
        if response_log:
            sections.append(f"\n-- Response Log --\n{response_log}")
        if metrics_log:
            sections.append(f"\n-- Metrics Log --\n{metrics_log}")
    else:
        sections.append("\n=== PREVIOUS LLM REVIEW LOGS ===")
        sections.append("(No specific LLM review logs found for this file)")

    return "\n".join(sections)


def build_summary_prompt(
    reports_dir: Path,
    review_approach: str = "axle",
    input_dir: Optional[Path] = None,
) -> str:
    """
    Build a self-contained summary report prompt by reading the summary
    template and appending all per-file evaluation reports from reports_dir.

    Args:
        reports_dir: Path to the directory containing *_eval_report.md files.
        review_approach: "axle" or "llm". Controls which summary template is used.
        input_dir: Path to template directory. Defaults based on review_approach.

    Returns:
        The complete summary prompt string ready to send to the LLM.
    """
    if input_dir is None:
        if review_approach == "llm":
            input_dir = PROJECT_ROOT / "source" / "llm_approach_input"
        else:
            input_dir = DEFAULT_AXLE_INPUT

    # --- Load summary prompt template ---
    summary_template = _read_file_safe(input_dir / "summary_report_prompt.txt")
    if not summary_template:
        raise FileNotFoundError(
            f"summary_report_prompt.txt not found in {input_dir}"
        )

    # Inject actual date
    summary_template = summary_template.replace(
        "[today's date]", datetime.date.today().isoformat()
    )

    # --- Collect all eval reports ---
    report_files = sorted(reports_dir.glob("*_eval_report.md"))
    if not report_files:
        raise FileNotFoundError(
            f"No *_eval_report.md files found in {reports_dir}"
        )

    # --- Assemble prompt ---
    sections = [summary_template]

    sections.append("\n" + "=" * 60)
    sections.append("=== PER-FILE EVALUATION REPORTS ===")
    sections.append("=" * 60 + "\n")

    for i, report_path in enumerate(report_files, 1):
        report_content = _read_file_safe(report_path)
        if report_content:
            sections.append(f"\n{'─' * 60}")
            sections.append(f"### Report {i}/{len(report_files)}: {report_path.name}")
            sections.append(f"{'─' * 60}\n")
            sections.append(report_content)

    sections.append(f"\n{'=' * 60}")
    sections.append("=== END OF PER-FILE EVALUATION REPORTS ===")
    sections.append(f"{'=' * 60}")

    return "\n".join(sections)

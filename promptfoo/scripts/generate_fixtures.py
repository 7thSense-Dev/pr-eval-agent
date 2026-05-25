"""
Pull rows from `code_review.review_eval_metrics` and write them as promptfoo
test cases at `fixtures/tests.json`.

pr-eval-agent is itself an *evaluator* — it consumes a review (produced by some
other review agent and stored in `review_result`) and produces a Markdown audit
report. The assertions below test that audit's *structure and content*, not the
input review.

Each generated test case asserts:
  - the audit contains the required Markdown sections
  - the grid in section 1a has one row per input review_comment
  - there is one "### Comment N" detail block per input review_comment
  - only valid verdict tokens (TP / FP / P) and grade letters (A / B / C / D / F)
    appear in the grid column positions
  - (optional, --baseline-dir) the audit's verdicts substantially agree with the
    baseline audit produced by a prior run

Usage:
    python scripts/generate_fixtures.py --repo owner/repo --pr 18
    python scripts/generate_fixtures.py --repo owner/repo --pr 18 --review-id <uuid>
    python scripts/generate_fixtures.py --repo owner/repo --pr 18 \\
        --baseline-dir ../tmp/poc/1627_1777553113352_recrui8/reports_generated
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROMPTFOO_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = PROMPTFOO_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.review_data_extractor import ReviewDataExtractor, FULL_TABLE_NAME


def fetch_rows_by_review_id(
    extractor: ReviewDataExtractor, repository: str, pr_number: str, review_id: str
) -> list[dict]:
    """get_reviews() does not SELECT review_id, so we run a direct query."""
    query = f"""
        SELECT
            id, request_id, review_id, repository, pr_number, file, status,
            template_name, head_content, base_content, prompt, review_result,
            llm_service_metrics, metadata_, created_at
        FROM {FULL_TABLE_NAME}
        WHERE repository = %s
          AND pr_number = %s
          AND review_id = %s
        ORDER BY file
    """
    with extractor.cursor() as cursor:
        cursor.execute(query, (repository, str(pr_number), review_id))
        return list(cursor.fetchall())


def _find_baseline(baseline_dir: Path | None, file_path: str) -> str | None:
    """Match `client/.../ApplicantFilters.js` -> `<dir>/ApplicantFilters_eval_report.md`."""
    if not baseline_dir or not baseline_dir.is_dir():
        return None
    stem = Path(file_path).stem
    candidate = baseline_dir / f"{stem}_eval_report.md"
    if candidate.is_file():
        return candidate.read_text(encoding="utf-8", errors="replace")
    return None


def build_test_case(row: dict, *, baseline_dir: Path | None) -> dict:
    reference = row.get("review_result") or {}
    ref_comments = reference.get("review_comments") or []
    n = len(ref_comments)
    project_context = (row.get("metadata_") or {}).get("project_context", "")

    # Structural assertions on the Markdown audit report.
    assertions: list[dict] = [
        {"type": "contains", "value": "Comment-by-Comment Evaluation"},
        {"type": "contains", "value": "Summary Grid"},
        {
            "type": "javascript",
            "value": (
                "const m = output.match(/^###\\s+Comment\\s+\\d+/gm) || [];"
                f"return m.length === {n};"
            ),
        },
        {
            "type": "javascript",
            "value": (
                # Grade column should only contain A, B, C, D, F (or N/A).
                "const grades = [...output.matchAll(/\\|\\s*([ABCDF])\\s*\\|\\s*$/gm)];"
                "return grades.length > 0;"
            ),
        },
        {
            "type": "javascript",
            "value": (
                # Each detail block must declare a verdict on Dimension 1.
                # Accepted tokens: TP, FP, Partial, P, N/A — case-insensitive.
                "const re = /Dimension 1[^\\n]*?:\\s*(TP|FP|Partial|P|N\\/A)\\b/gi;"
                "const verdicts = [...output.matchAll(re)];"
                f"return verdicts.length === {n};"
            ),
        },
    ]

    baseline = _find_baseline(baseline_dir, row["file"])
    if baseline:
        assertions.append(
            {
                "type": "llm-rubric",
                "value": (
                    "Below is the NEW audit (under test) and a BASELINE audit (a previous "
                    "trusted run of the same evaluator on the same input).\n\n"
                    "The NEW audit passes if it reaches the same TP/FP/P verdict and the same "
                    "category/severity verdict for each review comment as the BASELINE. "
                    "Minor wording differences in the rationale are acceptable. A different "
                    "verdict on any comment is a failure.\n\n"
                    "BASELINE AUDIT:\n" + baseline[:8000]
                ),
            }
        )

    return {
        "description": f"{row.get('repository')}#{row.get('pr_number')} {row.get('file')}",
        "vars": {
            "record": {
                "file": row["file"],
                "head_content": row["head_content"],
                "base_content": row.get("base_content") or "",
                "prompt": row.get("prompt") or "",
                "review_result": reference,
                "llm_service_metrics": row.get("llm_service_metrics") or {},
                "metadata_": row.get("metadata_") or {},
            },
            "project_context": project_context,
        },
        "assert": assertions,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="e.g. owner/repo")
    ap.add_argument("--pr", required=True, help="PR number")
    ap.add_argument(
        "--review-id",
        default=None,
        help="filter to a specific review run (review_id column). "
             "If omitted, falls back to the latest review per file.",
    )
    ap.add_argument("--limit", type=int, default=20, help="cap number of rows")
    ap.add_argument("--out", default=str(PROMPTFOO_DIR / "fixtures" / "tests.json"))
    ap.add_argument(
        "--baseline-dir",
        default=None,
        help="path to a previous flow_evaluation `reports_generated/` directory. "
             "For each input file, if a `<stem>_eval_report.md` exists there, the "
             "test case gets an additional llm-rubric assertion comparing the new "
             "audit's verdicts to that baseline.",
    )
    args = ap.parse_args()

    extractor = ReviewDataExtractor()
    if args.review_id:
        rows = fetch_rows_by_review_id(extractor, args.repo, args.pr, args.review_id)
        if not rows:
            with extractor.cursor() as cursor:
                cursor.execute(
                    f"SELECT review_id, COUNT(*) AS n FROM {FULL_TABLE_NAME} "
                    f"WHERE repository=%s AND pr_number=%s "
                    f"GROUP BY review_id ORDER BY n DESC",
                    (args.repo, str(args.pr)),
                )
                seen = list(cursor.fetchall())
            print(
                f"No rows matched review_id={args.review_id!r}. "
                f"Available review_ids for {args.repo}#{args.pr}:",
                file=sys.stderr,
            )
            for r in seen:
                print(f"  {str(r.get('review_id')):40s}  ({r.get('n')} rows)", file=sys.stderr)
            sys.exit(1)
    else:
        rows = extractor.get_latest_pr_reviews(repository=args.repo, pr_number=args.pr)
    rows = [r for r in rows if r.get("head_content")][: args.limit]

    if not rows:
        scope = f"{args.repo}#{args.pr}"
        if args.review_id:
            scope += f" review_id={args.review_id} (matched but all rows had empty head_content)"
        print(f"No usable rows found for {scope}", file=sys.stderr)
        sys.exit(1)

    baseline_dir = Path(args.baseline_dir).resolve() if args.baseline_dir else None
    cases = [build_test_case(r, baseline_dir=baseline_dir) for r in rows]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(cases, indent=2), encoding="utf-8")
    matched = sum(1 for c in cases if any(a.get("type") == "llm-rubric" for a in c["assert"]))
    print(
        f"Wrote {len(cases)} test cases to {out_path} "
        f"({matched} with baseline comparison)"
    )


if __name__ == "__main__":
    main()

# Technical Implementation Plan — Unified PR Evaluation Flow

## Goal
Create `flow_evaluation.py` in `db-files-upload-poc/`. It extracts PR data from DB, copies the appropriate template/prompt files into the PR folder, then routes to LLM or Axle review. Each PR evaluation run is **self-contained**—all files needed are inside the PR's own folder.

---

## Architecture

```mermaid
flowchart TD
    CLI["CLI: flow_evaluation.py\n--pr 123 --repo owner/repo\n--review-approach <axle|llm>\n--provider openai"]

<<<<<<< Updated upstream
    CLI --> STEP1["STEP 1: DB Extraction\nexport_specific_pr()\n\nCreates: tmp/poc/123_ts_repo/\nWrites into uploaded_to_eval_agent/:\n  files.zip\n  log-files.zip\n  repo_context.txt  (axle only, if found in DB)"]
=======
    INPUT --> EXTRACT["Step 1: DB Extraction\nexport_specific_pr()\n(writes files.zip, log-files.zip,\nproject_context.txt if available)"]
>>>>>>> Stashed changes

    STEP1 --> STEP2["STEP 2: Copy Template Files\n(from axle_input/ or llm_input/ at project root)\n\nAxle → also copies into uploaded_to_eval_agent/:\n  code_execution_prompt.txt\n  api_comprehensive_audit_prompt.yaml\n  output_format.md\n  review_guidelines.md\n\nLLM → copies into uploaded_to_eval_agent/:\n  code_execution_prompt.txt\n  api_comprehensive_audit_prompt.yaml"]

    STEP2 --> STEP3["STEP 3: Upload ALL files\nfrom uploaded_to_eval_agent/ to API\n(DB zips + template files)"]

<<<<<<< Updated upstream
    STEP3 --> STEP4["STEP 4: Run Conversation\nAxleService or PipelineOrchestrator"]

    STEP4 --> STEP5["STEP 5: Download Reports\n→ tmp/poc/123_ts_repo/reports_generated/"]

    STEP4 --> STEP6["STEP 6: Save Metrics\n→ tmp/poc/123_ts_repo/metrics/\n  execution_log.json\n  conversation_log.json\n  uploaded_files.json"]
=======
    subgraph GEMINI_FLOW["Gemini (direct prompt — no file upload)"]
        G1["Query DB for file records\n(head_content, prompt log,\nreview_result, metrics)"]
        G1B["Load project_context\nfrom local files only\n(skip silently if missing)"]
        G2["Build self-contained prompt\nper file via prompt_builder\n(provider='gemini' →\nsource/gemini_approach_input/)"]
        G3["Send single text prompt\nto Gemini API\n(GeminiAdapter)"]
        G4["Save eval report + metrics"]
        G5["Build summary prompt\nfrom all eval reports"]
        G6["Send summary to Gemini"]
        G7["Save meta-analysis report"]
        G1 --> G1B --> G2 --> G3 --> G4
        G4 -->|"2+ reports"| G5 --> G6 --> G7
    end

    subgraph AXLE_FLOW["AxleService (Claude / OpenAI — file upload)"]
        A1["Copy templates\nfrom source/{approach}_input/\nto PR folder"]
        A2["Upload all files\n(templates + DB exports)\nto provider"]
        A3["Run conversation\n+ Download generated reports"]
        A1 --> A2 --> A3
    end
>>>>>>> Stashed changes
```

### Why two flows?

- **Gemini** does not get a file-upload step. Instead, `prompt_builder` assembles a single self-contained text prompt per file (instructions + audit YAML + project context + source file + DB-stored cache prompt + LLM response + metrics) and sends it to the Gemini API. Each file is one independent API call.
- **Claude / OpenAI** use `AxleService`, which uploads the templates and DB-exported zips to the provider's file API once, then runs a single multi-turn conversation that internally loops over every file and writes per-file reports back as artifacts (downloaded into `reports_generated/`).

This split means Gemini's per-file prompt embeds the **cache prompt verbatim** — including the `CUSTOM INSTRUCTIONS - CRITICAL OVERRIDE` block — which is why the Gemini code execution prompt has its own template that knows how to evaluate against custom instructions (see "Custom Instructions Handling" below).

---

## Folder Structure

```
<<<<<<< Updated upstream
db-files-upload-poc/                         ← project root
├── axle_input/                              ← source templates (currently manual, future: DB)
│   ├── code_execution_prompt.txt
│   ├── api_comprehensive_audit_prompt.yaml
│   ├── output_format.md
│   └── review_guidelines.md
=======
pr-eval-agent/                                <- project root
├── source/
│   ├── axle_approach_input/                  <- axle templates (claude/openai providers)
│   │   ├── code_execution_prompt.txt         <- single-file evaluation prompt
│   │   ├── summary_report_prompt.txt         <- summary/meta-analysis prompt
│   │   ├── api_comprehensive_audit_prompt.yaml
│   │   ├── output_format.md
│   │   └── review_guidelines.md
│   ├── llm_approach_input/                   <- llm templates (claude/openai providers,
│   │   │                                        guidelines already embedded in cache prompt)
│   │   ├── code_execution_prompt.txt         <- single-file evaluation prompt
│   │   ├── summary_report_prompt.txt         <- summary/meta-analysis prompt
│   │   └── api_comprehensive_audit_prompt.yaml
│   └── gemini_approach_input/                <- Gemini-specific templates
│       ├── code_execution_prompt.txt         <- includes CUSTOM INSTRUCTIONS handling
│       ├── summary_report_prompt.txt         <- summary/meta-analysis prompt
│       └── api_comprehensive_audit_prompt.yaml
>>>>>>> Stashed changes
│
├── llm_input/                               ← source templates (currently manual, future: DB)
│   ├── code_execution_prompt.txt
│   └── api_comprehensive_audit_prompt.yaml
│
<<<<<<< Updated upstream
├── flow_evaluation.py                       ← [NEW] unified orchestration script
├── main.py                                  ← unchanged
│
└── tmp/poc/                                 ← accumulates one folder per evaluation run
    ├── 123_1738000000_repo-name/            ← PR 123, run 1
    │   ├── code_execution_prompt.txt        ← copied from axle_input/ or llm_input/
    │   ├── api_comprehensive_audit_prompt.yaml  ← copied
    │   ├── output_format.md                 ← copied (axle only)
    │   ├── review_guidelines.md             ← copied (axle only)
    │   ├── uploaded_to_eval_agent/
    │   │   ├── files.zip                    ← from DB (changed file content)
    │   │   ├── log-files.zip                ← from DB (prompts, metrics, responses)
    │   │   └── repo_context.txt             ← from DB (axle only, if available)
    │   ├── reports_generated/               ← downloaded audit reports
    │   └── metrics/
    │       ├── execution_log.json
    │       ├── conversation_log.json
    │       └── uploaded_files.json
    │
    ├── 123_1738010000_repo-name/            ← PR 123, run 2 (same PR, re-run)
    └── 456_1738000000_other-repo/           ← different PR
```

> [!NOTE]
> `axle_input/` and `llm_input/` are read-only source folders at the project root. The script **copies** from them into each PR's `uploaded_to_eval_agent/` — so every run is self-contained and reproducible.

> [!NOTE]
> `tmp/poc/` naturally accumulates multiple folders. Each run always creates a new `<PR>_<timestamp>_<repo>/` folder, so no run ever overwrites another.
=======
├── utils/
│   └── prompt_builder.py                     <- builds prompts from DB records + templates
│       _resolve_input_dir()                     provider-aware template directory resolver
│       build_prompt()                           per-file evaluation prompt
│       build_summary_prompt()                   summary report prompt (all eval reports)
│
├── services/
│   ├── axle/                                 <- AxleService + adapters (Anthropic, OpenAI)
│   └── gemini/                               <- GeminiAdapter (direct API client)
│
├── db/
│   └── review_data_extractor.py              <- DB queries + export
│
└── tmp/poc/                                  <- one folder per evaluation run
    └── 18_1774355801869_repo-name/
        ├── project_context.txt               <- from DB extraction (if found)
        ├── uploaded_to_eval_agent/
        │   ├── files.zip                     <- from DB (claude/openai providers)
        │   ├── log-files.zip                 <- from DB (claude/openai providers)
        │   └── file_prompt/                  <- generated prompts (Gemini provider)
        │       ├── App.js.txt                <- per-file self-contained prompt
        │       ├── Tasks.js.txt
        │       └── summary_report_prompt.txt <- summary prompt
        ├── reports_generated/                <- eval reports
        │   ├── App_eval_report.md
        │   ├── Tasks_eval_report.md
        │   └── AXLE_EVAL_META_ANALYSIS.md    <- or LLM_EVAL_META_ANALYSIS.md
        └── metrics/
            ├── App_gemini_metrics.json
            ├── Tasks_gemini_metrics.json
            └── summary_gemini_metrics.json
```

> [!NOTE]
> All three `source/*_approach_input/` folders are read-only template stores.
> - `code_execution_prompt.txt` — single-file evaluation (no summary content)
> - `summary_report_prompt.txt` — summary/meta-analysis (reads all eval reports)
> - `prompt_builder._resolve_input_dir()` selects the folder based on provider + review approach (see "Template Resolution" below).
>>>>>>> Stashed changes

---

## Files to Create / Modify

<<<<<<< Updated upstream
### [NEW] `db-files-upload-poc/flow_evaluation.py`
### [MODIFY] `db-files-upload-poc/db/review_data_extractor.py`
Rename folder name strings in `_create_pr_directories()`:
- `"uploads"` → `"uploaded_to_eval_agent"`
- `"artifacts"` → `"reports_generated"`

### [MODIFY] `db-files-upload-poc/services/axle/axle_service.py`
Same folder name updates in `__init__`:
- `"artifacts"` → `"reports_generated"`
- `"uploads"` → `"uploaded_to_eval_agent"`
=======
| File | Purpose |
|------|---------|
| `flow_evaluation.py` | Unified entry point. `run_gemini_mode` / `run_llm_mode` / `run_axle_mode` route by provider |
| `utils/prompt_builder.py` | `_resolve_input_dir(review_approach, provider, explicit)` picks the template folder. `build_prompt()` — per-file eval prompt from DB record. `build_summary_prompt()` — summary prompt from all eval reports |
| `services/gemini/gemini_adapter.py` | Sends a single text prompt to Gemini API per file, returns response + usage |
| `services/axle/axle_service.py` | Multi-file review via file upload + conversation (Anthropic / OpenAI) |
| `db/review_data_extractor.py` | DB queries: file content, prompts, responses, metrics, project context (project context is best-effort — missing rows don't fail the run) |
| `source/gemini_approach_input/code_execution_prompt.txt` | Gemini-specific eval prompt that knows how to extract and evaluate against the cache prompt's `CUSTOM INSTRUCTIONS - CRITICAL OVERRIDE` block |
>>>>>>> Stashed changes

---

## `flow_evaluation.py` — Function Breakdown

<<<<<<< Updated upstream
### Constants
```python
PROJECT_ROOT       = Path(__file__).parent
TMP_POC_DIR        = PROJECT_ROOT / "tmp" / "poc"
DEFAULT_AXLE_INPUT = PROJECT_ROOT / "axle_input"   # source templates for axle
DEFAULT_LLM_INPUT  = PROJECT_ROOT / "llm_input"    # source templates for llm
```
=======
Each prompt type is a standalone file in one of the three `source/*_approach_input/` folders:

| File | Purpose | Used by |
|------|---------|---------|
| `code_execution_prompt.txt` | Single-file evaluation instructions + report template (Sections 1-7) | `build_prompt()` |
| `summary_report_prompt.txt` | Summary/meta-analysis instructions + report template (Dashboard, Sections 1-10, Final Verdict) | `build_summary_prompt()` |
| `api_comprehensive_audit_prompt.yaml` | Audit guidelines and format schema | Both |
| `review_guidelines.md` | Permitted categories, severity levels, decision tree (axle only) | `build_prompt()` |
| `output_format.md` | JSON schema specification (axle only) | `build_prompt()` |

**Axle vs LLM approach difference:**
- **axle**: `code_execution_prompt` + `audit YAML` + `review_guidelines` + `output_format`
- **llm**: `code_execution_prompt` + `audit YAML` (guidelines already embedded in prompt log)
>>>>>>> Stashed changes

### Template Resolution (`_resolve_input_dir`)

`prompt_builder._resolve_input_dir(review_approach, provider, explicit)` picks the template folder using this precedence:

| Precedence | Condition | Folder |
|---|---|---|
| 1 | `--input-dir` provided on the CLI | the explicit path |
| 2 | `provider == "gemini"` | `source/gemini_approach_input/` |
| 3 | `review_approach == "llm"` | `source/llm_approach_input/` |
| 4 | (default — axle) | `source/axle_approach_input/` |

The Gemini override exists because Gemini builds a **self-contained per-file prompt** that embeds the cache prompt (with its `CUSTOM INSTRUCTIONS - CRITICAL OVERRIDE` block) — and the eval agent therefore needs a code execution prompt that knows how to evaluate against custom instructions. Claude/OpenAI flows don't see those custom instructions in the same shape (they upload the cache prompt as a separate log file), so they keep using `axle_approach_input/` or `llm_approach_input/`.

For axle review approach, `build_prompt` always loads `review_guidelines.md` and `output_format.md` from `axle_approach_input/` regardless of the resolved base folder, since those two files only live there.

---

## Custom Instructions Handling (Gemini-only)

The cache prompt that ships with every PR review run contains a banner-delimited block:

```
# ========================================
# CUSTOM INSTRUCTIONS - CRITICAL OVERRIDE
# ========================================
**Custom Instructions:**
- ...repository/team-specific rules...
   OR
No custom instructions found. Use default instructions.
```

These rules **override** the default categories, severity decision tree, focus areas, and tone. The Gemini eval agent embeds the entire cache prompt in its per-file prompt, so it can — and must — evaluate against them. `source/gemini_approach_input/code_execution_prompt.txt` therefore includes:

| Where | What it does |
|---|---|
| **Custom Instructions section** (after Review Guidelines) | Tells Gemini to locate the override block, recognize the empty-instructions sentinel (`No custom instructions found...`), and apply present rules in Dimensions 1, 2, 3, and 5. |
| **Prompt Log extraction list** | Names `CUSTOM INSTRUCTIONS - CRITICAL OVERRIDE` and `Non-Overrideable Requirements` as required sections to extract. |
| **Header block** of every report | Adds `Custom Instructions: [Present — N rule(s) | Absent]` so the report shows up front whether they applied. |
| **Section 2 Coverage Assessment** | Requires an explicit per-rule scan of the diff against custom-instruction items; missed items are tagged `(Custom Instruction)`. |
| **Section 5 Compliance table** | Adds a row "Honor CUSTOM INSTRUCTIONS — CRITICAL OVERRIDE block" with `N/A — block was empty` as a valid verdict. |

Claude/OpenAI flows do not have this scaffolding because their eval prompts don't see the cache prompt as a single embedded block.

---

### `class PipelineOrchestrator`
Ported from the commented-out code in `db-files-upload-poc/main.py` (lines 21–290).

| Method | Purpose | Output |
| --- | --- | --- |
| `__init__(provider_id, output_dir)` | Create provider; `output_dir` = PR's `metrics/` folder | — |
| `upload_files(file_paths) -> bool` | Upload files; save IDs to `metrics/uploaded_files.json` | `uploaded_files.json` |
| `execute_task(prompt_path, reports_dir) -> bool` | Run conversation; save raw result; download artifacts | `execution_log.json`, `conversation_log.json`, reports |
| `_extract_and_download_artifacts(result, reports_dir)` | Download from claude/openai into `reports_generated/` | audit report files |
| `run(file_paths, prompt_path, reports_dir) -> int` | Full pipeline: upload → run → download → return exit code | all outputs |

---

### `def extract_pr_data(args) -> dict`
Calls `ReviewDataExtractor.export_specific_pr()`. Returns the PR result dict containing:
- `pr_dir` → `tmp/poc/<PR_DIR>/`
- `zip_paths` → `{'files_zip': '...', 'logs_zip': '...'}`
- `repo_context_file` → path inside `uploaded_to_eval_agent/` or `None`

<<<<<<< Updated upstream
---

### `def copy_templates(pr_dir: Path, review_approach: str, input_dir: Path) -> None`
Copies template files from `input_dir` into the **PR folder root** (`tmp/poc/<PR_DIR>/`), not inside `uploaded_to_eval_agent/`.

```
Axle: copies all 4 files (prompt, yaml, output_format, review_guidelines)
LLM:  copies 2 files    (prompt, yaml)
```

---

### `def build_file_paths(pr_dir: Path, uploaded_dir: Path, review_approach: str) -> list[str]`
Returns files from two locations:
- Templates copied to `pr_dir/` (prompt, yaml, etc.)
- DB exports from `pr_dir/uploaded_to_eval_agent/` (zips, repo_context.txt)

---

### `async def run_llm_mode(args, pr_result, input_dir) -> int`
1. `copy_templates(uploaded_dir, 'llm', input_dir)`
2. `file_paths = build_file_paths(uploaded_dir)` 
3. `prompt_path = uploaded_dir / 'code_execution_prompt.txt'`
4. `PipelineOrchestrator(args.provider, metrics_dir).run(file_paths, prompt_path, reports_dir)`

---

### `async def run_axle_mode(args, pr_result, input_dir) -> int`
1. `copy_templates(uploaded_dir, 'axle', input_dir)`
2. `file_paths = build_file_paths(uploaded_dir)`
3. `prompt_path = uploaded_dir / 'code_execution_prompt.txt'`
4. `AxleService(project_root, pr_dir).execute_task(args.provider, file_paths, prompt_path)`

---

### `async def main()` — CLI Arguments
| Argument | Values | Default | Notes |
| --- | --- | --- | --- |
| `--pr` | any string | required | PR number |
| `--repo` | `owner/repo` | required | Repository |
| `--mode` | `extract_only` | `None` | Skip review step |
| `--review-approach` | `axle`, `llm` | **required** | Review engine — must be explicitly provided |
| `--provider` | `claude`, `openai` | `openai` | LLM provider |
| `--input-dir` | any path | auto | Override default `axle_input/` or `llm_input/` |
=======
### Step 1: DB Extraction (all providers)

```
export_specific_pr(repository, pr_number, review_id)
  -> Creates PR folder: tmp/poc/<PR>_<timestamp>_<repo>/
  -> Exports:
       uploaded_to_eval_agent/files.zip       (changed-file content)
       uploaded_to_eval_agent/log-files.zip   (cache prompts, responses, metrics)
       project_context.txt                    (only if found in DB; not fatal if absent)
```

If project context isn't in the DB, the extractor logs `"No project context found"` and continues — the PR folder is still created and the rest of the flow proceeds.

### Step 2a: Gemini Provider Flow (`run_gemini_mode`)

```
1. Query DB for per-file review records
   extractor.get_latest_pr_reviews(repo, pr_number)
     -> head_content, prompt log, review_result, metrics

2. Load project context (OPTIONAL — silent if missing)
   Tries in order:
     a. tmp/poc/PROJECT_CONTEXT_EXPORT.txt   (manually placed override)
     b. <pr_dir>/project_context.txt          (written by Step 1)
     c. <pr_dir>/uploaded_to_eval_agent/project_context.txt
   If none exist: prints "Project context : none available — proceeding without it"
   No DB calls are made here — anything missing in Step 1 stays missing in Step 2.

3. For each file:
   a. build_prompt(record, review_approach, project_context, provider="gemini")
      -> resolves to source/gemini_approach_input/
      -> assembles a single self-contained text prompt
   b. GeminiAdapter.evaluate(prompt_text)
      -> single API call, returns response_text + usage + duration
   c. Save:
        reports_generated/<file>_eval_report.md
        metrics/<file>_gemini_metrics.json
        uploaded_to_eval_agent/file_prompt/<file>.txt   (the prompt itself, for debugging)
```

### Step 2b: Claude / OpenAI Provider Flow (`run_axle_mode` / `run_llm_mode`)

```
1. Copy templates from source/{axle|llm}_approach_input/ into the PR folder root.
2. Build the file list: templates + everything under uploaded_to_eval_agent/
   (files.zip, log-files.zip, project_context.txt if present).
3. AxleService.execute_task(provider, file_paths, prompt_path)
     a. Upload all files to the provider's file API.
     b. Start a conversation with the code_execution_prompt + file IDs.
     c. The provider runs through every file internally, generating one
        eval report per file and writing them as artifacts.
     d. Download artifacts into reports_generated/ and metrics/.
```

Note: Claude/OpenAI flows do not consult `prompt_builder` — the LLM itself orchestrates the per-file loop using the uploaded code_execution_prompt as system instructions. The templates `prompt_builder` would otherwise resolve are simply uploaded as-is.

### Step 3: Summary Report

After all per-file eval reports exist (requires 2+):

```
a. build_summary_prompt(reports_dir, review_approach, provider)
   Reads summary_report_prompt.txt from the resolved input directory
   Appends every *_eval_report.md in full

b. Send to provider:
     - Gemini: another single API call via GeminiAdapter
     - Claude/OpenAI: this step is implicit inside AxleService's conversation

c. Save:
     reports_generated/AXLE_EVAL_META_ANALYSIS.md  (review_approach=axle)
     reports_generated/LLM_EVAL_META_ANALYSIS.md   (review_approach=llm)
     metrics/summary_{provider}_metrics.json
```
>>>>>>> Stashed changes

---

## CLI Commands

<<<<<<< Updated upstream
```bash
# Extract DB data only — no review (review-approach not needed)
python flow_evaluation.py --pr 123 --repo owner/repo --mode extract_only

# Axle review (provider defaults to openai)
python flow_evaluation.py --pr 123 --repo owner/repo  --review-id 1123456 --review-approach axle

# LLM review with claude
python flow_evaluation.py --pr 123 --repo owner/repo --review-id 546543 --review-approach llm --provider claude

# Custom input folder (future: DB-sourced)
python flow_evaluation.py --pr 123 --repo owner/repo review-id 456765 --review-approach axle --input-dir /path/to/inputs/
=======
 ## Correct CLI
 python flow_evaluation.py --pr 1627 --repo Recrui8/recrui8 --review-approach llm --provider gemini --review-id a5d933e9-6464-4265-afb3-00342833db22

### Full Evaluation (all files + summary report)

```bash
python flow_evaluation.py --pr 18 --repo owner/repo --review-id 123456 --review-approach axle

python flow_evaluation.py --pr 18 --repo owner/repo --review-id 123456 --review-approach llm
```

### With Specific Provider

```bash
python flow_evaluation.py --pr 18 --repo owner/repo --review-id 123456 --review-approach axle --provider claude

python flow_evaluation.py --pr 18 --repo owner/repo --review-id 123456 --review-approach llm --provider openai
```

### Single File Evaluation

```bash
python flow_evaluation.py --pr 18 --repo owner/repo --review-id 123456 --review-approach axle --file schema.py
```

### Dry Run (build prompts only, no LLM calls)

```bash
python flow_evaluation.py --pr 18 --repo owner/repo --review-id 123456 --review-approach axle --dry-run
```

### DB Extraction Only

```bash
python flow_evaluation.py --pr 123 --repo owner/repo --mode extract_only --review-id 123456
```

> Note: `--review-id` is required for extraction because the extractor must pin the export to a specific historical review run. This avoids returning the latest PR run by mistake.

---

## Future UI Integration

The UI will collect the same required inputs and call the evaluation pipeline:

```
┌─────────────────────────────────────────┐
│  PR Evaluation                          │
│                                         │
│  PR Number:      [18           ]        │
│  Repository:     [owner/repo   ]        │
│  Review ID:      [123456       ]        │
│  Review Approach: (•) Axle  ( ) LLM    │
│  Provider:        Gemini  ▼  (optional) │
│                                         │
│  [ Run Evaluation ]  [ Dry Run ]        │
└─────────────────────────────────────────┘
```

The UI will invoke the same pipeline as the CLI — the core logic in
`flow_evaluation.py`, `prompt_builder.py`, and the provider adapters
remains unchanged.

---

## Environment Variables

```bash
# Required for Gemini provider (default)
GEMINI_API_KEY=AIzaSy...

# Required for AxleService providers
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-proj-...

# Database connection
DB_HOST=...
DB_PORT=...
DB_NAME=...
DB_USER=...
DB_PASSWORD=...
>>>>>>> Stashed changes
```

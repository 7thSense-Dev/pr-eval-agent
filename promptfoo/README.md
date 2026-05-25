# promptfoo eval harness

Pulls real review rows from `code_review.review_eval_metrics`, builds the production prompt via `utils.prompt_builder.build_prompt()`, sends to the configured LLM, and asserts on the output.

## One-time install

```bash
cd promptfoo
npm install
```

Promptfoo will be installed locally under `node_modules/.bin/promptfoo`. The `npm run` scripts use that local binary — no global install needed.

Promptfoo invokes Python for the custom provider. Make sure `python` resolves to the same interpreter that has pr-eval-agent's deps installed (`requirements.txt`). Override with `PROMPTFOO_PYTHON=/abs/path/to/python` if needed.

## Workflow

1. **Generate test cases from the DB** for a given PR:

   ```bash
   python scripts/generate_fixtures.py --repo owner/repo --pr 18
   ```

   This queries `code_review.review_eval_metrics`, takes the latest review row per file, and writes `fixtures/tests.json` — one promptfoo test case per file. Each case includes:
   - `vars.record` — the full DB row (file, head_content, base_content, prompt, review_result, llm_service_metrics, metadata_)
   - `vars.project_context` — extracted from `metadata_.project_context`
   - Assertions: schema validity, severity enum check, comment-count tolerance vs reference, LLM-rubric semantic match against the stored review_result

2. **Run the eval**:

   ```bash
   npm run eval
   ```

3. **View the diff report**:

   ```bash
   npm run view
   ```

   Opens a browser with pass/fail per test, per provider, plus the rendered prompt and response.

## Common flags for `generate_fixtures.py`

| Flag | Purpose |
|---|---|
| `--repo owner/repo` | repository filter (required) |
| `--pr 18` | PR number (required) |
| `--limit 5` | cap how many file rows to load (default 20) |
| `--no-reference` | drop the comment-count + LLM-rubric assertions that compare against the stored review. Use this when you want pure prompt-validation without treating past output as ground truth. |

## Adding more models

Edit `promptfooconfig.yaml`, add another entry under `providers:` with a different `config.model` (or `config.provider`). Promptfoo runs every test against every provider and shows side-by-side results.

Currently only Gemini is wired. To add Claude/OpenAI:
1. Add a branch in `providers/review_provider.py::_dispatch()` that calls the appropriate adapter from `services/axle/adapters/`.
2. Add the provider entry in the config.

## Layout

```
promptfoo/
  package.json
  promptfooconfig.yaml            # references fixtures/tests.json
  providers/
    review_provider.py            # vars -> build_prompt() -> adapter.evaluate()
  scripts/
    generate_fixtures.py          # query review_eval_metrics -> fixtures/tests.json
  fixtures/
    review_schema.json            # JSON schema for the LLM output
    tests.json                    # GENERATED — do not edit by hand
```

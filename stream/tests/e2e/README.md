# Gemini E2E JSONL Runner

This runner executes only S3/S4 against the live server, saves the Markdown
outputs under `reports/e2e-outputs/`, and writes one JSON object per line to
`reports/e2e-results.jsonl`.

## Required setup

- Start the server with Gemini enabled.
- Provide your API key in `GOOGLE_API_KEY` (or `GEMINI_API_KEY`).

Example environment:

```bash
export LLM_PROVIDER=gemini
export GOOGLE_API_KEY=your-key
export GEMINI_MODEL=gemini-2.5-flash
export GEMINI_TEMPERATURE=0
export GEMINI_TOP_P=0.1
export GEMINI_TOP_K=1
```

Start the server:

```bash
npm run dev:server
```

Run the E2E runner:

```bash
node tests/e2e/run-gemini-e2e.js
```

## JSONL fields

Each line is a JSON object with the following fields:

- `ts`: record timestamp (ISO 8601)
- `runId`: run identifier
- `iteration`: iteration number (1-based)
- `scenario`: `S3` or `S4`
- `modelName`: Gemini model name
- `temperature`, `topP`, `topK`: sampling settings
- `stopAfterMs`, `resumeAfterMs`: fixed interruption timing
- `outputLength`, `outputHash`: final output length and SHA-256
- `outputPath`: saved Markdown file path
- `offsetLastSeen`, `offsetExpectedLast`, `offsetMissingCount`: offset tracking
- `done`: whether a `done` event was observed
- `error`: error message if any
- `mdValid`: markdown structure pass/fail
- `replReady`: TypeScript snippet parses without syntax errors
- `codeBlockLines`: line count of the first TypeScript code block

`mdValid` checks: H1 present, at least two H2 headings, one TypeScript code
block, 10-20 code lines, and balanced code fences.

## Tunables

- `ITERATIONS` (default: 3)
- `STOP_AFTER_MS` (default: 1200)
- `RESUME_AFTER_MS` (default: 800)
- `BASE_URL` (default: http://localhost:8080)
- `OUTPUT_PATH` (default: reports/e2e-results.jsonl)
- `OUTPUT_DIR` (default: reports/e2e-outputs)
- `PROMPT_PATH` (default: tests/e2e/prompt.md)

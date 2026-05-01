"""Log-parsing agent: analyzes pipeline health and files GitHub issues."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import anthropic

REPO_ROOT = Path(__file__).resolve().parents[2]

SYSTEM_PROMPT = """You are a data pipeline health agent for the market_data project.

Your job is to analyze the pipeline's logs, state, and data freshness, then file \
GitHub issues for any problems worth tracking.

## What to look for

1. **Stale data** — check `logs/metrics.json` for when each data_type last ran \
successfully, and `state.json` for `last_run`. File an issue if any data type \
hasn't updated within its expected window:
   - ohlcv: 2 days
   - options: 14 days
   - fundamentals: 35 days
   - macro: 7 days
   - indices: 2 days

2. **Quarantined tickers** — in `state.json`, check `fetch_failures`. Any ticker \
with `count >= 5` is quarantined. File a single issue listing all quarantined tickers \
with their failure reasons and last-failure dates.

3. **High failure rates** — in `logs/metrics.json`, look at recent runs. If a run \
had more than 10% of `symbols_attempted` in `symbols_failed`, flag it. If the same \
ticker appears in `symbols_failed` across multiple runs, call that out.

4. **Silent failures** — if a batch run shows `symbols_attempted == 0` when the \
pipeline should have processed tickers, that is a silent failure worth noting.

5. **Empty data directories** — use `list_files` to confirm that `data/ohlcv`, \
`data/fundamentals`, `data/macro`, and `data/indices` exist and are non-empty.

## Rules for filing issues

- Only file issues for real, confirmed problems — not hypotheticals.
- One issue per distinct problem category (e.g. one issue for stale fundamentals, \
one for quarantined tickers). Do not file one issue per failing ticker.
- Be specific: include ticker names, dates, row counts, error reasons from the logs.
- If everything looks healthy, say so and don't file any issues.

## Output format

After your analysis, report:
1. A brief summary of each thing you checked and its status.
2. Any GitHub issues you filed, with their URLs.
3. A one-line overall verdict (healthy / issues found).

Start by reading `logs/metrics.json` and `state.json`."""

TOOLS: list[dict[str, Any]] = [
    {
        "name": "read_file",
        "description": (
            "Read a file from the market_data project. "
            "Accepts paths relative to the repo root (e.g. 'logs/metrics.json', "
            "'state.json') or absolute paths."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path relative to repo root or absolute.",
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "list_files",
        "description": (
            "List files in a project directory. "
            "Returns file names, sizes, and modification times."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "directory": {
                    "type": "string",
                    "description": (
                        "Directory path relative to repo root "
                        "(e.g. 'data/ohlcv', 'logs')."
                    ),
                },
                "pattern": {
                    "type": "string",
                    "description": (
                        "Glob pattern to filter files "
                        "(e.g. '*.parquet', '*.json'). Defaults to '*'."
                    ),
                },
            },
            "required": ["directory"],
        },
    },
    {
        "name": "create_github_issue",
        "description": (
            "Create a GitHub issue in the michaelk95/market_data repository. "
            "Use this when you find a confirmed problem worth tracking."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Concise issue title (under 80 characters).",
                },
                "body": {
                    "type": "string",
                    "description": (
                        "Issue body in Markdown. Include: what was found, "
                        "relevant data (failure counts, staleness ages, affected "
                        "symbols), and suggested next steps."
                    ),
                },
                "labels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional label names to apply.",
                },
            },
            "required": ["title", "body"],
        },
    },
]


def _resolve(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else REPO_ROOT / p


def _tool_read_file(path: str) -> str:
    resolved = _resolve(path)
    if not resolved.exists():
        return f"Error: file not found: {resolved}"
    try:
        return resolved.read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        return f"Error reading {resolved}: {exc}"


def _tool_list_files(directory: str, pattern: str = "*") -> str:
    import datetime

    resolved = _resolve(directory)
    if not resolved.exists():
        return f"Error: directory not found: {resolved}"
    if not resolved.is_dir():
        return f"Error: not a directory: {resolved}"

    rows = []
    for f in sorted(resolved.glob(pattern)):
        if f.is_file():
            mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime(
                "%Y-%m-%d %H:%M"
            )
            size_kb = f.stat().st_size // 1024
            rows.append(f"{f.name}  ({size_kb} KB, modified {mtime})")

    if not rows:
        return f"No files matching '{pattern}' in {directory}"
    return "\n".join(rows)


def _tool_create_github_issue(
    title: str,
    body: str,
    labels: list[str] | None = None,
) -> str:
    cmd = ["gh", "issue", "create", "--title", title, "--body", body]
    for label in labels or []:
        cmd += ["--label", label]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
        if result.returncode == 0:
            return f"Issue created: {result.stdout.strip()}"
        return f"Error creating issue: {result.stderr.strip()}"
    except FileNotFoundError:
        return (
            "Error: 'gh' CLI not found. "
            "Install GitHub CLI and run 'gh auth login'."
        )


def _execute_tool(name: str, inputs: dict[str, Any]) -> str:
    if name == "read_file":
        return _tool_read_file(inputs["path"])
    if name == "list_files":
        return _tool_list_files(inputs["directory"], inputs.get("pattern", "*"))
    if name == "create_github_issue":
        return _tool_create_github_issue(
            inputs["title"],
            inputs["body"],
            inputs.get("labels"),
        )
    return f"Error: unknown tool '{name}'"


def run_agent(verbose: bool = False) -> None:
    client = anthropic.Anthropic()
    messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "content": (
                "Analyze the pipeline logs and file GitHub issues "
                "for any problems you find."
            ),
        }
    ]

    while True:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            thinking={"type": "adaptive"},
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=TOOLS,
            messages=messages,
        )

        tool_uses = [b for b in response.content if b.type == "tool_use"]
        text_blocks = [b for b in response.content if b.type == "text"]

        for block in text_blocks:
            print(block.text)

        if response.stop_reason == "end_turn" or not tool_uses:
            break

        messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        for tool in tool_uses:
            if verbose:
                print(f"\n[tool: {tool.name}]", flush=True)
                print(json.dumps(tool.input, indent=2), flush=True)
            result = _execute_tool(tool.name, tool.input)
            if verbose:
                preview = result[:600] + "…" if len(result) > 600 else result
                print(f"[result]\n{preview}", flush=True)
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": tool.id,
                    "content": result,
                }
            )

        messages.append({"role": "user", "content": tool_results})


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Pipeline health agent — reads logs, checks data freshness, "
            "and files GitHub issues for confirmed problems."
        )
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print tool calls and results.",
    )
    args = parser.parse_args()

    try:
        run_agent(verbose=args.verbose)
    except anthropic.AuthenticationError:
        print("Error: ANTHROPIC_API_KEY is not set or invalid.", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()

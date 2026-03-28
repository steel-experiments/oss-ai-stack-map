# AGENTS

## Report Publishing

- The published report has a stable entrypoint at [docs/index.html](/home/agent/oss-ai-stack-map/docs/index.html).
- Each render also refreshes:
  - [docs/oss-ai-stack-report-latest.html](/home/agent/oss-ai-stack-map/docs/oss-ai-stack-report-latest.html)
  - [docs/report-latest.json](/home/agent/oss-ai-stack-map/docs/report-latest.json)
- Versioned reports such as `docs/oss-ai-stack-report-2026-03-25-v12.html` are retained, but the stable URL should always point at `docs/`.

## Rendering

- Render with:
  - `uv run python scripts/render_html_report.py --input-dir <snapshot-dir> --output docs/oss-ai-stack-report-<date>-<version>.html`
- The renderer automatically updates the stable entrypoints above.
- The stable HTML includes a small polling script that reloads when `report-latest.json` changes.

## Serving

- Use [scripts/serve_latest_report.sh](/home/agent/oss-ai-stack-map/scripts/serve_latest_report.sh) to publish the docs directory over Tailscale.
- On this machine, `tailscale serve` requires `sudo`.
- The expected serve target is the directory:
  - `/home/agent/oss-ai-stack-map/docs`
- Do not pin Tailscale Serve to a versioned HTML file; serve the directory so `/` always resolves to the latest report.

## Current Live URL

- `https://claude-code-vm.tailef8c96.ts.net/`

# sigma-ci

CI/CD validation and reporting for [Sigma Computing](https://sigmacomputing.com) data models. Catches schema drift, broken formulas, blast-radius exposure, and workbooks that bypass the data model layer — before they reach production.

## What it checks

| Check | What it flags |
|---|---|
| **Schema drift** | Columns dropped from the warehouse that a data model element still references — these will error at query time |
| **Content / blast radius** | How many workbooks (direct and transitive) would break if a model element were removed |
| **Formula integrity** | Columns with broken or unresolvable formula references |
| **Workbook direct sources** | Workbook elements sourced directly from warehouse tables or Custom SQL (bypassing the data model layer); flags columns the element references that have since been dropped from the warehouse |

## Setup

**Prerequisites:** Node 20+, a Sigma API client ID and secret.

```bash
git clone https://github.com/twells89/sigma-ci.git
cd sigma-ci
npm install && npm run build
```

Set environment variables:

```bash
export SIGMA_CLIENT_ID=your-client-id
export SIGMA_CLIENT_SECRET=your-client-secret
# Optional — defaults to https://aws-api.sigmacomputing.com
export SIGMA_BASE_URL=https://aws-api.sigmacomputing.com
```

## CLI usage

### `report` — full HTML report (opens in browser)

```bash
# All models in the org
node dist/index.js report --all

# Specific models
node dist/index.js report --model <id> --model <id>

# Output as JSON instead of opening browser
node dist/index.js report --all -f json > report.json
```

### `validate` — machine-readable output for CI

```bash
# Validate all models, output text summary
node dist/index.js validate --all

# Validate specific models, output JSON
node dist/index.js validate --model <id> -f json

# Flags
--content-only     # Skip schema drift check
--drift-only       # Skip content / blast-radius check
--skip-sync        # Skip pre-syncing table schemas before drift check
--open             # Generate HTML report and open in browser
```

### Output formats

| Flag | Description |
|---|---|
| `-f text` | Human-readable summary (default for `validate`) |
| `-f json` | Structured JSON — suitable for downstream tooling |
| `-f html` | Full HTML report |
| `--open` | Write HTML to a temp file and open in the system browser |

## GitHub Actions

Add to `.github/workflows/sigma-validate.yml`:

```yaml
name: Sigma CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: twells89/sigma-ci@main
        with:
          sigma-client-id: ${{ secrets.SIGMA_CLIENT_ID }}
          sigma-client-secret: ${{ secrets.SIGMA_CLIENT_SECRET }}
          # Leave blank to validate all models, or list specific IDs:
          # model-ids: 'abc123 def456'
          fail-on-drift: 'true'
```

### Action inputs

| Input | Required | Default | Description |
|---|---|---|---|
| `sigma-client-id` | Yes | — | Sigma API client ID |
| `sigma-client-secret` | Yes | — | Sigma API client secret |
| `model-ids` | No | _(all)_ | Space-separated model IDs. Leave blank for all models. |
| `fail-on-drift` | No | `true` | Exit non-zero if schema drift is detected (blocks merge) |

### Action outputs

| Output | Description |
|---|---|
| `has-drift` | `"true"` if drift was detected in any model |
| `report-markdown` | Validation summary as GitHub-flavored markdown |
| `report-json` | Full report as a JSON string |

## JSON report structure

```json
{
  "generatedAt": "2024-01-01T00:00:00.000Z",
  "content": {
    "models": [
      {
        "modelId": "...",
        "modelName": "...",
        "modelUrl": "https://...",
        "path": "Dev",
        "ownerId": "abc123",
        "downstreamWorkbooks": [{ "workbookId": "...", "name": "...", "folder": "...", "url": "https://..." }],
        "transitiveWorkbooks": [...]
      }
    ],
    "modelDependencies": { "modelId": ["upstreamModelId"] }
  },
  "schemaDrift": {
    "models": [
      {
        "modelId": "...",
        "modelName": "...",
        "hasDrift": true,
        "tables": [
          {
            "tableName": "ORDERS",
            "droppedColumns": ["LEGACY_COL"]
          }
        ]
      }
    ]
  },
  "formulaCheck": {
    "models": [...]
  },
  "directSourceWorkbooks": {
    "workbooks": [
      {
        "workbookId": "...",
        "workbookName": "My Workbook",
        "workbookUrl": "https://...",
        "path": "My Documents",
        "ownerId": "abc123",
        "hasDrift": false,
        "hasCustomSql": true,
        "elements": [
          {
            "elementId": "...",
            "elementName": "ORDERS",
            "sourceKind": "warehouse-table",
            "tableName": "ORDERS",
            "tableInodeId": "inode-...",
            "connectionId": "...",
            "referencedColumns": ["ORDER_ID", "AMOUNT"],
            "actualColumns": ["ORDER_ID", "AMOUNT", "STATUS"],
            "missingColumns": []
          },
          {
            "elementId": "...",
            "elementName": null,
            "sourceKind": "custom-sql",
            "connectionId": "...",
            "sqlDefinition": "select * from MY_SCHEMA.ORDERS",
            "referencedColumns": [],
            "actualColumns": [],
            "missingColumns": []
          }
        ]
      }
    ],
    "totalWorkbooksScanned": 47,
    "totalDirectElements": 5,
    "totalCustomSqlElements": 2,
    "totalMissingColumns": 0
  }
}
```

## Web UI (Render)

The repo includes a `render.yaml` for deploying a persistent web UI via [Render](https://render.com). The web server (`src/server.ts`) exposes a form-based interface for triggering on-demand reports without the CLI.

The HTML report includes:
- **Overview table** — all models with folder path, owner name, downstream workbook counts, and check statuses at a glance
- **Direct-source workbooks section** — workbooks bypassing the data model layer, with folder and owner info
- **Detail cards** — per-model breakdown of downstream workbooks, schema drift tables, and formula issues

Set `SIGMA_CLIENT_ID` and `SIGMA_CLIENT_SECRET` as environment variables in the Render dashboard.

## GitHub sync workflows

`github-integration/` contains two optional workflows for bidirectional sync between GitHub and Sigma:

- **`pull-from-sigma.yml`** — runs daily (or on demand) to pull the latest data model specs from Sigma into the repo as JSON files
- **`sync-to-sigma.yml`** — triggers on pushes to `data-models/*.json` to push spec changes back to Sigma

## Rate limiting

The Sigma API enforces rate limits. On large orgs (100+ models), the drift check's column pre-fetch will hit 429s and back off automatically (exponential, up to 30s delay, 4 retries). Progress is written to stderr so it stays visible in CI logs without polluting the report output. Use `--skip-sync` to skip the column pre-fetch if you know schemas haven't changed.

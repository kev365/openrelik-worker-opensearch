# openrelik-worker-opensearch

OpenRelik worker for indexing workflow results into OpenSearch and uploading to Timesketch. Ships two Celery tasks from a single Docker image.

## Search Guides

For tips on querying your uploaded data, see:

- [Searching in OpenSearch Dashboards](docs/searching-opensearch.md)
- [Searching in Timesketch](docs/searching-timesketch.md)

## Tasks

### OpenSearch Upload

Indexes documents from upstream workflow files into an OpenSearch index via the bulk API.

- Auto-detects file format: JSONL, JSON, CSV, TSV, Parquet, and gzipped variants.
- Adds OpenRelik context fields (`openrelik_workflow_id`, `openrelik_source_file`, `openrelik_source_path`).
- Optional timelining: fan out rows by datetime fields into timeline-friendly documents with `datetime` and `timestamp_desc` columns.
- Optional index alias association and index pattern creation in OpenSearch Dashboards.

### Timesketch Upload

Streams documents to Timesketch via the ImportStreamer client with sketch and timeline registration.

- Creates or reuses sketches and timelines.
- Auto-detects `datetime`, `message`, and `timestamp_desc` fields from source columns.
- Supports timelining with multiple datetime fields (one event per field per row).
- Converts timestamps to ISO8601 UTC before upload.
- Timeline name prefix mode for multi-file uploads, or merge mode for combining files into a single timeline.

## Environment Variables

### OpenSearch Variables

| Variable | Description |
|---|---|
| `OPENSEARCH_URL` | OpenSearch URL (default: `https://opensearch:9200`) |
| `OPENSEARCH_USERNAME` | Basic auth username (optional) |
| `OPENSEARCH_PASSWORD` | Basic auth password (optional) |
| `OPENSEARCH_API_KEY` | API key auth (optional, takes precedence over basic auth) |
| `OPENSEARCH_VERIFY_CERTS` | TLS cert verification (default: `true`) |
| `OPENSEARCH_DASHBOARDS_URL` | Dashboards URL for index pattern creation (optional, e.g. `http://opensearch-dashboards:5601`) |

### Timesketch Variables

| Variable | Description |
|---|---|
| `TIMESKETCH_SERVER_URL` | Timesketch API URL (e.g. `http://timesketch-web:5000`) |
| `TIMESKETCH_SERVER_EXTERNAL_URL` | Browser-accessible URL for sketch links in reports, if different from `TIMESKETCH_SERVER_URL` (optional) |
| `TIMESKETCH_USERNAME` | Timesketch username (can be overridden per task) |
| `TIMESKETCH_PASSWORD` | Timesketch password (can be overridden per task) |

## Task Configuration

### OpenSearch Upload Config

| Field | Type | Required | Description |
|---|---|---|---|
| `index_name` | text | Yes | OpenSearch index name for storing documents |
| `mapping` | select | No | Index mapping: `dynamic` (default), a bundled name (e.g. `kstrike_ual`), or `custom` (.mapping file in workflow) |
| `delimiter` | select | No | Source file delimiter. `auto` detects from file extension. Set manually for CSV/TSV |
| `source_timezone` | select | No | Timezone of source timestamps. Converted to UTC before indexing |
| `datetime_fields` | text | No | Comma-separated timestamp fields for timelining (e.g. `created_at,modified_at`). Each row expands into one document per field |
| `datetime_format` | select | No | Timestamp format. `auto` attempts automatic parsing |
| `index_alias` | text | No | Associate an alias with the index after indexing |
| `create_index_pattern` | checkbox | No | Create index patterns in OpenSearch Dashboards (requires `OPENSEARCH_DASHBOARDS_URL` env var) |
| `batch_size` | text | No | Documents per bulk request (default: 1000, max: 10000) |
| `opensearch_url` | text | No | OpenSearch URL override (default: `http://opensearch:9200`) |
| `username` | text | No | Basic auth username |
| `password` | text | No | Basic auth password |
| `api_key` | text | No | API key auth (takes precedence over basic auth) |
| `disable_verify_certs` | checkbox | No | Skip TLS certificate verification |

### Timesketch Upload Config

**Sketch:**

| Field | Type | Required | Description |
|---|---|---|---|
| `sketch_name` | text | No | Name for a new sketch (ignored if sketch ID is set) |
| `sketch_id` | text | No | Existing sketch ID to add the timeline to |
| `timeline_name` | text | No | Prefix for timeline names (e.g. `case42` becomes `case42 - filename.txt`). With merge enabled, used as the exact timeline name |
| `merge_timelines` | checkbox | No | Upload all files into one timeline using the name above |

**Field mapping:**

| Field | Type | Required | Description |
|---|---|---|---|
| `ts_datetime_field` | text | No | Comma-separated timestamp field names (e.g. `created_at,modified_at`). Auto-detected if source has a `datetime` column |
| `ts_message_fields` | text | No | Comma-separated fields to concatenate into Timesketch `message` (e.g. `user,action,resource`). Auto-detected if source has a `message` column |

**Source format:**

| Field | Type | Required | Description |
|---|---|---|---|
| `delimiter` | select | No | Source file delimiter. `auto` detects from file extension |
| `ts_datetime_format` | select | No | Timestamp format. `auto` attempts automatic parsing. Values are converted to ISO8601 |
| `source_timezone` | select | No | Timezone of source timestamps. Converted to UTC before upload |

**Connection:**

| Field | Type | Required | Description |
|---|---|---|---|
| `timesketch_url` | text | Yes | Timesketch API URL (default: `http://host.docker.internal`) |
| `timesketch_username` | text | Yes | Timesketch username |
| `timesketch_password` | text | Yes | Timesketch password |

## Bundled Mappings

| Name | Description |
|---|---|
| `kstrike_ual` | KStrike UAL parser output with typed date fields (`insertdate`, `lastaccess`) |

Bundled mappings are auto-discovered from `src/mappings/*.json`. They define OpenSearch index field types for the export task. Use `dynamic` to let OpenSearch auto-detect types, or `custom` to upload your own `.mapping` file.

## Local Development

```bash
# Install dependencies with test and timesketch extras
uv sync --group test --extra timesketch

# Run tests
uv run pytest -v
```

"""OpenRelik OpenSearch and Timesketch workers.

Two Celery tasks registered from one image:
  - ``export``            — bulk-index data into OpenSearch
  - ``timesketch_upload`` — stream data to Timesketch via ImportStreamer
"""

import csv
import gzip
import json
import os
import re
from pathlib import Path
from typing import Any, Iterator, Optional, List, Dict

import requests
from celery import signals
from dateutil import parser as dateutil_parser
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

from openrelik_worker_common.logging import Logger
from openrelik_worker_common.reporting import Report, Priority, MarkdownTable
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TASK_NAME_EXPORT = "openrelik-worker-opensearch.tasks.export"
TASK_NAME_TIMESKETCH = "openrelik-worker-opensearch.tasks.timesketch_upload"

MAPPINGS_DIR = Path(__file__).parent / "mappings"

KSTRIKE_HEADER_PREFIX = "RoleGuid (RoleName)||"

DEFAULT_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 10_000

# Characters forbidden in OpenSearch index names.
_INDEX_NAME_INVALID_RE = re.compile(r'[\\/*?"<>| ,#:]')
_URL_ALLOWED_SCHEMES = {"http", "https"}

# Map file extensions to format identifiers.
# Compound extensions (.json.gz) are checked before single extensions.
FORMAT_MAP_COMPOUND = {
    ".jsonl.gz": "jsonl_gz",
    ".json.gz": "json_gz",
}
FORMAT_MAP_SINGLE = {
    ".jsonl": "jsonl",
    ".json": "json",
    ".csv": "csv",
    ".tsv": "tsv",
    ".txt": "kstrike",
    ".parquet": "parquet",
}

# ---------------------------------------------------------------------------
# Task metadata
# ---------------------------------------------------------------------------

# Mapping choices for the select dropdown.
# "dynamic" relies on OpenSearch to auto-detect field types.
# "custom" means the user uploads a .mapping file in the workflow.
# Bundled mappings are auto-discovered from the mappings/ directory.
_bundled = sorted(p.stem for p in MAPPINGS_DIR.glob("*.json")) if MAPPINGS_DIR.is_dir() else []
MAPPING_CHOICES = ["dynamic"] + _bundled + ["custom"]

# Delimiter choices for source file parsing.
# The display label is for readability; the actual delimiter char is resolved
# in _resolve_delimiter().
DELIMITER_CHOICES = [
    "auto",
    "comma (,)",
    "tab (\\t)",
    "pipe (|)",
    "double-pipe (||)",
    "semicolon (;)",
]

# Maps the dropdown label back to the actual delimiter character.
_DELIMITER_MAP = {
    "auto": "",
    "comma (,)": ",",
    "tab (\\t)": "\t",
    "pipe (|)": "|",
    "double-pipe (||)": "||",
    "semicolon (;)": ";",
}

# Datetime format choices for Timesketch timestamp conversion.
DATETIME_FORMAT_CHOICES = [
    "auto",
    "ISO8601",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "epoch_seconds",
    "epoch_millis",
]

# Common timezone choices. The worker converts all timestamps to UTC
# before uploading. OpenSearch Dashboards can then adjust display timezone.
TIMEZONE_CHOICES = [
    "UTC",
    "US/Eastern",
    "US/Central",
    "US/Mountain",
    "US/Pacific",
    "Europe/London",
    "Europe/Berlin",
    "Europe/Paris",
    "Asia/Tokyo",
    "Asia/Shanghai",
    "Australia/Sydney",
]

EXPORT_TASK_METADATA = {
    "display_name": "OpenSearch Upload",
    "description": (
        "Upload data into OpenSearch. "
        "All input files are indexed into the same index — use separate "
        "workflows for different data types and different indices."
    ),
    "task_config": [
        {
            "name": "index_name",
            "label": "Index name",
            "description": "\U0001FAAA OpenSearch index where selected documents will be stored.",
            "type": "text",
            "required": True,
        },
        {
            "name": "mapping",
            "label": "Index mapping",
            "description": (
                "\U0001F9ED Select a bundled mapping for the source data. "
                "'dynamic' relies on OpenSearch to auto-detect field types. "
                "'custom' uses a .mapping file uploaded in the workflow."
            ),
            "type": "select",
            "items": MAPPING_CHOICES,
            "required": False,
        },
        {
            "name": "delimiter",
            "label": "Source file delimiter",
            "description": (
                "\U0001F537 Delimiter used in the source file. Use 'auto' for JSON, "
                "JSONL, NDJSON, Parquet, and other structured formats — "
                "the delimiter is detected from the file extension. "
                "Select a specific delimiter for flat-file formats like CSV or TSV."
            ),
            "type": "select",
            "items": DELIMITER_CHOICES,
            "required": False,
        },
        {
            "name": "source_timezone",
            "label": "Source data timezone",
            "description": (
                "\U0001F30E Timezone of the timestamps in the source data. "
                "All timestamps are converted to UTC before indexing. "
                "Use OpenSearch Dashboards to adjust the display timezone."
            ),
            "type": "select",
            "items": TIMEZONE_CHOICES,
            "required": False,
        },
        {
            "name": "datetime_fields",
            "label": "Datetime fields for timelining",
            "description": (
                "\U0001FAA7 Timelining: Comma-separated list of source field names "
                "that contain timestamps (e.g. 'created_at,modified_at,last_access'). "
                "Each source row is expanded into one indexed document per "
                "field, with a 'datetime' and 'timestamp_desc' column — "
                "enabling timeline views in OpenSearch Dashboards. "
                "Timestamps are converted to ISO8601 UTC. "
                "Leave blank to skip timelining."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "datetime_format",
            "label": "Datetime format",
            "description": (
                "\U0001f4c4 Format of the source timestamps. 'auto' attempts to parse "
                "automatically. Only applies to fields listed in "
                "'Datetime fields' above."
            ),
            "type": "select",
            "items": DATETIME_FORMAT_CHOICES,
            "required": False,
        },
        {
            "name": "index_alias",
            "label": "Index alias",
            "description": "\U0001fac6 Associate the index with this alias after indexing.",
            "type": "text",
            "required": False,
        },
        {
            "name": "create_index_pattern",
            "label": "Create index pattern for dashboard. ",
            "description": (
                "\U0001fac6 Create an index pattern in OpenSearch Dashboards "
                "for the index (and alias, if set). "
            ),
            "type": "checkbox",
            "required": False,
        },
        {
            "name": "batch_size",
            "label": "Bulk batch size",
            "description": "\U0001F535 Documents per bulk request (default: 1000).",
            "type": "text",
            "required": False,
        },
        {
            "name": "opensearch_url",
            "label": "OpenSearch URL",
            "description": "\U0001F517 Provide the OpenSearch URL.",
            "type": "text",
            "required": False,
            "value": "http://opensearch:9200",
        },
        {
            "name": "username",
            "label": "Username",
            "description": "\U0001F464 Username for basic auth (leave blank for no auth).",
            "type": "text",
            "required": False,
        },
        {
            "name": "password",
            "label": "Password",
            "description": "\U0001F511 Password for basic auth (leave blank for no auth).",
            "type": "text",
            "required": False,
        },
        {
            "name": "api_key",
            "label": "API key",
            "description": (
                "\U0001f510 API key for OpenSearch authentication "
                "Takes precedence over username/password if both are set."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "disable_verify_certs",
            "label": "Disable TLS verification",
            "description": "Check to skip TLS certificate verification.",
            "type": "checkbox",
            "required": False,
        },
    ],
}

TIMESKETCH_TASK_METADATA = {
    "display_name": "Timesketch Upload",
    "description": (
        "Upload data to Timesketch with field mapping and sketch registration. "
        "All input files are uploaded to the same sketch — use separate "
        "workflows for different data types and sketches."
    ),
    "task_config": [
        # -- Sketch --
        {
            "name": "sketch_name",
            "label": "Sketch name",
            "description": "\U0001F58C Name for a new Sketch (ignored if Sketch ID is set).",
            "type": "text",
            "required": False,
        },
        {
            "name": "sketch_id",
            "label": "Sketch ID",
            "description": "\U0001FAAA Existing Sketch ID to add the timeline to.",
            "type": "text",
            "required": False,
        },
        {
            "name": "timeline_name",
            "label": "Timeline prefix or name",
            "description": (
                "\U0001F7E6 Used as a prefix on each source filename by default "
                "(e.g. 'case42' \u2192 'case42 - evtx_security.txt'). "
                "Check 'Merge timelines' below to use this as the exact name "
                "for a single combined timeline."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "merge_timelines",
            "label": " \U0001F502 Merge timelines",
            "type": "checkbox",
            "required": False,
        },
        # -- Field mapping --
        {
            "name": "ts_datetime_field",
            "label": "Datetime fields",
            "description": (
                "\U0001FAA7 Comma-separated list of source field names that contain "
                "timestamps (e.g. 'created_at,modified_at'). "
                "Each SOURCE row is expanded into one Timesketch event per "
                "field, with 'timestamp_desc' auto-generated from the field "
                "name \u2014 enabling rich timeline views. "
                "Auto-detected if the data has a 'datetime' column."
            ),
            "type": "text",
            "required": False,
        },
                {
            "name": "ts_message_fields",
            "label": "Message fields",
            "description": (
                "\U0001F4CB Comma-separated list of SOURCE field names to concatenate "
                "into the Timesketch 'message' field as "
                "'field: value | field: value | ...' (one per line in detail views). "
                "(e.g. 'user,action,resource'). Original fields are preserved. "
                "Auto-detected if the data has a 'message' column."
            ),
            "type": "text",
            "required": False,
        },
        # -- Source format --
        {
            "name": "delimiter",
            "label": "Source file delimiter",
            "description": (
                "\U0001F537 Delimiter used in the SOURCE file. Use 'auto' for JSON, "
                "JSONL, NDJSON, Parquet, and other structured formats \u2014 "
                "delimiters can be detected from the file extension. "
                "Select a specific delimiter for flat-file formats like CSV or TSV."
            ),
            "type": "select",
            "items": DELIMITER_CHOICES,
            "required": False,
        },
        {
            "name": "ts_datetime_format",
            "label": "Datetime format",
            "description": (
                "\U0001f4c4 Format of the SOURCE timestamp. 'auto' attempts to parse "
                "automatically. Timesketch requires ISO8601 \u2014 values will be "
                "converted before upload."
            ),
            "type": "select",
            "items": DATETIME_FORMAT_CHOICES,
            "required": False,
        },
        {
            "name": "source_timezone",
            "label": "Source data timezone",
            "description": (
                "\U0001F30E Timezone of the timestamps in the SOURCE data. "
                "All timestamps are converted to UTC before upload."
            ),
            "type": "select",
            "items": TIMEZONE_CHOICES,
            "required": False,
        },
        # -- Connection --
        {
            "name": "timesketch_url",
            "label": "Timesketch URL",
            "description": "\U0001F517 Provide the Timesketch URL.",
            "type": "text",
            "required": True,
        },
        {
            "name": "timesketch_username",
            "label": "Timesketch username",
            "description": "\U0001F464 Enter the Timesketch Username.",
            "type": "text",
            "required": True,
            "value": "",
        },
        {
            "name": "timesketch_password",
            "label": "Timesketch password",
            "description": "\U0001F511 Enter the Timesketch Password.",
            "type": "text",
            "required": True,
        },
    ],
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = Logger()
logger = log.get_logger(__name__)


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_) -> None:
    log.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name="OpenSearch/Timesketch Worker",
    )


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def _to_bool(value: Any, default: bool) -> bool:
    """Convert a value to bool, handling string representations."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _safe_str(task_config: dict, key: str, default: str = "") -> str:
    return (task_config.get(key, default) or "").strip()


def _safe_int(task_config: dict, key: str, default: int) -> int:
    raw = _safe_str(task_config, key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _resolve_delimiter(raw: str) -> str:
    """Map a delimiter dropdown label to the actual character(s)."""
    return _DELIMITER_MAP.get(raw, raw)


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


def _validate_index_name(name: str) -> str:
    """Validate an OpenSearch index name and return it lowercased.

    Rejects names that could target system indices, use wildcards, or
    contain characters forbidden by the OpenSearch API.
    """
    if not name:
        raise RuntimeError("index_name is required")
    name = name.strip().lower()
    if name.startswith((".","_")):
        raise RuntimeError(
            f"Index name must not start with '.' or '_': {name!r}"
        )
    if _INDEX_NAME_INVALID_RE.search(name):
        raise RuntimeError(
            f"Index name contains invalid characters: {name!r}"
        )
    if len(name.encode("utf-8")) > 255:
        raise RuntimeError("Index name exceeds 255 bytes")
    return name


def _validate_alias_name(name: str) -> str:
    """Validate an OpenSearch alias name."""
    if not name:
        return ""
    name = name.strip().lower()
    if _INDEX_NAME_INVALID_RE.search(name):
        raise RuntimeError(
            f"Alias name contains invalid characters: {name!r}"
        )
    return name


_BLOCKED_HOSTNAMES = {"169.254.169.254", "metadata.google.internal"}


def _validate_url(url: str, label: str) -> str:
    """Validate that a URL uses an allowed scheme (http/https only).

    Prevents SSRF via file://, gopher://, or other dangerous schemes,
    and blocks cloud metadata endpoints.
    """
    if not url:
        return url
    from urllib.parse import urlparse
    parsed = urlparse(url)
    if parsed.scheme not in _URL_ALLOWED_SCHEMES:
        raise RuntimeError(
            f"{label} must use http:// or https:// (got {parsed.scheme!r})"
        )
    if not parsed.hostname:
        raise RuntimeError(f"{label} is missing a hostname: {url!r}")
    if parsed.hostname in _BLOCKED_HOSTNAMES:
        raise RuntimeError(
            f"{label} points to a blocked address: {parsed.hostname}"
        )
    return url


def _validate_batch_size(value: int) -> int:
    """Clamp batch size to a safe range."""
    return max(1, min(value, MAX_BATCH_SIZE))


def _sanitize_error(e: Exception) -> str:
    """Return a user-safe error string that strips internal URLs/hostnames."""
    msg = str(e)
    # Strip URLs that may reveal internal infrastructure.
    msg = re.sub(r"https?://[^\s'\"]+", "<url>", msg)
    # Truncate overly long messages.
    if len(msg) > 200:
        msg = msg[:200] + "..."
    return f"{type(e).__name__}: {msg}"


# ---------------------------------------------------------------------------
# OpenSearch client
# ---------------------------------------------------------------------------


def _build_os_client(task_config: dict[str, Any]) -> OpenSearch:
    """Build an OpenSearch client from task config and env vars.

    Auth resolution order:
      1. Explicit ``auth_mode`` (basic, apikey, none)
      2. Auto-detect: API key env var → basic-auth env vars → anonymous
    """
    os_url = _validate_url(
        task_config.get("opensearch_url")
        or os.getenv("OPENSEARCH_URL")
        or "https://opensearch:9200",
        "OpenSearch URL",
    )
    username = task_config.get("username") or os.getenv("OPENSEARCH_USERNAME")
    password = task_config.get("password") or os.getenv("OPENSEARCH_PASSWORD")
    api_key = (
        task_config.get("api_key")
        or os.getenv("OPENSEARCH_API_KEY")
        or ""
    )
    disable_verify = _to_bool(
        task_config.get("disable_verify_certs"),
        default=False,
    )
    env_verify = os.getenv("OPENSEARCH_VERIFY_CERTS")
    if env_verify is not None:
        verify_certs = _to_bool(env_verify, default=True)
    else:
        verify_certs = not disable_verify
    auth_mode = (task_config.get("auth_mode") or "").strip().lower()

    client_kwargs: dict[str, Any] = {
        "verify_certs": verify_certs,
        "ssl_show_warn": verify_certs,
        "timeout": 30,
    }

    if auth_mode and auth_mode not in {"none", "basic", "apikey"}:
        raise RuntimeError("auth_mode must be one of: none, basic, apikey")

    if auth_mode == "basic":
        if not (username and password):
            raise RuntimeError(
                "username and password are required for auth_mode=basic"
            )
        client_kwargs["http_auth"] = (username, password)
        return OpenSearch(hosts=[os_url], **client_kwargs)

    if auth_mode == "apikey":
        if not api_key:
            raise RuntimeError(
                "api_key is required for auth_mode=apikey "
                "(set via task config or OPENSEARCH_API_KEY env var)"
            )
        client_kwargs["api_key"] = api_key
        return OpenSearch(hosts=[os_url], **client_kwargs)

    if auth_mode == "none":
        return OpenSearch(hosts=[os_url], **client_kwargs)

    # Auto-detect: API key takes precedence, then basic auth.
    if api_key:
        client_kwargs["api_key"] = api_key
    elif username and password:
        client_kwargs["http_auth"] = (username, password)

    try:
        return OpenSearch(hosts=[os_url], **client_kwargs)
    except Exception as e:
        raise RuntimeError(
            f"Failed to create OpenSearch client for {os_url}: {type(e).__name__}"
        ) from None


# ---------------------------------------------------------------------------
# Format detection and parsers
# ---------------------------------------------------------------------------


def _detect_format(file_path: str) -> str:
    """Detect the data format from the file extension."""
    name = file_path.lower()
    for ext, fmt in FORMAT_MAP_COMPOUND.items():
        if name.endswith(ext):
            return fmt
    for ext, fmt in FORMAT_MAP_SINGLE.items():
        if name.endswith(ext):
            return fmt
    return "jsonl"  # default


def _sanitize_kstrike_field(name: str) -> str:
    """Convert a KStrike header field to a clean OpenSearch field name.

    'RoleGuid (RoleName)' -> 'role_guid_role_name'
    """
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = name.strip("_").lower()
    return name


def _detect_encoding(file_path: str) -> str:
    """Detect file encoding by checking for a BOM."""
    with open(file_path, "rb") as fh:
        raw = fh.read(4)
    if raw[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "utf-16"
    return "utf-8"


def _parse_jsonl(file_path: str) -> Iterator[dict]:
    with open(file_path, encoding=_detect_encoding(file_path)) as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
                yield parsed if isinstance(parsed, dict) else {"value": parsed}
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSON line in %s", file_path)


def _parse_json(file_path: str) -> Iterator[dict]:
    """Parse a JSON file. Uses incremental decoding for arrays to avoid
    loading the entire file into memory."""
    with open(file_path, encoding=_detect_encoding(file_path)) as fh:
        raw = fh.read()
    decoder = json.JSONDecoder()
    raw = raw.lstrip()
    if not raw:
        return
    # Single object or non-array value.
    if not raw.startswith("["):
        data = json.loads(raw)
        yield data if isinstance(data, dict) else {"value": data}
        return
    # Array — decode items one at a time from the raw string to avoid
    # holding the entire parsed list in memory simultaneously.
    idx = 1  # skip opening '['
    length = len(raw)
    while idx < length:
        idx = _skip_ws(raw, idx, length)
        if idx >= length or raw[idx] == "]":
            break
        if raw[idx] == ",":
            idx += 1
            continue
        try:
            item, idx = decoder.raw_decode(raw, idx)
            yield item if isinstance(item, dict) else {"value": item}
        except json.JSONDecodeError:
            logger.warning("Skipping malformed JSON element in %s", file_path)
            break


def _skip_ws(s: str, idx: int, length: int) -> int:
    """Advance past whitespace."""
    while idx < length and s[idx] in " \t\n\r":
        idx += 1
    return idx


def _parse_csv(file_path: str, delimiter: str = ",") -> Iterator[dict]:
    with open(file_path, encoding=_detect_encoding(file_path), newline="") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        for row in reader:
            yield dict(row)


def _parse_tsv(file_path: str) -> Iterator[dict]:
    yield from _parse_csv(file_path, delimiter="\t")


def _parse_kstrike(file_path: str) -> Iterator[dict]:
    with open(file_path, encoding=_detect_encoding(file_path)) as fh:
        header_line = fh.readline().strip()
        if not header_line:
            return
        # KStrike uses || as delimiter; trailing || leaves an empty last field.
        raw_fields = header_line.split("||")
        headers = [_sanitize_kstrike_field(f) for f in raw_fields if f.strip()]

        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            values = stripped.split("||")
            doc = {}
            for i, header in enumerate(headers):
                doc[header] = values[i].strip() if i < len(values) else ""
            # Coerce totalaccesses to int when possible.
            if "totalaccesses" in doc:
                try:
                    doc["totalaccesses"] = int(doc["totalaccesses"])
                except (ValueError, TypeError):
                    pass
            yield doc


def _parse_gzip_jsonl(file_path: str) -> Iterator[dict]:
    with gzip.open(file_path, "rt", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
                yield parsed if isinstance(parsed, dict) else {"value": parsed}
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSON line in %s", file_path)


def _parse_gzip_json(file_path: str) -> Iterator[dict]:
    """Parse a gzipped JSON file with incremental array decoding."""
    with gzip.open(file_path, "rt", encoding="utf-8") as fh:
        raw = fh.read()
    decoder = json.JSONDecoder()
    raw = raw.lstrip()
    if not raw:
        return
    if not raw.startswith("["):
        data = json.loads(raw)
        yield data if isinstance(data, dict) else {"value": data}
        return
    idx = 1
    length = len(raw)
    while idx < length:
        idx = _skip_ws(raw, idx, length)
        if idx >= length or raw[idx] == "]":
            break
        if raw[idx] == ",":
            idx += 1
            continue
        try:
            item, idx = decoder.raw_decode(raw, idx)
            yield item if isinstance(item, dict) else {"value": item}
        except json.JSONDecodeError:
            logger.warning("Skipping malformed JSON element in %s", file_path)
            break


def _parse_parquet(file_path: str) -> Iterator[dict]:
    """Parse a Parquet file, preserving timestamp precision and timezone.

    Arrow timestamp columns are converted to ISO8601 strings so downstream
    code (OpenSearch, ``_to_iso8601``) receives consistent string values
    with full sub-second precision and timezone info intact.
    """
    import pyarrow.parquet as pq
    from datetime import datetime, date, time

    def _normalize(value):
        """Convert Arrow-native types to JSON-safe values."""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, time):
            return value.isoformat()
        return value

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches():
        columns = batch.column_names
        arrays = {col: batch.column(col).to_pylist() for col in columns}
        for i in range(batch.num_rows):
            yield {col: _normalize(arrays[col][i]) for col in columns}


_PARSERS = {
    "jsonl": _parse_jsonl,
    "json": _parse_json,
    "csv": _parse_csv,
    "tsv": _parse_tsv,
    "kstrike": _parse_kstrike,
    "parquet": _parse_parquet,
    "jsonl_gz": _parse_gzip_jsonl,
    "json_gz": _parse_gzip_json,
}


def _parse_delimited(file_path: str, delimiter: str) -> Iterator[dict]:
    """Parse a delimited file with a multi-character or single-char delimiter.

    Falls back to csv.DictReader for single-char delimiters, but handles
    multi-char delimiters (e.g. '||') with manual splitting.
    """
    if len(delimiter) == 1:
        yield from _parse_csv(file_path, delimiter=delimiter)
        return

    encoding = _detect_encoding(file_path)
    with open(file_path, encoding=encoding) as fh:
        header_line = fh.readline().strip()
        if not header_line:
            return
        headers = [h.strip() for h in header_line.split(delimiter)]
        # Drop trailing empty header from trailing delimiter.
        if headers and not headers[-1]:
            headers = headers[:-1]

        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            values = stripped.split(delimiter)
            doc = {}
            for i, header in enumerate(headers):
                doc[header] = values[i].strip() if i < len(values) else ""
            yield doc


def _iter_documents(file_path: str, delimiter: str = "") -> Iterator[dict]:
    """Auto-detect format and yield documents from a file.

    Args:
        delimiter: Override delimiter for CSV/TSV files. Empty or 'auto'
                   uses the format detected from the file extension.
    """
    fmt = _detect_format(file_path)
    if delimiter and delimiter != "auto" and fmt in ("csv", "tsv"):
        yield from _parse_delimited(file_path, delimiter)
        return
    parser = _PARSERS.get(fmt, _parse_jsonl)
    yield from parser(file_path)


# ---------------------------------------------------------------------------
# Mapping support
# ---------------------------------------------------------------------------


def _load_bundled_mapping(name: str) -> dict:
    """Load a bundled mapping JSON file by name."""
    path = (MAPPINGS_DIR / f"{name}.json").resolve()
    # Prevent path traversal — resolved path must stay inside MAPPINGS_DIR.
    if not str(path).startswith(str(MAPPINGS_DIR.resolve())):
        raise RuntimeError(f"Invalid mapping name: {name!r}")
    if not path.is_file():
        available = _list_bundled_mappings()
        raise RuntimeError(
            f"Bundled mapping '{name}' not found. Available: {available}"
        )
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _list_bundled_mappings() -> list[str]:
    """List available bundled mapping names."""
    if not MAPPINGS_DIR.is_dir():
        return []
    return [p.stem for p in MAPPINGS_DIR.glob("*.json")]


def _is_mapping_file(data: dict) -> bool:
    """Check if a parsed JSON dict looks like an OpenSearch mapping definition."""
    if "mappings" in data and isinstance(data["mappings"], dict):
        return "properties" in data["mappings"]
    return "properties" in data and isinstance(data["properties"], dict)


def _resolve_mapping(
    task_config: dict, input_files: list[dict]
) -> Optional[dict]:
    """Resolve the mapping configuration.

    Returns the OpenSearch mapping dict, or None for dynamic mapping.
    """
    mapping_name = _safe_str(task_config, "mapping").lower()

    if not mapping_name or mapping_name == "dynamic":
        return None

    if mapping_name == "custom":
        # Scan input files for a .mapping file (JSON format).
        for i, f in enumerate(input_files):
            fpath = f.get("path", "")
            if not fpath.lower().endswith(".mapping"):
                continue
            try:
                with open(fpath, encoding="utf-8") as fh:
                    data = json.load(fh)
                if _is_mapping_file(data):
                    input_files.pop(i)
                    return data
            except (json.JSONDecodeError, OSError):
                continue
        raise RuntimeError(
            "mapping='custom' but no .mapping file found in inputs. "
            "Upload a .mapping file containing the OpenSearch mapping JSON."
        )

    # Bundled mapping.
    return _load_bundled_mapping(mapping_name)


# ---------------------------------------------------------------------------
# Index management
# ---------------------------------------------------------------------------


def _ensure_index(
    client: OpenSearch,
    index_name: str,
    mapping: Optional[dict],
) -> None:
    """Create the index if it does not exist (append-only)."""
    if client.indices.exists(index=index_name):
        logger.info("Appending to existing index '%s'", index_name)
        return

    body = {}
    if mapping and "mappings" in mapping:
        body["mappings"] = mapping["mappings"]
    client.indices.create(index=index_name, body=body)
    logger.info("Created index '%s'", index_name)


def _apply_alias(client: OpenSearch, index_name: str, alias_name: str) -> None:
    """Associate an alias with the index."""
    client.indices.put_alias(index=index_name, name=alias_name)
    logger.info("Associated alias '%s' with index '%s'", alias_name, index_name)


def _create_index_pattern(
    dashboards_url: str,
    pattern_title: str,
    time_field: str = "",
) -> None:
    """Create an index pattern in OpenSearch Dashboards.

    Uses the Saved Objects API with ``overwrite=true`` for idempotency.
    The *time_field* is set as the default time filter in Discover.

    Args:
        dashboards_url: Base URL of OpenSearch Dashboards (e.g.
            ``http://opensearch-dashboards:5601``).
        pattern_title: Index pattern title — usually the index name or
            a wildcard like ``my-index-*``.
        time_field: Field name to use as the time filter. Empty string
            means no default time field.
    """
    url = (
        f"{dashboards_url.rstrip('/')}"
        f"/api/saved_objects/index-pattern/{pattern_title}"
    )
    headers = {
        "osd-xsrf": "true",
        "Content-Type": "application/json",
    }
    attributes: dict[str, str] = {"title": pattern_title}
    if time_field:
        attributes["timeFieldName"] = time_field

    # Support basic auth for secured Dashboards instances.
    db_user = os.getenv("OPENSEARCH_DASHBOARDS_USERNAME", "")
    db_pass = os.getenv("OPENSEARCH_DASHBOARDS_PASSWORD", "")
    auth = (db_user, db_pass) if db_user and db_pass else None

    resp = requests.post(
        url,
        json={"attributes": attributes},
        headers=headers,
        params={"overwrite": "true"},
        timeout=30,
        auth=auth,
    )
    resp.raise_for_status()
    logger.info(
        "Created index pattern '%s' (time_field=%s) in Dashboards",
        pattern_title,
        time_field or "<none>",
    )


# ---------------------------------------------------------------------------
# Timesketch field mapping
# ---------------------------------------------------------------------------


def _to_iso8601(
    value: str, fmt: str = "", source_tz: str = "UTC"
) -> str:
    """Convert a timestamp string to ISO8601 UTC.

    If the value is already a valid ISO8601 timestamp in UTC, it is returned
    unchanged — no precision is lost by re-formatting.  Microseconds and
    sub-second digits in the source are always preserved.

    Args:
        value: The raw timestamp string.
        fmt: Explicit format hint — 'auto'/''/ISO8601 use dateutil,
             'epoch_seconds'/'epoch_millis' treat value as a numeric epoch,
             otherwise treated as a strptime pattern.
        source_tz: IANA timezone of the source data (e.g. 'US/Eastern').
             Timestamps without timezone info are assumed to be in this zone
             and converted to UTC.

    Returns the original value if parsing fails.
    """
    from datetime import datetime, timezone
    import zoneinfo

    if not value or not isinstance(value, str):
        return str(value) if value else ""

    value = value.strip()

    # Resolve source timezone — default to UTC.
    try:
        src_tz = zoneinfo.ZoneInfo(source_tz) if source_tz and source_tz != "UTC" else timezone.utc
    except (KeyError, zoneinfo.ZoneInfoNotFoundError):
        src_tz = timezone.utc

    fmt_lower = (fmt or "").strip().lower()

    def _fmt_iso(dt):
        """Format a datetime as ISO8601, preserving microsecond precision."""
        # isoformat() preserves microseconds when they are non-zero, but
        # drops them when zero.  Always include at least the seconds.
        return dt.isoformat()

    def _to_utc(dt):
        """Localize naive datetimes to source_tz, then convert to UTC."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=src_tz)
        return _fmt_iso(dt.astimezone(timezone.utc))

    def _is_already_utc_iso(dt):
        """Check if the parsed datetime already has explicit UTC timezone.

        Returns True only when the original string carried timezone info
        that resolved to UTC — meaning we can return it unchanged and
        preserve whatever sub-second precision it had.  Naive timestamps
        always return False because they need timezone info added.
        """
        if dt.tzinfo is None:
            return False
        utc_offset = dt.utcoffset()
        if utc_offset is None:
            return False
        return utc_offset.total_seconds() == 0

    # Epoch formats (always UTC, never already a string we can preserve)
    if fmt_lower == "epoch_seconds":
        try:
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
            return _fmt_iso(dt)
        except (ValueError, OSError):
            return value
    if fmt_lower == "epoch_millis":
        try:
            dt = datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
            return _fmt_iso(dt)
        except (ValueError, OSError):
            return value

    # Explicit strptime pattern
    if fmt_lower and fmt_lower not in ("auto", "iso8601"):
        try:
            dt = datetime.strptime(value, fmt)
            if _is_already_utc_iso(dt):
                return value
            return _to_utc(dt)
        except ValueError:
            pass  # fall through to auto-detect

    # Guard: reject bare numeric values in auto-detect mode.
    # dateutil.parse() interprets numbers like "1904" or "2037" as years,
    # which silently corrupts non-timestamp fields.  Epoch timestamps
    # should use the explicit epoch_seconds/epoch_millis format.
    try:
        float(value)
        logger.warning("Bare numeric value is not a date string: %s", value)
        return value
    except ValueError:
        pass  # Not a number — proceed to dateutil

    # Auto-detect with dateutil
    try:
        dt = dateutil_parser.parse(value)
        if _is_already_utc_iso(dt):
            return value
        return _to_utc(dt)
    except (ValueError, OverflowError):
        logger.warning("Could not parse timestamp: %s", value)
        return value


def _timeline_expand(
    documents: Iterator[dict],
    datetime_fields: list[str],
    datetime_format: str = "",
    source_tz: str = "UTC",
    timestamp_desc: str = "",
) -> Iterator[dict]:
    """Fan out each document into one row per datetime field (timelining).

    For each source document, yields N copies (one per datetime field that
    has a non-null value).  Each copy gets:
      - ``datetime``       — the converted ISO8601 UTC value
      - ``timestamp_desc`` — the field name (or *timestamp_desc* when only
        one field is listed and the caller provides an explicit label)

    All original fields are preserved in every copy.

    If *datetime_fields* has a single entry, behavior is identical to the
    old single-field path — no duplication, just enrichment.
    """
    use_auto_desc = len(datetime_fields) > 1 or not timestamp_desc

    for doc in documents:
        emitted = False
        for field in datetime_fields:
            if field not in doc or doc[field] is None:
                continue
            row = doc.copy()
            row["datetime"] = _to_iso8601(
                str(doc[field]), datetime_format, source_tz,
            )
            row["timestamp_desc"] = field if use_auto_desc else timestamp_desc
            yield row
            emitted = True

        # If none of the listed fields were present, yield the doc once
        # so data is not silently dropped.
        if not emitted:
            yield doc


def _enrich_for_timesketch(
    doc: dict,
    ts_message_field: str,
) -> dict:
    """Add the Timesketch ``message`` field to a document.

    ``datetime`` and ``timestamp_desc`` are handled by ``_timeline_expand``
    upstream.  This function only builds the ``message`` field.

    Original fields are always preserved.
    """
    if ts_message_field:
        if "," in ts_message_field:
            fields = [f.strip() for f in ts_message_field.split(",") if f.strip()]
            parts = [
                f"{f}: {doc[f]}" for f in fields
                if f in doc and doc[f] is not None
            ]
            doc["message"] = ("\n| ").join(parts)
        elif ts_message_field in doc:
            doc["message"] = str(doc[ts_message_field])

    return doc


# ---------------------------------------------------------------------------
# Column preview / auto-detect
# ---------------------------------------------------------------------------

# The three fields Timesketch requires on every event.
TS_REQUIRED_FIELDS = {"message", "datetime", "timestamp_desc"}


def _preview_columns(file_path: str) -> list[str]:
    """Return the column names from the first record of a file.

    This is used to detect whether the source data already contains
    Timesketch-required fields so the worker can auto-map them.
    """
    for doc in _iter_documents(file_path):
        return list(doc.keys())
    return []


def _auto_detect_ts_fields(
    columns: list[str],
    ts_datetime_field: str,
    ts_message_field: str,
    ts_timestamp_desc: str,
) -> tuple[str, str, str]:
    """Fill in missing Timesketch field mappings when the source data already
    contains columns with the required names.

    Returns (ts_datetime_field, ts_message_field, ts_timestamp_desc) with any
    blanks populated from auto-detection.
    """
    col_set = set(columns)

    if not ts_datetime_field and "datetime" in col_set:
        ts_datetime_field = "datetime"
    if not ts_message_field and "message" in col_set:
        ts_message_field = "message"
    if not ts_timestamp_desc and "timestamp_desc" in col_set:
        ts_timestamp_desc = "timestamp_desc"

    return ts_datetime_field, ts_message_field, ts_timestamp_desc


# ---------------------------------------------------------------------------
# Timesketch upload via ImportStreamer
# ---------------------------------------------------------------------------


def _build_ts_client(task_config: dict):
    """Build a TimesketchApi client from task config and env vars."""
    try:
        from timesketch_api_client.client import TimesketchApi
    except ImportError:
        raise RuntimeError(
            "timesketch-import-client is required for upload_target=timesketch. "
            "Install with: pip install timesketch-import-client"
        )

    ts_url = (
        _safe_str(task_config, "timesketch_url")
        or os.getenv("TIMESKETCH_SERVER_URL")
        or ""
    )
    ts_username = (
        _safe_str(task_config, "timesketch_username")
        or os.getenv("TIMESKETCH_USERNAME", "")
    )
    ts_password = (
        _safe_str(task_config, "timesketch_password")
        or os.getenv("TIMESKETCH_PASSWORD", "")
    )

    if not ts_url:
        raise RuntimeError(
            "Timesketch server URL is required (timesketch_url or "
            "TIMESKETCH_SERVER_URL)"
        )
    _validate_url(ts_url, "Timesketch URL")

    try:
        return TimesketchApi(
            host_uri=ts_url,
            username=ts_username,
            password=ts_password,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to connect to Timesketch at {ts_url}: {type(e).__name__}"
        ) from None


def _get_or_create_sketch(ts_client, task_config: dict, workflow_id: str):
    """Resolve or create the target Timesketch sketch."""
    sketch_id = _safe_str(task_config, "sketch_id")
    sketch_name = _safe_str(task_config, "sketch_name")

    if sketch_id:
        try:
            sid = int(sketch_id)
        except (ValueError, TypeError):
            raise RuntimeError(
                f"sketch_id must be a numeric ID, got: {sketch_id!r}"
            )
        return ts_client.get_sketch(sid)
    if sketch_name:
        return ts_client.create_sketch(sketch_name)

    auto_name = f"openrelik-workflow-{workflow_id}"
    for s in ts_client.list_sketches():
        if s.name == auto_name:
            return s
    return ts_client.create_sketch(auto_name)


def _upload_to_timesketch(
    documents: Iterator[dict],
    sketch,
    task_config: dict,
    workflow_id: str,
    timeline_name: str,
    ts_datetime_fields: list[str],
    ts_message_fields: str,
    ts_timestamp_desc: str,
    ts_datetime_format: str = "",
    source_tz: str = "UTC",
    display_name: str = "",
    progress_callback=None,
) -> tuple[int, str]:
    """Stream documents to Timesketch via ImportStreamer.

    Documents are fanned out by ``_timeline_expand`` (one row per datetime
    field), then enriched with ``_enrich_for_timesketch`` (message field)
    before streaming.

    Args:
        sketch: Pre-created Timesketch sketch object (reused across files).
        ts_datetime_fields: List of source field names containing timestamps.
            Each field produces a separate timeline row per source document.
        progress_callback: Optional callable(doc_count, timeline_name)
            invoked every 1000 documents to report progress.

    Returns (doc_count, sketch_url).
    """
    try:
        from timesketch_import_client import importer
    except ImportError:
        raise RuntimeError(
            "timesketch-import-client is required for upload_target=timesketch. "
            "Install with: pip install timesketch-import-client"
        )

    doc_count = 0

    # Fan out documents — one row per datetime field.
    expanded = _timeline_expand(
        documents, ts_datetime_fields,
        ts_datetime_format, source_tz, ts_timestamp_desc,
    ) if ts_datetime_fields else documents

    with importer.ImportStreamer() as streamer:
        streamer.set_sketch(sketch)
        streamer.set_timeline_name(timeline_name)
        streamer.set_provider("openrelik-worker-opensearch")
        streamer.set_upload_context(
            f"Uploaded via openrelik-worker-opensearch (workflow {workflow_id})"
        )
        streamer.set_data_label(display_name)

        # The expand step sets datetime/timestamp_desc per row.
        if ts_datetime_fields:
            streamer.set_datetime_column("datetime")

        for doc in expanded:
            doc = _enrich_for_timesketch(doc, ts_message_fields)
            streamer.add_dict(doc)
            doc_count += 1

            if progress_callback and doc_count % 1000 == 0:
                progress_callback(doc_count, timeline_name)

    public_url = os.getenv("TIMESKETCH_SERVER_EXTERNAL_URL", "")
    ts_url = (
        task_config.get("timesketch_url")
        or os.getenv("TIMESKETCH_SERVER_URL")
        or ""
    )
    base_url = public_url or ts_url
    sketch_url = f"{base_url}/sketch/{sketch.id}/" if base_url else ""

    logger.info(
        "Uploaded %d documents to Timesketch sketch %s", doc_count, sketch.id
    )
    return doc_count, sketch_url


# ---------------------------------------------------------------------------
# Bulk action generator
# ---------------------------------------------------------------------------


def _generate_actions(
    documents: Iterator[dict],
    index_name: str,
    workflow_id: str,
    display_name: str,
    file_path: str,
    datetime_fields: list[str] | None = None,
    datetime_format: str = "",
    source_tz: str = "UTC",
) -> Iterator[dict]:
    """Yield bulk-indexing action dicts for the OpenSearch path.

    When *datetime_fields* is provided, each source document is fanned out
    into one indexed document per datetime field via ``_timeline_expand``
    (timelining).  Each copy gets a ``datetime`` and ``timestamp_desc``
    field, making the index timeline-ready for OpenSearch Dashboards.
    """
    if datetime_fields:
        documents = _timeline_expand(
            documents, datetime_fields, datetime_format, source_tz,
        )

    for doc in documents:
        # OpenRelik metadata
        doc.setdefault("openrelik_workflow_id", workflow_id)
        doc.setdefault("openrelik_source_file", display_name)
        doc.setdefault("openrelik_source_path", file_path)

        yield {
            "_index": index_name,
            "_source": doc,
        }


# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------


@celery.task(bind=True, name=TASK_NAME_EXPORT, metadata=EXPORT_TASK_METADATA)
def export(
    self,
    pipe_result: Optional[str] = None,
    input_files: Optional[List[Dict[str, Any]]] = None,
    output_path: Optional[str] = None,
    workflow_id: Optional[str] = None,
    task_config: Optional[Dict[str, Any]] = None,
) -> str:
    """Index data into OpenSearch."""
    del output_path  # unused
    log.bind(workflow_id=workflow_id)
    logger.debug("Starting %s for workflow %s", TASK_NAME_EXPORT, workflow_id)

    task_config = task_config or {}
    input_files = get_input_files(pipe_result or None, input_files or [])  # type: ignore[arg-type]

    index_name = _validate_index_name(_safe_str(task_config, "index_name"))
    index_alias = _validate_alias_name(_safe_str(task_config, "index_alias"))
    batch_size = _validate_batch_size(
        _safe_int(task_config, "batch_size", DEFAULT_BATCH_SIZE)
    )
    delimiter = _resolve_delimiter(_safe_str(task_config, "delimiter"))
    source_timezone = _safe_str(task_config, "source_timezone") or "UTC"
    datetime_format = _safe_str(task_config, "datetime_format")
    raw_dt_fields = _safe_str(task_config, "datetime_fields")
    datetime_fields = (
        [f.strip() for f in raw_dt_fields.split(",") if f.strip()]
        if raw_dt_fields else None
    )
    dashboards_url = os.environ.get(
        "OPENSEARCH_DASHBOARDS_URL", ""
    )

    # -- Resolve mapping --
    mapping_body = _resolve_mapping(task_config, input_files)
    mapping_label = _safe_str(task_config, "mapping") or "dynamic"

    # -- Build client and ensure index --
    client = _build_os_client(task_config)
    _ensure_index(client, index_name, mapping_body)

    # -- Index documents --
    indexed_total = 0
    failed_total = 0
    file_stats: list[dict] = []

    for input_file in input_files:
        fpath = input_file.get("path", "")
        display_name = input_file.get("display_name", "unknown")
        fmt = _detect_format(fpath)

        file_indexed = 0
        file_failed = 0

        try:
            actions = _generate_actions(
                _iter_documents(fpath, delimiter=delimiter),
                index_name,
                workflow_id or "",
                display_name,
                fpath,
                datetime_fields=datetime_fields,
                datetime_format=datetime_format,
                source_tz=source_timezone,
            )

            batch: list[dict] = []
            for action in actions:
                batch.append(action)
                if len(batch) >= batch_size:
                    success, errors = bulk(
                        client, batch, raise_on_error=False
                    )
                    file_indexed += success
                    file_failed += len(errors)
                    batch = []

                    self.send_event(
                        "task-progress",
                        data={
                            "indexed_documents": indexed_total + file_indexed,
                            "current_file": display_name,
                            "index_name": index_name,
                        },
                    )

            # Flush remaining batch.
            if batch:
                success, errors = bulk(client, batch, raise_on_error=False)
                file_indexed += success
                file_failed += len(errors)

        except Exception as e:
            logger.error("Failed to process %s: %s", display_name, e)
            file_stats.append({
                "file": display_name,
                "format": fmt,
                "documents": 0,
                "status": f"Error: {_sanitize_error(e)}",
            })
            continue

        indexed_total += file_indexed
        failed_total += file_failed
        file_stats.append({
            "file": display_name,
            "format": fmt,
            "documents": file_indexed,
            "status": f"Indexed ({file_failed} failed)" if file_failed else "Indexed",
        })

    # -- Apply alias --
    if index_alias:
        try:
            _apply_alias(client, index_name, index_alias)
        except Exception as e:
            logger.error("Failed to apply alias '%s': %s", index_alias, e)

    # -- Create index pattern(s) in Dashboards --
    create_patterns = _to_bool(task_config.get("create_index_pattern"), False)
    patterns_created: list[str] = []
    patterns_failed: list[str] = []
    if create_patterns and dashboards_url:
        time_field = "datetime" if datetime_fields else ""
        for pattern_name in [index_name] + ([index_alias] if index_alias else []):
            try:
                _create_index_pattern(dashboards_url, pattern_name, time_field)
                patterns_created.append(pattern_name)
            except Exception as e:
                logger.error(
                    "Failed to create index pattern '%s': %s", pattern_name, e
                )
                patterns_failed.append(f"{pattern_name}: {_sanitize_error(e)}")

    # -- Build report --
    report = Report("OpenSearch Export Results")
    report.priority = Priority.INFO

    summary_parts = [f"{indexed_total} documents indexed into '{index_name}'"]
    if failed_total:
        summary_parts.append(f"{failed_total} failed")
    skipped = sum(1 for s in file_stats if s["status"].startswith("Error"))
    if skipped:
        summary_parts.append(f"{skipped} file(s) skipped")
    report.summary = ". ".join(summary_parts) + "."

    section = report.add_section()
    section.add_header("Files")

    table = MarkdownTable(["File", "Format", "Documents", "Status"])
    for stat in file_stats:
        table.add_row([
            stat["file"], stat["format"],
            str(stat["documents"]), stat["status"],
        ])
    section.add_table(table)

    if mapping_label != "dynamic":
        section.add_header("Mapping", level=3)
        section.add_paragraph(f"Applied mapping: {mapping_label}")

    if index_alias:
        section.add_header("Alias", level=3)
        section.add_paragraph(f"Index associated with alias: {index_alias}")

    section.add_header("Index Patterns", level=3)
    if patterns_created:
        time_note = " (time field: datetime)" if datetime_fields else ""
        for p in patterns_created:
            section.add_paragraph(
                f"Created index pattern '{p}'{time_note} in Dashboards"
            )
    if patterns_failed:
        for pf in patterns_failed:
            section.add_paragraph(f"Failed to create index pattern: {pf}")
    if not create_patterns:
        pattern_names = [index_name] + ([index_alias] if index_alias else [])
        section.add_paragraph(
            "Index pattern creation was not requested. "
            f"You can manually create patterns in OpenSearch Dashboards for: "
            f"{', '.join(pattern_names)}"
        )
    elif create_patterns and not dashboards_url:
        section.add_paragraph(
            "Index pattern creation was requested but "
            "OPENSEARCH_DASHBOARDS_URL is not configured."
        )

    meta = {
        "index_name": index_name,
        "indexed_documents": indexed_total,
        "failed_documents": failed_total,
    }
    if index_alias:
        meta["index_alias"] = index_alias
    if patterns_created:
        meta["index_patterns"] = patterns_created

    return create_task_result(
        output_files=[],
        workflow_id=workflow_id or "",
        command="opensearch.index",
        meta=meta,
        task_report=report.to_dict(),
    )


# ---------------------------------------------------------------------------
# Timesketch upload task
# ---------------------------------------------------------------------------


@celery.task(bind=True, name=TASK_NAME_TIMESKETCH, metadata=TIMESKETCH_TASK_METADATA)
def timesketch_upload(
    self,
    pipe_result: Optional[str] = None,
    input_files: Optional[List[Dict[str, Any]]] = None,
    output_path: Optional[str] = None,
    workflow_id: Optional[str] = None,
    task_config: Optional[Dict[str, Any]] = None,
) -> str:
    """Upload data to Timesketch with field mapping and sketch registration."""
    del output_path  # unused
    log.bind(workflow_id=workflow_id)
    logger.debug("Starting %s for workflow %s", TASK_NAME_TIMESKETCH, workflow_id)

    task_config = task_config or {}
    input_files = get_input_files(pipe_result or None, input_files or [])  # type: ignore[arg-type]

    delimiter = _resolve_delimiter(_safe_str(task_config, "delimiter"))
    ts_datetime_format = _safe_str(task_config, "ts_datetime_format")
    source_timezone = _safe_str(task_config, "source_timezone") or "UTC"

    # Timesketch field mapping — user config → auto-detect.
    raw_ts_datetime = _safe_str(task_config, "ts_datetime_field")
    ts_message_fields = _safe_str(task_config, "ts_message_fields")

    # Auto-detect from source columns.
    if input_files:
        first_path = input_files[0].get("path", "")
        if first_path:
            columns = _preview_columns(first_path)
            raw_ts_datetime, ts_message_fields, _ = (
                _auto_detect_ts_fields(
                    columns,
                    raw_ts_datetime,
                    ts_message_fields,
                    "",
                )
            )

    # Parse comma-separated datetime fields into a list.
    ts_datetime_fields = [
        f.strip() for f in raw_ts_datetime.split(",") if f.strip()
    ] if raw_ts_datetime else []

    # -- Build Timesketch client and sketch once for all files --
    ts_client = _build_ts_client(task_config)
    sketch = _get_or_create_sketch(ts_client, task_config, workflow_id or "")

    # -- Upload files --
    uploaded_total = 0
    file_stats: list[dict] = []
    sketch_url = ""

    def _on_progress(count, tl_name):
        self.send_event(
            "task-progress",
            data={
                "uploaded_documents": uploaded_total + count,
                "current_file": tl_name,
            },
        )

    for input_file in input_files:
        fpath = input_file.get("path", "")
        display_name = input_file.get("display_name", "unknown")
        fmt = _detect_format(fpath)
        tl_name_cfg = _safe_str(task_config, "timeline_name")
        merge = _to_bool(task_config.get("merge_timelines"), False)
        if merge and tl_name_cfg:
            timeline_name = tl_name_cfg
        elif tl_name_cfg:
            timeline_name = f"{tl_name_cfg} - {display_name}"
        else:
            timeline_name = display_name

        try:
            doc_count, sketch_url = _upload_to_timesketch(
                _iter_documents(fpath, delimiter=delimiter),
                sketch,
                task_config,
                workflow_id or "",
                timeline_name,
                ts_datetime_fields,
                ts_message_fields,
                "",
                ts_datetime_format,
                source_timezone,
                display_name=display_name,
                progress_callback=_on_progress,
            )
            uploaded_total += doc_count
            file_stats.append({
                "file": display_name,
                "format": fmt,
                "documents": doc_count,
                "status": "Uploaded",
            })
        except Exception as e:
            logger.error("Failed to upload %s to Timesketch: %s", display_name, e)
            file_stats.append({
                "file": display_name,
                "format": fmt,
                "documents": 0,
                "status": f"Error: {_sanitize_error(e)}",
            })

    # -- Build report --
    report = Report("Timesketch Upload Results")
    report.priority = Priority.INFO

    summary_parts = [f"{uploaded_total} documents uploaded to Timesketch"]
    skipped = sum(1 for s in file_stats if s["status"].startswith("Error"))
    if skipped:
        summary_parts.append(f"{skipped} file(s) skipped")
    report.summary = ". ".join(summary_parts) + "."

    section = report.add_section()
    section.add_header("Files")
    table = MarkdownTable(["File", "Format", "Documents", "Status"])
    for stat in file_stats:
        table.add_row([
            stat["file"], stat["format"],
            str(stat["documents"]), stat["status"],
        ])
    section.add_table(table)

    if sketch_url:
        section.add_header("Timesketch", level=3)
        section.add_paragraph(f"Sketch: {sketch_url}")

    section.add_header("Field Mapping", level=3)
    section.add_paragraph(
        f"datetime ← {', '.join(ts_datetime_fields) or '(auto)'}, "
        f"message ← {ts_message_fields or '(auto)'}, "
        f"timestamp_desc ← (auto-generated from field name)"
    )

    meta: dict[str, Any] = {
        "uploaded_documents": uploaded_total,
    }
    if sketch_url:
        meta["sketch_url"] = sketch_url

    return create_task_result(
        output_files=[],
        workflow_id=workflow_id or "",
        command="timesketch.upload",
        meta=meta,
        task_report=report.to_dict(),
    )

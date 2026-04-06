"""Tests for the OpenSearch export and Timesketch upload workers."""

import gzip
import json
from unittest import mock

import pytest

from src import tasks


# ---------------------------------------------------------------------------
# _to_bool
# ---------------------------------------------------------------------------


def test_to_bool_defaults():
    assert tasks._to_bool(None, True) is True
    assert tasks._to_bool(None, False) is False


def test_to_bool_strings():
    assert tasks._to_bool("true", False) is True
    assert tasks._to_bool("false", True) is False
    assert tasks._to_bool("1", False) is True
    assert tasks._to_bool("0", True) is False


# ---------------------------------------------------------------------------
# _build_os_client
# ---------------------------------------------------------------------------


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_no_auth(mock_os):
    with mock.patch.dict("os.environ", {}, clear=True):
        tasks._build_os_client({"opensearch_url": "https://localhost:9200"})

    mock_os.assert_called_once_with(
        hosts=["https://localhost:9200"],
        verify_certs=True,
        ssl_show_warn=True, timeout=30,
    )


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_basic_auth(mock_os):
    with mock.patch.dict("os.environ", {}, clear=True):
        tasks._build_os_client({
            "opensearch_url": "https://os:9200",
            "auth_mode": "basic",
            "username": "admin",
            "password": "secret",
        })

    mock_os.assert_called_once_with(
        hosts=["https://os:9200"],
        verify_certs=True,
        ssl_show_warn=True, timeout=30,
        http_auth=("admin", "secret"),
    )


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_env_vars(mock_os):
    with mock.patch.dict("os.environ", {
        "OPENSEARCH_URL": "https://env-os:9200",
        "OPENSEARCH_USERNAME": "envuser",
        "OPENSEARCH_PASSWORD": "envpass",
    }, clear=True):
        tasks._build_os_client({})

    mock_os.assert_called_once_with(
        hosts=["https://env-os:9200"],
        verify_certs=True,
        ssl_show_warn=True, timeout=30,
        http_auth=("envuser", "envpass"),
    )


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_task_config_overrides_env(mock_os):
    with mock.patch.dict("os.environ", {
        "OPENSEARCH_URL": "https://env:9200",
        "OPENSEARCH_USERNAME": "envuser",
        "OPENSEARCH_PASSWORD": "envpass",
    }, clear=True):
        tasks._build_os_client({
            "opensearch_url": "https://override:9200",
            "auth_mode": "basic",
            "username": "override_user",
            "password": "override_pass",
            "disable_verify_certs": True,
        })

    mock_os.assert_called_once_with(
        hosts=["https://override:9200"],
        verify_certs=False,
        ssl_show_warn=False, timeout=30,
        http_auth=("override_user", "override_pass"),
    )


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_disable_verify_certs_checkbox(mock_os):
    """disable_verify_certs=True should set verify_certs=False."""
    with mock.patch.dict("os.environ", {}, clear=True):
        tasks._build_os_client({
            "opensearch_url": "https://localhost:9200",
            "disable_verify_certs": True,
        })

    mock_os.assert_called_once_with(
        hosts=["https://localhost:9200"],
        verify_certs=False,
        ssl_show_warn=False, timeout=30,
    )


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_env_verify_overrides_checkbox(mock_os):
    """OPENSEARCH_VERIFY_CERTS env var takes precedence over checkbox."""
    with mock.patch.dict("os.environ", {
        "OPENSEARCH_VERIFY_CERTS": "true",
    }, clear=True):
        tasks._build_os_client({
            "opensearch_url": "https://localhost:9200",
            "disable_verify_certs": True,  # would be False, but env overrides
        })

    mock_os.assert_called_once_with(
        hosts=["https://localhost:9200"],
        verify_certs=True,
        ssl_show_warn=True, timeout=30,
    )


def test_build_os_client_invalid_auth_mode():
    with mock.patch.dict("os.environ", {}, clear=True):
        with pytest.raises(RuntimeError, match="auth_mode"):
            tasks._build_os_client({"auth_mode": "oauth"})


def test_build_os_client_basic_missing_creds():
    with mock.patch.dict("os.environ", {}, clear=True):
        with pytest.raises(RuntimeError, match="username and password"):
            tasks._build_os_client({"auth_mode": "basic"})


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_apikey_explicit(mock_os):
    with mock.patch.dict("os.environ", {}, clear=True):
        tasks._build_os_client({
            "opensearch_url": "https://os:9200",
            "auth_mode": "apikey",
            "api_key": "my-api-key-value",
        })
    mock_os.assert_called_once_with(
        hosts=["https://os:9200"],
        verify_certs=True,
        ssl_show_warn=True, timeout=30,
        api_key="my-api-key-value",
    )


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_apikey_env(mock_os):
    with mock.patch.dict("os.environ", {
        "OPENSEARCH_API_KEY": "env-api-key",
    }, clear=True):
        tasks._build_os_client({
            "opensearch_url": "https://os:9200",
        })
    mock_os.assert_called_once_with(
        hosts=["https://os:9200"],
        verify_certs=True,
        ssl_show_warn=True, timeout=30,
        api_key="env-api-key",
    )


def test_build_os_client_apikey_missing():
    with mock.patch.dict("os.environ", {}, clear=True):
        with pytest.raises(RuntimeError, match="api_key is required"):
            tasks._build_os_client({"auth_mode": "apikey"})


@mock.patch("src.tasks.OpenSearch")
def test_build_os_client_apikey_precedence(mock_os):
    """API key should take precedence over basic auth in auto-detect."""
    with mock.patch.dict("os.environ", {
        "OPENSEARCH_API_KEY": "my-key",
        "OPENSEARCH_USERNAME": "user",
        "OPENSEARCH_PASSWORD": "pass",
    }, clear=True):
        tasks._build_os_client({
            "opensearch_url": "https://os:9200",
        })
    call_kwargs = mock_os.call_args[1]
    assert "api_key" in call_kwargs
    assert "http_auth" not in call_kwargs


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("filename,expected", [
    ("data.jsonl", "jsonl"),
    ("data.json", "json"),
    ("data.csv", "csv"),
    ("data.tsv", "tsv"),
    ("data.txt", "kstrike"),
    ("data.parquet", "parquet"),
    ("data.json.gz", "json_gz"),
    ("data.jsonl.gz", "jsonl_gz"),
    ("data.unknown", "jsonl"),  # default
    ("DATA.JSON.GZ", "json_gz"),  # case insensitive
])
def test_detect_format(filename, expected):
    assert tasks._detect_format(filename) == expected


# ---------------------------------------------------------------------------
# KStrike field sanitization
# ---------------------------------------------------------------------------


def test_sanitize_kstrike_field():
    assert tasks._sanitize_kstrike_field("RoleGuid (RoleName)") == "roleguid_rolename"
    assert tasks._sanitize_kstrike_field("TotalAccesses") == "totalaccesses"
    assert tasks._sanitize_kstrike_field("InsertDate") == "insertdate"
    assert tasks._sanitize_kstrike_field("ConvertedAddress (Correlated_HostName(s))") == "convertedaddress_correlated_hostname_s"


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def test_parse_jsonl(tmp_path):
    f = tmp_path / "data.jsonl"
    f.write_text('{"a": 1}\n{"b": 2}\n\n')
    docs = list(tasks._parse_jsonl(str(f)))
    assert docs == [{"a": 1}, {"b": 2}]


def test_parse_jsonl_scalar(tmp_path):
    f = tmp_path / "data.jsonl"
    f.write_text('"hello"\n42\n')
    docs = list(tasks._parse_jsonl(str(f)))
    assert docs == [{"value": "hello"}, {"value": 42}]


def test_parse_json_array(tmp_path):
    f = tmp_path / "data.json"
    f.write_text('[{"a": 1}, {"b": 2}]')
    docs = list(tasks._parse_json(str(f)))
    assert docs == [{"a": 1}, {"b": 2}]


def test_parse_json_object(tmp_path):
    f = tmp_path / "data.json"
    f.write_text('{"a": 1}')
    docs = list(tasks._parse_json(str(f)))
    assert docs == [{"a": 1}]


def test_parse_csv(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("name,value\nfoo,1\nbar,2\n")
    docs = list(tasks._parse_csv(str(f)))
    assert docs == [{"name": "foo", "value": "1"}, {"name": "bar", "value": "2"}]


def test_parse_tsv(tmp_path):
    f = tmp_path / "data.tsv"
    f.write_text("name\tvalue\nfoo\t1\n")
    docs = list(tasks._parse_tsv(str(f)))
    assert docs == [{"name": "foo", "value": "1"}]


def test_parse_csv_utf16(tmp_path):
    """CSV files with UTF-16 BOM should be parsed correctly."""
    f = tmp_path / "data.csv"
    content = "name,value\nfoo,1\nbar,2\n"
    f.write_bytes(b"\xff\xfe" + content.encode("utf-16-le"))
    docs = list(tasks._parse_csv(str(f)))
    assert docs == [{"name": "foo", "value": "1"}, {"name": "bar", "value": "2"}]


def test_detect_encoding_utf8(tmp_path):
    f = tmp_path / "data.txt"
    f.write_text("hello")
    assert tasks._detect_encoding(str(f)) == "utf-8"


def test_detect_encoding_utf16(tmp_path):
    f = tmp_path / "data.txt"
    f.write_bytes(b"\xff\xfe" + "hello".encode("utf-16-le"))
    assert tasks._detect_encoding(str(f)) == "utf-16"


def test_detect_encoding_utf8_bom(tmp_path):
    f = tmp_path / "data.txt"
    f.write_bytes(b"\xef\xbb\xbf" + "hello".encode("utf-8"))
    assert tasks._detect_encoding(str(f)) == "utf-8-sig"


def test_parse_kstrike(tmp_path):
    f = tmp_path / "data.txt"
    header = "RoleGuid (RoleName)||TenantId||TotalAccesses||InsertDate||"
    row = "abc123 (DNS)||tenant1||42||2021-03-15 14:32:45.000000||"
    f.write_text(f"{header}\n{row}\n")
    docs = list(tasks._parse_kstrike(str(f)))
    assert len(docs) == 1
    assert docs[0]["roleguid_rolename"] == "abc123 (DNS)"
    assert docs[0]["tenantid"] == "tenant1"
    assert docs[0]["totalaccesses"] == 42
    assert docs[0]["insertdate"] == "2021-03-15 14:32:45.000000"


def test_parse_gzip_jsonl(tmp_path):
    f = tmp_path / "data.jsonl.gz"
    with gzip.open(str(f), "wt", encoding="utf-8") as fh:
        fh.write('{"a": 1}\n{"b": 2}\n')
    docs = list(tasks._parse_gzip_jsonl(str(f)))
    assert docs == [{"a": 1}, {"b": 2}]


def test_parse_gzip_json(tmp_path):
    f = tmp_path / "data.json.gz"
    with gzip.open(str(f), "wt", encoding="utf-8") as fh:
        json.dump([{"a": 1}], fh)
    docs = list(tasks._parse_gzip_json(str(f)))
    assert docs == [{"a": 1}]


def test_parse_parquet(tmp_path):
    pyarrow = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    table = pyarrow.table({"col1": [1, 2], "col2": ["a", "b"]})
    f = tmp_path / "data.parquet"
    pq.write_table(table, str(f))

    docs = list(tasks._parse_parquet(str(f)))
    assert docs == [{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}]


# ---------------------------------------------------------------------------
# Mapping support
# ---------------------------------------------------------------------------


def test_load_bundled_mapping():
    m = tasks._load_bundled_mapping("kstrike_ual")
    assert "mappings" in m
    assert "properties" in m["mappings"]
    assert "roleguid_rolename" in m["mappings"]["properties"]


def test_load_bundled_mapping_not_found():
    with pytest.raises(RuntimeError, match="not found"):
        tasks._load_bundled_mapping("nonexistent")


def test_list_bundled_mappings():
    mappings = tasks._list_bundled_mappings()
    assert "kstrike_ual" in mappings


def test_is_mapping_file_true():
    assert tasks._is_mapping_file({"mappings": {"properties": {"a": {}}}})


def test_is_mapping_file_false():
    assert not tasks._is_mapping_file({"a": 1, "b": 2})


def test_resolve_mapping_dynamic():
    body = tasks._resolve_mapping({"mapping": "dynamic"}, [])
    assert body is None


def test_resolve_mapping_bundled():
    body = tasks._resolve_mapping({"mapping": "kstrike_ual"}, [])
    assert body is not None
    assert "mappings" in body


def test_resolve_mapping_custom(tmp_path):
    mapping_file = tmp_path / "my_mapping.mapping"
    mapping_data = {"mappings": {"properties": {"field1": {"type": "keyword"}}}}
    mapping_file.write_text(json.dumps(mapping_data))

    input_files = [
        {"path": str(mapping_file), "display_name": "my_mapping.mapping"},
        {"path": "/data/real_data.jsonl", "display_name": "real_data.jsonl"},
    ]
    body = tasks._resolve_mapping({"mapping": "custom"}, input_files)
    assert body == mapping_data
    # Mapping file should be removed from input_files.
    assert len(input_files) == 1
    assert input_files[0]["display_name"] == "real_data.jsonl"


def test_resolve_mapping_custom_not_found():
    with pytest.raises(RuntimeError, match=".mapping"):
        tasks._resolve_mapping({"mapping": "custom"}, [])


# ---------------------------------------------------------------------------
# Index management (append-only)
# ---------------------------------------------------------------------------


def test_ensure_index_exists():
    """Existing index should not be recreated."""
    client = mock.Mock()
    client.indices.exists.return_value = True
    tasks._ensure_index(client, "test-idx", mapping=None)
    client.indices.delete.assert_not_called()
    client.indices.create.assert_not_called()


def test_ensure_index_not_exists():
    """New index should be created with mapping."""
    client = mock.Mock()
    client.indices.exists.return_value = False
    mapping = {"mappings": {"properties": {"a": {"type": "keyword"}}}}
    tasks._ensure_index(client, "test-idx", mapping=mapping)
    client.indices.create.assert_called_once_with(
        index="test-idx",
        body={"mappings": mapping["mappings"]},
    )


def test_ensure_index_not_exists_no_mapping():
    """New index without mapping should be created with empty body."""
    client = mock.Mock()
    client.indices.exists.return_value = False
    tasks._ensure_index(client, "test-idx", mapping=None)
    client.indices.create.assert_called_once_with(
        index="test-idx",
        body={},
    )


def test_apply_alias():
    client = mock.Mock()
    tasks._apply_alias(client, "test-idx", "my-alias")
    client.indices.put_alias.assert_called_once_with(
        index="test-idx", name="my-alias"
    )


# ---------------------------------------------------------------------------
# Column preview / auto-detect
# ---------------------------------------------------------------------------


def test_preview_columns(tmp_path):
    f = tmp_path / "data.jsonl"
    f.write_text('{"alpha": 1, "beta": 2, "gamma": 3}\n{"alpha": 4}\n')
    cols = tasks._preview_columns(str(f))
    assert cols == ["alpha", "beta", "gamma"]


def test_preview_columns_empty_file(tmp_path):
    f = tmp_path / "empty.jsonl"
    f.write_text("")
    assert tasks._preview_columns(str(f)) == []


def test_auto_detect_ts_fields_all_present():
    columns = ["message", "datetime", "timestamp_desc", "host", "user"]
    dt, msg, desc = tasks._auto_detect_ts_fields(columns, "", "", "")
    assert dt == "datetime"
    assert msg == "message"
    assert desc == "timestamp_desc"


def test_auto_detect_ts_fields_none_present():
    columns = ["host", "user", "action"]
    dt, msg, desc = tasks._auto_detect_ts_fields(columns, "", "", "")
    assert dt == ""
    assert msg == ""
    assert desc == ""


def test_auto_detect_ts_fields_partial():
    columns = ["message", "event_time", "host"]
    dt, msg, desc = tasks._auto_detect_ts_fields(columns, "", "", "")
    assert dt == ""  # no "datetime" column
    assert msg == "message"
    assert desc == ""


def test_auto_detect_ts_fields_user_override_preserved():
    """User-provided values should not be overwritten by auto-detect."""
    columns = ["message", "datetime", "timestamp_desc"]
    dt, msg, desc = tasks._auto_detect_ts_fields(
        columns, "my_time", "user,action", "Custom Desc"
    )
    assert dt == "my_time"
    assert msg == "user,action"
    assert desc == "Custom Desc"


# ---------------------------------------------------------------------------
# Timesketch field mapping
# ---------------------------------------------------------------------------


def test_to_iso8601_kstrike_format():
    result = tasks._to_iso8601("2021-03-15 14:32:45.123456")
    assert "2021-03-15" in result
    assert "14:32:45" in result
    assert "+" in result or "Z" in result  # has timezone


def test_to_iso8601_already_valid():
    result = tasks._to_iso8601("2021-03-15T14:32:45+00:00")
    assert result == "2021-03-15T14:32:45+00:00"


def test_to_iso8601_us_date():
    result = tasks._to_iso8601("03/15/2021 2:32 PM")
    assert "2021" in result
    assert "+" in result or "Z" in result


def test_to_iso8601_unparseable():
    result = tasks._to_iso8601("not a date")
    assert result == "not a date"


def test_to_iso8601_empty():
    assert tasks._to_iso8601("") == ""
    assert tasks._to_iso8601(None) == ""


def test_to_iso8601_epoch_seconds():
    result = tasks._to_iso8601("1615820000", fmt="epoch_seconds")
    assert "2021-03-15" in result


def test_to_iso8601_epoch_millis():
    result = tasks._to_iso8601("1615820000000", fmt="epoch_millis")
    assert "2021-03-15" in result


def test_to_iso8601_explicit_format():
    result = tasks._to_iso8601("15/03/2021 14:32:45", fmt="%d/%m/%Y %H:%M:%S")
    assert "2021-03-15" in result
    assert "14:32:45" in result


def test_enrich_for_timesketch_single_field():
    doc = {"user": "admin", "message": "already set"}
    result = tasks._enrich_for_timesketch(doc, "user")
    assert result["message"] == "admin"


def test_enrich_for_timesketch_comma_separated_fields():
    doc = {"user": "admin", "action": "login", "host": "srv1"}
    result = tasks._enrich_for_timesketch(doc, "user,action,host")
    assert result["message"] == "user: admin\n| action: login\n| host: srv1"
    # Original fields are preserved.
    assert result["user"] == "admin"
    assert result["action"] == "login"
    assert result["host"] == "srv1"


def test_enrich_for_timesketch_no_message_field():
    doc = {"a": 1}
    result = tasks._enrich_for_timesketch(doc, "")
    assert "message" not in result


# ---------------------------------------------------------------------------
# _timeline_expand
# ---------------------------------------------------------------------------


def test_timeline_expand_single_field():
    """Single field produces one output doc per input doc."""
    docs = [{"ts": "2021-01-01T00:00:00Z", "msg": "hello"}]
    result = list(tasks._timeline_expand(iter(docs), ["ts"]))
    assert len(result) == 1
    assert result[0]["datetime"] == "2021-01-01T00:00:00Z"
    assert result[0]["timestamp_desc"] == "ts"
    assert result[0]["msg"] == "hello"


def test_timeline_expand_single_field_explicit_desc():
    """Single field with explicit timestamp_desc uses the provided label."""
    docs = [{"ts": "2021-01-01T00:00:00Z"}]
    result = list(tasks._timeline_expand(
        iter(docs), ["ts"], timestamp_desc="Last Access",
    ))
    assert result[0]["timestamp_desc"] == "Last Access"


def test_timeline_expand_multi_field_fan_out():
    """Multiple fields fan out one source doc into N rows."""
    docs = [{"created": "2021-01-01T00:00:00Z", "modified": "2021-06-15T12:00:00Z", "name": "f.txt"}]
    result = list(tasks._timeline_expand(iter(docs), ["created", "modified"]))
    assert len(result) == 2
    assert result[0]["datetime"] == "2021-01-01T00:00:00Z"
    assert result[0]["timestamp_desc"] == "created"
    assert result[0]["name"] == "f.txt"
    assert result[1]["datetime"] == "2021-06-15T12:00:00Z"
    assert result[1]["timestamp_desc"] == "modified"
    assert result[1]["name"] == "f.txt"


def test_timeline_expand_multi_field_ignores_explicit_desc():
    """Multiple fields auto-generate timestamp_desc, ignoring explicit value."""
    docs = [{"a": "2021-01-01T00:00:00Z", "b": "2021-06-01T00:00:00Z"}]
    result = list(tasks._timeline_expand(
        iter(docs), ["a", "b"], timestamp_desc="Ignored",
    ))
    assert result[0]["timestamp_desc"] == "a"
    assert result[1]["timestamp_desc"] == "b"


def test_timeline_expand_skips_null():
    """Null/missing datetime fields are skipped (no row emitted)."""
    docs = [{"a": "2021-01-01T00:00:00Z", "b": None, "c": "hello"}]
    result = list(tasks._timeline_expand(iter(docs), ["a", "b"]))
    assert len(result) == 1
    assert result[0]["timestamp_desc"] == "a"


def test_timeline_expand_no_match_passthrough():
    """If no listed fields exist in the doc, yield once so data isn't lost."""
    docs = [{"other": "value"}]
    result = list(tasks._timeline_expand(iter(docs), ["missing_field"]))
    assert len(result) == 1
    assert result[0] == {"other": "value"}


def test_timeline_expand_timezone_conversion():
    """Source timezone should be applied during expansion."""
    docs = [{"ts": "2021-03-15 14:00:00"}]
    result = list(tasks._timeline_expand(
        iter(docs), ["ts"], source_tz="US/Eastern",
    ))
    assert "18:00:00" in result[0]["datetime"]


def test_timeline_expand_preserves_originals():
    """Original timestamp fields are preserved alongside datetime."""
    docs = [{"created": "2021-01-01T00:00:00Z", "modified": "2021-06-15T12:00:00Z"}]
    result = list(tasks._timeline_expand(iter(docs), ["created", "modified"]))
    # Both rows should have both original fields
    for row in result:
        assert "created" in row
        assert "modified" in row


# ---------------------------------------------------------------------------
# Timesketch client helpers
# ---------------------------------------------------------------------------


def test_build_ts_client_missing_url():
    mock_ts_module = mock.MagicMock()
    with mock.patch.dict("sys.modules", {
        "timesketch_api_client": mock_ts_module,
        "timesketch_api_client.client": mock_ts_module.client,
    }):
        with mock.patch.dict("os.environ", {}, clear=True):
            with pytest.raises(RuntimeError, match="Timesketch server URL"):
                tasks._build_ts_client({})


@mock.patch("src.tasks._build_ts_client")
def test_get_or_create_sketch_by_id(mock_client):
    mock_ts = mock.Mock()
    mock_client.return_value = mock_ts
    sketch = mock.Mock()
    mock_ts.get_sketch.return_value = sketch

    result = tasks._get_or_create_sketch(mock_ts, {"sketch_id": "42"}, "wf-1")
    mock_ts.get_sketch.assert_called_once_with(42)
    assert result is sketch


@mock.patch("src.tasks._build_ts_client")
def test_get_or_create_sketch_by_name(mock_client):
    mock_ts = mock.Mock()
    mock_client.return_value = mock_ts
    sketch = mock.Mock()
    mock_ts.create_sketch.return_value = sketch

    result = tasks._get_or_create_sketch(mock_ts, {"sketch_name": "My Sketch"}, "wf-1")
    mock_ts.create_sketch.assert_called_once_with("My Sketch")
    assert result is sketch


@mock.patch("src.tasks._build_ts_client")
def test_get_or_create_sketch_auto_name(mock_client):
    mock_ts = mock.Mock()
    mock_client.return_value = mock_ts
    mock_ts.list_sketches.return_value = []
    sketch = mock.Mock()
    mock_ts.create_sketch.return_value = sketch

    result = tasks._get_or_create_sketch(mock_ts, {}, "wf-99")
    mock_ts.create_sketch.assert_called_once_with("openrelik-workflow-wf-99")
    assert result is sketch


# ---------------------------------------------------------------------------
# _upload_to_timesketch
# ---------------------------------------------------------------------------


def test_upload_to_timesketch():
    mock_sketch = mock.Mock()
    mock_sketch.id = 7

    mock_streamer = mock.Mock()
    mock_streamer.__enter__ = mock.Mock(return_value=mock_streamer)
    mock_streamer.__exit__ = mock.Mock(return_value=False)

    # Mock the timesketch_import_client module at sys.modules level since
    # it's not installed in the test environment.
    mock_import_module = mock.MagicMock()
    mock_import_module.importer.ImportStreamer.return_value = mock_streamer

    with mock.patch.dict("sys.modules", {
        "timesketch_import_client": mock_import_module,
        "timesketch_import_client.importer": mock_import_module.importer,
    }):
        with mock.patch.dict("os.environ", {
            "TIMESKETCH_SERVER_URL": "http://ts:5000",
        }, clear=True):
            docs = iter([
                {"event_time": "2021-01-01T00:00:00Z", "user": "a"},
                {"event_time": "2021-01-02T00:00:00Z", "user": "b"},
            ])
            count, url = tasks._upload_to_timesketch(
                docs,
                mock_sketch,
                {"timesketch_url": "http://ts:5000"},
                "wf-1",
                "my-timeline",
                ["event_time"],
                "user",
                "Event Time",
            )

    assert count == 2
    assert "sketch/7" in url
    mock_streamer.set_sketch.assert_called_once_with(mock_sketch)
    mock_streamer.set_timeline_name.assert_called_once_with("my-timeline")
    mock_streamer.set_datetime_column.assert_called_once_with("datetime")
    assert mock_streamer.add_dict.call_count == 2


# ---------------------------------------------------------------------------
# Task metadata
# ---------------------------------------------------------------------------


def test_export_task_metadata():
    assert tasks.EXPORT_TASK_METADATA["display_name"] == "OpenSearch Upload"
    config_names = [c["name"] for c in tasks.EXPORT_TASK_METADATA["task_config"]]
    assert "index_name" in config_names
    assert "disable_verify_certs" in config_names
    assert "delimiter" in config_names
    # mapping should be a select dropdown
    mapping_cfg = next(c for c in tasks.EXPORT_TASK_METADATA["task_config"] if c["name"] == "mapping")
    assert mapping_cfg["type"] == "select"
    assert "dynamic" in mapping_cfg["items"]
    assert "kstrike_ual" in mapping_cfg["items"]


def test_timesketch_task_metadata():
    assert tasks.TIMESKETCH_TASK_METADATA["display_name"] == "Timesketch Upload"
    config_names = [c["name"] for c in tasks.TIMESKETCH_TASK_METADATA["task_config"]]
    assert "ts_datetime_field" in config_names
    assert "ts_datetime_format" in config_names
    assert "ts_message_fields" in config_names
    assert "delimiter" in config_names
    assert "sketch_id" in config_names


# ---------------------------------------------------------------------------
# Export task — OpenSearch indexing
# ---------------------------------------------------------------------------


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks.bulk")
@mock.patch("src.tasks._build_os_client")
@mock.patch("src.tasks.get_input_files")
def test_export_jsonl_success(
    mock_get_files, mock_build_client, mock_bulk, mock_result, tmp_path
):
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"id":"1","msg":"ok"}\n{"id":"2","msg":"ok2"}\n')

    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]

    mock_client = mock.Mock()
    mock_client.indices.exists.return_value = True
    mock_build_client.return_value = mock_client
    mock_bulk.return_value = (2, [])
    mock_result.return_value = "encoded"

    result = tasks.export.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={
            "index_name": "test-index",
        },
    )

    assert result == "encoded"
    mock_bulk.assert_called()
    mock_build_client.assert_called_once()


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks.bulk")
@mock.patch("src.tasks._build_os_client")
@mock.patch("src.tasks.get_input_files")
def test_export_csv_success(
    mock_get_files, mock_build_client, mock_bulk, mock_result, tmp_path
):
    input_file = tmp_path / "data.csv"
    input_file.write_text("name,value\nfoo,1\nbar,2\n")

    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.csv"}
    ]

    mock_client = mock.Mock()
    mock_client.indices.exists.return_value = False
    mock_build_client.return_value = mock_client
    mock_bulk.return_value = (2, [])
    mock_result.return_value = "encoded"

    result = tasks.export.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={"index_name": "csv-index"},
    )

    assert result == "encoded"
    mock_client.indices.create.assert_called_once()


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks.bulk")
@mock.patch("src.tasks._build_os_client")
@mock.patch("src.tasks.get_input_files")
def test_export_with_alias(
    mock_get_files, mock_build_client, mock_bulk, mock_result, tmp_path
):
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')

    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]

    mock_client = mock.Mock()
    mock_client.indices.exists.return_value = True
    mock_build_client.return_value = mock_client
    mock_bulk.return_value = (1, [])
    mock_result.return_value = "encoded"

    tasks.export.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={"index_name": "idx", "index_alias": "my-alias"},
    )

    mock_client.indices.put_alias.assert_called_once_with(
        index="idx", name="my-alias"
    )


@mock.patch("src.tasks.get_input_files")
def test_export_requires_index_name(mock_get_files):
    mock_get_files.return_value = []
    with pytest.raises(RuntimeError, match="index_name"):
        tasks.export.run(task_config={})


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks.bulk")
@mock.patch("src.tasks._build_os_client")
@mock.patch("src.tasks.get_input_files")
def test_export_always_appends(
    mock_get_files, mock_build_client, mock_bulk, mock_result, tmp_path
):
    """Export should always append — never delete an existing index."""
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')
    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]

    mock_client = mock.Mock()
    mock_client.indices.exists.return_value = True
    mock_build_client.return_value = mock_client
    mock_bulk.return_value = (1, [])
    mock_result.return_value = "encoded"

    tasks.export.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={"index_name": "idx"},
    )

    mock_client.indices.delete.assert_not_called()
    mock_client.indices.create.assert_not_called()


# ---------------------------------------------------------------------------
# Index pattern creation
# ---------------------------------------------------------------------------


@mock.patch("src.tasks.requests")
def test_create_index_pattern_with_time_field(mock_requests):
    mock_resp = mock.Mock()
    mock_resp.raise_for_status = mock.Mock()
    mock_requests.post.return_value = mock_resp

    tasks._create_index_pattern(
        "http://dashboards:5601", "my-index", time_field="datetime"
    )

    mock_requests.post.assert_called_once()
    call_kwargs = mock_requests.post.call_args
    assert call_kwargs.kwargs["json"]["attributes"]["title"] == "my-index"
    assert call_kwargs.kwargs["json"]["attributes"]["timeFieldName"] == "datetime"
    assert call_kwargs.kwargs["headers"]["osd-xsrf"] == "true"
    assert call_kwargs.kwargs["params"] == {"overwrite": "true"}
    assert "my-index" in call_kwargs.args[0]


@mock.patch("src.tasks.requests")
def test_create_index_pattern_without_time_field(mock_requests):
    mock_resp = mock.Mock()
    mock_resp.raise_for_status = mock.Mock()
    mock_requests.post.return_value = mock_resp

    tasks._create_index_pattern(
        "http://dashboards:5601", "my-index", time_field=""
    )

    payload = mock_requests.post.call_args.kwargs["json"]["attributes"]
    assert "timeFieldName" not in payload


@mock.patch("src.tasks.requests")
def test_create_index_pattern_trailing_slash_url(mock_requests):
    mock_resp = mock.Mock()
    mock_resp.raise_for_status = mock.Mock()
    mock_requests.post.return_value = mock_resp

    tasks._create_index_pattern(
        "http://dashboards:5601/", "my-index"
    )

    url = mock_requests.post.call_args.args[0]
    assert "//" not in url.replace("http://", "")


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._create_index_pattern")
@mock.patch("src.tasks.bulk")
@mock.patch("src.tasks._build_os_client")
@mock.patch("src.tasks.get_input_files")
def test_export_creates_index_pattern_with_timelining(
    mock_get_files, mock_build_client, mock_bulk, mock_create_pattern,
    mock_result, tmp_path, monkeypatch,
):
    """When OPENSEARCH_DASHBOARDS_URL is set and datetime_fields configured,
    create index pattern with time_field='datetime'."""
    monkeypatch.setenv("OPENSEARCH_DASHBOARDS_URL", "http://dashboards:5601")
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"ts": "2021-01-01T00:00:00Z", "msg": "hi"}\n')
    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_client = mock.Mock()
    mock_client.indices.exists.return_value = True
    mock_build_client.return_value = mock_client
    mock_bulk.return_value = (1, [])
    mock_result.return_value = "encoded"

    tasks.export.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={
            "index_name": "idx",
            "datetime_fields": "ts",
            "create_index_pattern": True,
        },
    )

    mock_create_pattern.assert_called_once_with(
        "http://dashboards:5601", "idx", "datetime"
    )


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._create_index_pattern")
@mock.patch("src.tasks.bulk")
@mock.patch("src.tasks._build_os_client")
@mock.patch("src.tasks.get_input_files")
def test_export_creates_index_pattern_without_timelining(
    mock_get_files, mock_build_client, mock_bulk, mock_create_pattern,
    mock_result, tmp_path, monkeypatch,
):
    """When OPENSEARCH_DASHBOARDS_URL is set but no datetime_fields, create
    index pattern without a time field."""
    monkeypatch.setenv("OPENSEARCH_DASHBOARDS_URL", "http://dashboards:5601")
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')
    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_client = mock.Mock()
    mock_client.indices.exists.return_value = True
    mock_build_client.return_value = mock_client
    mock_bulk.return_value = (1, [])
    mock_result.return_value = "encoded"

    tasks.export.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={
            "index_name": "idx",
            "create_index_pattern": True,
        },
    )

    mock_create_pattern.assert_called_once_with(
        "http://dashboards:5601", "idx", ""
    )


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._create_index_pattern")
@mock.patch("src.tasks.bulk")
@mock.patch("src.tasks._build_os_client")
@mock.patch("src.tasks.get_input_files")
def test_export_creates_index_pattern_for_alias(
    mock_get_files, mock_build_client, mock_bulk, mock_create_pattern,
    mock_result, tmp_path, monkeypatch,
):
    """When an alias is configured, create index patterns for both the
    index name and the alias."""
    monkeypatch.setenv("OPENSEARCH_DASHBOARDS_URL", "http://dashboards:5601")
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')
    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_client = mock.Mock()
    mock_client.indices.exists.return_value = True
    mock_build_client.return_value = mock_client
    mock_bulk.return_value = (1, [])
    mock_result.return_value = "encoded"

    tasks.export.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={
            "index_name": "idx",
            "index_alias": "my-alias",
            "create_index_pattern": True,
        },
    )

    assert mock_create_pattern.call_count == 2
    mock_create_pattern.assert_any_call("http://dashboards:5601", "idx", "")
    mock_create_pattern.assert_any_call("http://dashboards:5601", "my-alias", "")


@mock.patch("src.tasks._create_index_pattern")
@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks.bulk")
@mock.patch("src.tasks._build_os_client")
@mock.patch("src.tasks.get_input_files")
def test_export_skips_index_pattern_when_no_dashboards_url(
    mock_get_files, mock_build_client, mock_bulk, mock_result,
    mock_create_pattern, tmp_path,
):
    """No dashboards_url means no index pattern creation."""
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')
    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_client = mock.Mock()
    mock_client.indices.exists.return_value = True
    mock_build_client.return_value = mock_client
    mock_bulk.return_value = (1, [])
    mock_result.return_value = "encoded"

    tasks.export.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={"index_name": "idx"},
    )

    mock_create_pattern.assert_not_called()


# ---------------------------------------------------------------------------
# Timesketch upload task
# ---------------------------------------------------------------------------


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_basic(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    input_file = tmp_path / "data.jsonl"
    input_file.write_text(
        '{"message":"hi","datetime":"2021-01-01T00:00:00Z","timestamp_desc":"Test"}\n'
    )

    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_upload_ts.return_value = (1, "http://ts:5000/sketch/1/")
    mock_result.return_value = "encoded"

    result = tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={},
    )

    assert result == "encoded"
    mock_upload_ts.assert_called_once()
    # Verify auto-detected fields were passed.
    call_args = mock_upload_ts.call_args[0]
    assert call_args[5] == ["datetime"]      # ts_datetime_fields (list)
    assert call_args[6] == "message"         # ts_message_fields
    assert call_args[7] == ""  # ts_timestamp_desc always empty (auto from field name)


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_with_field_config(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    """User-provided field mappings should be passed to the uploader."""
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')

    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_upload_ts.return_value = (1, "http://ts/sketch/1/")
    mock_result.return_value = "encoded"

    tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={
            "ts_datetime_field": "insertdate,lastaccess",
            "ts_message_fields": "authenticatedusername,roleguid_rolename",
        },
    )

    call_args = mock_upload_ts.call_args[0]
    assert call_args[5] == ["insertdate", "lastaccess"]
    assert "authenticatedusername" in call_args[6]
    assert "roleguid_rolename" in call_args[6]
    assert call_args[7] == ""


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_single_datetime_field(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    """Single datetime field — timestamp_desc is always auto-generated from field name."""
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')

    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_upload_ts.return_value = (1, "http://ts/sketch/1/")
    mock_result.return_value = "encoded"

    tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={
            "ts_datetime_field": "my_custom_time",
            "ts_message_fields": "my_custom_msg",
        },
    )

    call_args = mock_upload_ts.call_args[0]
    assert call_args[5] == ["my_custom_time"]
    assert call_args[6] == "my_custom_msg"
    assert call_args[7] == ""  # always empty — auto-generated from field name


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_no_index_name_required(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    """Timesketch upload should not require an index_name."""
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')
    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_upload_ts.return_value = (1, "http://ts/sketch/1/")
    mock_result.return_value = "encoded"

    # No index_name in config — should not raise.
    result = tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={},
    )
    assert result == "encoded"


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_timeline_name_prefix(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    """Timeline name is used as a prefix on each filename by default."""
    f1 = tmp_path / "security.jsonl"
    f2 = tmp_path / "system.jsonl"
    f1.write_text('{"a": 1}\n')
    f2.write_text('{"b": 2}\n')

    mock_get_files.return_value = [
        {"path": str(f1), "display_name": "security.jsonl"},
        {"path": str(f2), "display_name": "system.jsonl"},
    ]
    mock_upload_ts.return_value = (1, "http://ts/sketch/1/")
    mock_result.return_value = "encoded"

    tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={"timeline_name": "case42"},
    )

    assert mock_upload_ts.call_count == 2
    names = [c[0][4] for c in mock_upload_ts.call_args_list]
    assert names == ["case42 - security.jsonl", "case42 - system.jsonl"]


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_merge_timelines(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    """Merge timelines uses the timeline name as-is for all files."""
    f1 = tmp_path / "evtx_part1.jsonl"
    f2 = tmp_path / "evtx_part2.jsonl"
    f1.write_text('{"a": 1}\n')
    f2.write_text('{"b": 2}\n')

    mock_get_files.return_value = [
        {"path": str(f1), "display_name": "evtx_part1.jsonl"},
        {"path": str(f2), "display_name": "evtx_part2.jsonl"},
    ]
    mock_upload_ts.return_value = (1, "http://ts/sketch/1/")
    mock_result.return_value = "encoded"

    tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={
            "timeline_name": "Windows Event Logs",
            "merge_timelines": True,
        },
    )

    assert mock_upload_ts.call_count == 2
    names = [c[0][4] for c in mock_upload_ts.call_args_list]
    assert names == ["Windows Event Logs", "Windows Event Logs"]


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_no_timeline_name_uses_filename(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    """No timeline name configured — falls back to display_name."""
    f1 = tmp_path / "data.jsonl"
    f1.write_text('{"a": 1}\n')

    mock_get_files.return_value = [
        {"path": str(f1), "display_name": "data.jsonl"},
    ]
    mock_upload_ts.return_value = (1, "http://ts/sketch/1/")
    mock_result.return_value = "encoded"

    tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={},
    )

    assert mock_upload_ts.call_args[0][4] == "data.jsonl"


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_error_handling(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    """Errors during upload should be caught and reported in file_stats."""
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')
    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_upload_ts.side_effect = RuntimeError("Connection refused")
    mock_result.return_value = "encoded"

    result = tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={},
    )

    assert result == "encoded"
    # create_task_result should still be called (report includes the error).
    mock_result.assert_called_once()


# ---------------------------------------------------------------------------
# Timezone conversion
# ---------------------------------------------------------------------------


def test_to_iso8601_source_tz_eastern():
    """Naive timestamp in US/Eastern should be converted to UTC."""
    result = tasks._to_iso8601("2021-03-15 14:00:00", source_tz="US/Eastern")
    assert "2021-03-15" in result
    assert "18:00:00" in result  # EDT is UTC-4


def test_to_iso8601_source_tz_utc_default():
    """Default source_tz=UTC should not shift the time."""
    result = tasks._to_iso8601("2021-03-15 14:00:00")
    assert "14:00:00" in result


def test_to_iso8601_source_tz_invalid_falls_back():
    """Invalid timezone should fall back to UTC."""
    result = tasks._to_iso8601("2021-03-15 14:00:00", source_tz="Not/A/Zone")
    assert "14:00:00" in result


def test_to_iso8601_preserves_already_utc():
    """ISO8601 UTC string should be returned unchanged (no precision loss)."""
    original = "2021-03-15T14:32:45.123456+00:00"
    assert tasks._to_iso8601(original) == original


def test_to_iso8601_preserves_utc_z_suffix():
    """Z-suffixed UTC string should be returned unchanged."""
    original = "2021-03-15T14:32:45.123Z"
    assert tasks._to_iso8601(original) == original


def test_to_iso8601_preserves_milliseconds():
    """Millisecond precision in UTC should not be truncated."""
    original = "2021-03-15T14:32:45.999+00:00"
    assert tasks._to_iso8601(original) == original


def test_to_iso8601_naive_utc_gets_tz_added():
    """Naive timestamp with source_tz=UTC should get UTC timezone added."""
    result = tasks._to_iso8601("2021-03-15 14:32:45.123456", source_tz="UTC")
    assert "14:32:45.123456" in result
    assert "+00:00" in result


def test_to_iso8601_non_utc_offset_converts():
    """Timestamp with non-UTC offset must be converted."""
    result = tasks._to_iso8601("2021-03-15T14:00:00-04:00")
    assert "18:00:00" in result


def test_to_iso8601_bare_integer_rejected():
    """Bare integers must NOT be interpreted as years by dateutil."""
    for v in ["1904", "1968", "2037", "437", "100"]:
        result = tasks._to_iso8601(v)
        assert result == v, f"Bare integer {v!r} should pass through unchanged"


def test_to_iso8601_bare_float_rejected():
    """Bare floats must NOT be auto-parsed as dates."""
    assert tasks._to_iso8601("3.14") == "3.14"
    assert tasks._to_iso8601("1615820000") == "1615820000"


def test_to_iso8601_bare_negative_rejected():
    """Negative numbers must NOT be auto-parsed as dates."""
    assert tasks._to_iso8601("-1") == "-1"


def test_to_iso8601_epoch_explicit_still_works():
    """Explicit epoch format should still parse numeric values."""
    result = tasks._to_iso8601("1615820000", fmt="epoch_seconds")
    assert "2021-03-15" in result
    result = tasks._to_iso8601("1615820000000", fmt="epoch_millis")
    assert "2021-03-15" in result


def test_to_iso8601_strips_whitespace():
    """Leading/trailing whitespace should be stripped before parsing."""
    result = tasks._to_iso8601("  9/17/2020 10:56 AM  ")
    assert "2020-09-17" in result
    assert "+00:00" in result


def test_timeline_expand_with_source_tz():
    """source_tz should be passed through to _to_iso8601 during expansion."""
    docs = [{"ts": "2021-03-15 14:00:00", "msg": "test"}]
    result = list(tasks._timeline_expand(
        iter(docs), ["ts"], source_tz="US/Eastern",
    ))
    assert "18:00:00" in result[0]["datetime"]


# ---------------------------------------------------------------------------
# Multi-char delimiter (_parse_delimited)
# ---------------------------------------------------------------------------


def test_parse_delimited_double_pipe(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("name||value||extra\nfoo||1||a\nbar||2||b\n")
    docs = list(tasks._parse_delimited(str(f), "||"))
    assert docs == [
        {"name": "foo", "value": "1", "extra": "a"},
        {"name": "bar", "value": "2", "extra": "b"},
    ]


def test_parse_delimited_single_char(tmp_path):
    """Single-char delimiter should fall back to csv.DictReader."""
    f = tmp_path / "data.csv"
    f.write_text("name;value\nfoo;1\n")
    docs = list(tasks._parse_delimited(str(f), ";"))
    assert docs == [{"name": "foo", "value": "1"}]


def test_iter_documents_double_pipe_delimiter(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("a||b\n1||2\n")
    docs = list(tasks._iter_documents(str(f), delimiter="||"))
    assert docs == [{"a": "1", "b": "2"}]


# ---------------------------------------------------------------------------
# _resolve_delimiter
# ---------------------------------------------------------------------------


def test_resolve_delimiter_named_labels():
    assert tasks._resolve_delimiter("comma (,)") == ","
    assert tasks._resolve_delimiter("tab (\\t)") == "\t"
    assert tasks._resolve_delimiter("pipe (|)") == "|"
    assert tasks._resolve_delimiter("double-pipe (||)") == "||"
    assert tasks._resolve_delimiter("semicolon (;)") == ";"
    assert tasks._resolve_delimiter("auto") == ""


def test_resolve_delimiter_passthrough():
    """Unknown values are passed through unchanged."""
    assert tasks._resolve_delimiter(",") == ","


# ---------------------------------------------------------------------------
# Timesketch upload passes source_timezone
# ---------------------------------------------------------------------------


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks._upload_to_timesketch")
@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
@mock.patch("src.tasks.get_input_files")
def test_timesketch_upload_passes_source_timezone(
    mock_get_files, mock_build_ts, mock_get_sketch,
    mock_upload_ts, mock_result, tmp_path
):
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"a": 1}\n')
    mock_get_files.return_value = [
        {"path": str(input_file), "display_name": "data.jsonl"}
    ]
    mock_upload_ts.return_value = (1, "http://ts/sketch/1/")
    mock_result.return_value = "encoded"

    tasks.timesketch_upload.run(
        pipe_result="p",
        workflow_id="wf-1",
        task_config={"source_timezone": "US/Eastern"},
    )

    call_args = mock_upload_ts.call_args[0]
    # source_tz is the 10th positional arg (index 9)
    assert call_args[9] == "US/Eastern"


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestValidateIndexName:
    def test_valid_name(self):
        assert tasks._validate_index_name("my-index-2024") == "my-index-2024"

    def test_lowercases(self):
        assert tasks._validate_index_name("MyIndex") == "myindex"

    def test_rejects_empty(self):
        with pytest.raises(RuntimeError, match="required"):
            tasks._validate_index_name("")

    def test_rejects_dot_prefix(self):
        with pytest.raises(RuntimeError, match="must not start"):
            tasks._validate_index_name(".opendistro_security")

    def test_rejects_underscore_prefix(self):
        with pytest.raises(RuntimeError, match="must not start"):
            tasks._validate_index_name("_internal")

    def test_rejects_wildcards(self):
        with pytest.raises(RuntimeError, match="invalid characters"):
            tasks._validate_index_name("index-*")

    def test_rejects_spaces(self):
        with pytest.raises(RuntimeError, match="invalid characters"):
            tasks._validate_index_name("my index")

    def test_rejects_slashes(self):
        with pytest.raises(RuntimeError, match="invalid characters"):
            tasks._validate_index_name("foo/bar")


class TestValidateAliasName:
    def test_valid(self):
        assert tasks._validate_alias_name("my-alias") == "my-alias"

    def test_empty_returns_empty(self):
        assert tasks._validate_alias_name("") == ""

    def test_rejects_wildcards(self):
        with pytest.raises(RuntimeError, match="invalid characters"):
            tasks._validate_alias_name("alias-*")


class TestValidateUrl:
    def test_http_allowed(self):
        assert tasks._validate_url("http://opensearch:9200", "test") == "http://opensearch:9200"

    def test_https_allowed(self):
        assert tasks._validate_url("https://os.example.com", "test") == "https://os.example.com"

    def test_rejects_file_scheme(self):
        with pytest.raises(RuntimeError, match="http:// or https://"):
            tasks._validate_url("file:///etc/passwd", "test")

    def test_rejects_no_hostname(self):
        with pytest.raises(RuntimeError, match="missing a hostname"):
            tasks._validate_url("http://", "test")

    def test_empty_passes(self):
        assert tasks._validate_url("", "test") == ""


class TestValidateBatchSize:
    def test_normal(self):
        assert tasks._validate_batch_size(500) == 500

    def test_clamps_zero(self):
        assert tasks._validate_batch_size(0) == 1

    def test_clamps_negative(self):
        assert tasks._validate_batch_size(-10) == 1

    def test_clamps_too_large(self):
        assert tasks._validate_batch_size(999_999) == tasks.MAX_BATCH_SIZE


class TestPathTraversal:
    def test_load_bundled_mapping_rejects_traversal(self):
        with pytest.raises(RuntimeError, match="Invalid mapping name"):
            tasks._load_bundled_mapping("../../etc/passwd")

    def test_load_bundled_mapping_rejects_absolute(self):
        with pytest.raises(RuntimeError, match="Invalid mapping name"):
            tasks._load_bundled_mapping("/etc/passwd")


class TestSketchIdValidation:
    def test_non_numeric_sketch_id(self):
        mock_ts = mock.Mock()
        with pytest.raises(RuntimeError, match="numeric"):
            tasks._get_or_create_sketch(mock_ts, {"sketch_id": "not-a-number"}, "wf-1")


# ---------------------------------------------------------------------------
# Export datetime_fields conversion
# ---------------------------------------------------------------------------


def test_generate_actions_datetime_fields_single():
    """Single datetime field: converts and adds datetime/timestamp_desc."""
    docs = iter([{"ts": "2021-03-15 14:00:00", "msg": "hello"}])
    actions = list(tasks._generate_actions(
        docs, "idx", "wf-1", "file.csv", "/tmp/file.csv",
        datetime_fields=["ts"],
        source_tz="US/Eastern",
    ))
    assert len(actions) == 1
    src = actions[0]["_source"]
    assert "18:00:00" in src["datetime"]  # EDT UTC-4
    assert src["timestamp_desc"] == "ts"
    assert src["msg"] == "hello"


def test_generate_actions_datetime_fields_fan_out():
    """Multiple datetime fields: one action per field per source doc."""
    docs = iter([{"a": "2021-01-01T00:00:00Z", "b": "2021-06-01T00:00:00Z", "x": "val"}])
    actions = list(tasks._generate_actions(
        docs, "idx", "wf-1", "file.csv", "/tmp/file.csv",
        datetime_fields=["a", "b"],
    ))
    assert len(actions) == 2
    assert actions[0]["_source"]["timestamp_desc"] == "a"
    assert actions[1]["_source"]["timestamp_desc"] == "b"
    assert actions[0]["_source"]["x"] == "val"


def test_generate_actions_no_datetime_fields():
    """Without datetime_fields, values should pass through unchanged."""
    docs = iter([{"ts": "not a date", "msg": "hello"}])
    actions = list(tasks._generate_actions(
        docs, "idx", "wf-1", "file.csv", "/tmp/file.csv",
    ))
    assert actions[0]["_source"]["ts"] == "not a date"
    assert "datetime" not in actions[0]["_source"]


def test_export_metadata_has_datetime_fields():
    config_names = [c["name"] for c in tasks.EXPORT_TASK_METADATA["task_config"]]
    assert "datetime_fields" in config_names
    assert "datetime_format" in config_names


# ---------------------------------------------------------------------------
# Parquet timestamp handling
# ---------------------------------------------------------------------------


def test_parse_parquet_timestamps(tmp_path):
    """Parquet datetime columns should be converted to ISO8601 strings."""
    pyarrow = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq
    from datetime import datetime, timezone

    ts = datetime(2021, 3, 15, 14, 32, 45, 123456, tzinfo=timezone.utc)
    table = pyarrow.table({
        "event_time": [ts],
        "msg": ["hello"],
    })
    f = tmp_path / "data.parquet"
    pq.write_table(table, str(f))

    docs = list(tasks._parse_parquet(str(f)))
    assert len(docs) == 1
    assert isinstance(docs[0]["event_time"], str)
    assert "2021-03-15" in docs[0]["event_time"]
    assert "14:32:45.123456" in docs[0]["event_time"]
    assert docs[0]["msg"] == "hello"


# ---------------------------------------------------------------------------
# Timesketch upload progress callback
# ---------------------------------------------------------------------------


@mock.patch("src.tasks._get_or_create_sketch")
@mock.patch("src.tasks._build_ts_client")
def test_upload_to_timesketch_progress_callback(mock_build_ts, mock_get_sketch):
    mock_ts = mock.Mock()
    mock_build_ts.return_value = mock_ts
    mock_sketch = mock.Mock()
    mock_sketch.id = 7
    mock_get_sketch.return_value = mock_sketch

    mock_streamer = mock.Mock()
    mock_streamer.__enter__ = mock.Mock(return_value=mock_streamer)
    mock_streamer.__exit__ = mock.Mock(return_value=False)

    mock_import_module = mock.MagicMock()
    mock_import_module.importer.ImportStreamer.return_value = mock_streamer

    progress_calls = []

    with mock.patch.dict("sys.modules", {
        "timesketch_import_client": mock_import_module,
        "timesketch_import_client.importer": mock_import_module.importer,
    }):
        with mock.patch.dict("os.environ", {
            "TIMESKETCH_SERVER_URL": "http://ts:5000",
        }, clear=True):
            # Generate 2500 docs to trigger progress at 1000 and 2000
            docs = iter([{"a": i} for i in range(2500)])
            count, url = tasks._upload_to_timesketch(
                docs,
                mock_sketch,
                {"timesketch_url": "http://ts:5000"},
                "wf-1", "my-timeline",
                [], "", "",
                progress_callback=lambda c, t: progress_calls.append((c, t)),
            )

    assert count == 2500
    assert len(progress_calls) == 2
    assert progress_calls[0] == (1000, "my-timeline")
    assert progress_calls[1] == (2000, "my-timeline")

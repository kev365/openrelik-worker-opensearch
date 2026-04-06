# Searching in OpenSearch Dashboards

Tips and considerations for querying data uploaded by the OpenRelik OpenSearch worker.

## How text search works

OpenSearch analyzes `text` fields using a **tokenizer** before indexing. The
default standard tokenizer:

- Splits on whitespace and most punctuation (`:`, `|`, `/`, `\n`, etc.)
- Lowercases all tokens
- Discards the punctuation characters themselves

For example, a field value of `C:\Users\admin\Documents\report.pdf` is indexed
as the tokens: `[c, users, admin, documents, report.pdf]`.

This means punctuation characters like `\`, `:`, and `/` are **invisible to
search** — they are stripped during tokenization.

## Searching across all fields

When you type a query without a field prefix in the OpenSearch Dashboards search
bar, it searches across **all fields** in the document by default. This is
controlled by the `index.query.default_field` setting, which defaults to `*`.

```
admin                    # matches "admin" in ANY field in the document
```

This is useful for broad discovery when you don't know which field contains the
value you're looking for.

## Free-text search

Multiple terms are combined with an implicit OR unless you specify otherwise:

```
admin login              # matches docs containing "admin" OR "login"
admin AND login          # matches docs containing both
admin OR login           # explicit OR (same as default)
NOT admin                # excludes docs containing "admin"
```

Because the search spans all fields, `admin AND login` matches a document where
`admin` appears in one field and `login` appears in a completely different field.

## Field-specific queries

Use field-level queries for precise filtering:

```
user:admin
action:login
host:srv1
```

Combine with boolean operators:

```
user:admin AND action:login
user:admin AND NOT action:logout
```

Field-specific queries are more reliable than free-text when you need to ensure
a value appears in a particular field, not just anywhere in the document.

## Phrase search

Wrap multiple words in quotes to match them in order:

```
"admin login"            # matches "admin" followed by "login" in any field
user:"admin local"       # phrase match scoped to the user field
```

Since tokenization strips punctuation, `"users admin"` matches `Users\admin` —
the backslash is not part of the indexed tokens.

## Wildcards

Use `*` and `?` for pattern matching on individual terms:

```
user:adm*                # matches admin, administrator, etc.
host:srv?                # matches srv1, srv2, etc.
```

Wildcards work on the tokenized form, so they apply to individual words, not
across field boundaries.

## Searching numbers

How number searching works depends on how OpenSearch mapped the field type.

### Dynamic mapping and field types

When the worker uses dynamic mapping (the default), OpenSearch auto-detects
field types from the **first document** indexed. If the first value for a field
looks numeric (e.g., `4624`), OpenSearch maps it as `long` or `float`. If it
looks like text, it maps as `text` + `keyword`.

This matters because:

- **Numeric fields** (`long`, `integer`, `float`) support exact match and range
  queries.
- **Text fields** tokenize numbers as string tokens — exact match works, but
  range queries do lexicographic (alphabetical) comparison, not numeric.
- Once a field type is set, it **cannot be changed** without reindexing.

### Exact match

Exact number matching works regardless of field type:

```text
EventID:4624
port:443
LogonType:3
```

### Matching multiple values

Use parentheses to group multiple values for the same field:

```text
EventID:(4624 OR 4625)
EventID:(4624 OR 4625 OR 4672)
```

This is equivalent to writing out each condition separately:

```text
EventID:4624 OR EventID:4625
```

### Range queries

Range queries only work correctly on numeric-typed fields:

```text
EventID:>4624
EventID:>=4624 AND EventID:<=4700
EventID:[4624 TO 4700]
port:[1024 TO 65535]
```

If a field was mapped as `text` (because the first document had a string value
like `"N/A"` for that field), range queries compare alphabetically — `"9"` would
be greater than `"10000"`. Check the field type in the index pattern settings if
range queries return unexpected results.

### Numbers in text fields

When a number appears inside a `text` field (e.g., a log message containing
`"Process ID: 1234"`), the number is tokenized as a string token. You can match
it with free-text or field-specific search:

```text
1234                     # matches "1234" as a token in any field
message:"Process ID 1234"
```

But you cannot do range queries against numbers embedded in text.

## Searching datetime fields

When the worker is configured with datetime fields for timelining, each source
row is expanded into one document per datetime field. The `timestamp_desc` field
identifies which timestamp the row represents:

```
timestamp_desc:"File Created"
timestamp_desc:"Last Access"
```

All timestamps are stored in ISO 8601 UTC. Use range queries in OpenSearch
Dashboards by adjusting the time picker, or with explicit syntax:

```
datetime:[2024-01-01 TO 2024-01-31]
```

## Tips

- **Start broad, then narrow.** Search across all fields first, then add
  field-specific filters to refine.
- **Use field-specific queries for precision.** Searching `admin` matches any
  field; `user:admin` matches only the `user` field.
- **Tokenization affects partial matching.** Searching `adm` will NOT match
  `admin` — use `adm*` with a wildcard instead.
- **Punctuation is stripped.** Searching for literal `\`, `:`, or `|` has no
  effect since they are removed during tokenization.
- **Check field names in the sidebar.** OpenSearch Dashboards shows all
  available fields in the left sidebar of the Discover view — use these for
  targeted queries.

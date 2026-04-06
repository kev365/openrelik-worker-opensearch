# Searching in Timesketch

Tips and considerations for querying data uploaded by the OpenRelik Timesketch
uploader task.

## How text search works

Timesketch stores events in OpenSearch under the hood, so the same tokenization
rules apply. The default standard tokenizer:

- Splits on whitespace and most punctuation (`:`, `|`, `/`, `\n`, etc.)
- Lowercases all tokens
- Discards the punctuation characters themselves

A `message` field stored as:

```text
user: admin
| action: login
| resource: /api/data
```

is indexed as the tokens: `[user, admin, action, login, resource, api, data]`.

The `|`, `:`, and newline characters are for **display only** — they do not
affect search results.

## The message field

The `message` field is the primary text shown in the timeline view. When the
worker builds a message from multiple source fields (e.g.,
`user,action,resource`), it formats them as:

```text
user: admin
| action: login
| resource: /api/data
```

In the timeline list this collapses to a single line:
`user: admin | action: login | resource: /api/data`

In the event detail panel the newlines render for easier reading.

All original source fields are preserved in the event, so you can always query
them directly.

## Searching across all fields

When you type a query without a field prefix in the Timesketch search bar, it
searches across **all fields** in the event — not just `message`. This is because
Timesketch uses OpenSearch underneath, where the default query field is `*` (all
fields).

```text
admin                    # matches "admin" in ANY field — message, user, path, etc.
```

This means `admin AND login` matches an event where `admin` appears in the
`user` field and `login` appears in the `action` field, even if neither word
appears in `message`.

## Free-text search

Multiple terms are combined with an implicit OR unless you specify otherwise:

```text
admin login              # matches events with "admin" OR "login"
admin AND login          # matches events with both (in any fields)
NOT admin                # excludes events containing "admin"
```

## Field-specific queries

Since the worker preserves all original source fields, you can query them
directly for precision:

```text
user:admin
action:login
EventID:4624
```

Combine with booleans:

```text
user:admin AND action:login
user:admin AND NOT action:logout
```

Field-specific queries are more reliable than free-text when you need to ensure
a value appears in a particular field, not just anywhere in the event.

## Phrase search

Use quotes to match words in sequence:

```text
"admin login"
"failed authentication"
```

Because tokenization strips punctuation, `"user admin"` matches `user: admin` in
the message — the colon is not part of the indexed tokens.

## Wildcards

Use `*` for prefix or pattern matching:

```text
user:adm*                # matches admin, administrator, etc.
host:srv*                # matches srv1, srv-web, etc.
```

Wildcards apply to individual tokens. Searching `adm` without a wildcard will
NOT match `admin`.

## Searching numbers

Since Timesketch uses OpenSearch as its backend, number searching depends on how
the field was mapped.

### Exact match

Exact number matching works regardless of how the field was typed:

```text
EventID:4624
LogonType:3
port:443
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

Range queries only work correctly on numeric-typed fields (`long`, `integer`,
`float`):

```text
EventID:>4624
EventID:[4624 TO 4700]
port:[1024 TO 65535]
```

Timesketch manages its own index mappings internally. Most fields from standard
sources (like Plaso) are mapped as expected, but if a range query returns
unexpected results, the field may have been mapped as `text` — where `"9"` sorts
after `"10000"` alphabetically instead of numerically.

### Numbers in the message field

Numbers inside the `message` field are tokenized as string tokens. You can match
them with free-text search:

```text
4624                     # matches "4624" as a token in any field
1234                     # matches in message, EventID, PID, etc.
```

But you cannot do range queries against numbers embedded in text. Use the
original source field for range filtering instead:

```text
EventID:>4624            # works — queries the numeric EventID field
```

## Timeline-specific considerations

### Expanded rows

When multiple datetime fields are configured (e.g., `created_at,modified_at`),
the worker expands each source row into one Timesketch event per field. Each
event has a `timestamp_desc` value derived from the field name:

```text
timestamp_desc:"created at"
timestamp_desc:"modified at"
```

This lets you filter the timeline to specific event types while keeping all
events visible on the same timeline.

### Datetime format

All timestamps are converted to ISO 8601 UTC before upload. Timesketch assumes
UTC — it does not adjust for display timezone. Keep this in mind when correlating
with wall-clock times from other sources.

## Searching source-specific fields

### Plaso / EVTX data

If your source data includes Plaso output fields:

- **`strings`**: The human-readable event message from Windows Event Viewer.
  Highly searchable — contains usernames, SIDs, IPs, and process paths in
  natural language.
- **`xml`**: The raw XML of the event record. Useful as a catch-all but noisy
  for free-text since XML tag names are also tokenized.

```text
strings:"account was successfully logged on"
xml:"TargetUserName"
```

Prefer `strings` for natural-language searches and the original parsed fields
(e.g., `EventID`, `user`) for structured filtering.

## Tips

- **Unqualified search hits all fields.** Typing `admin` without a field prefix
  searches every field in the event, not just `message`.
- **Use field-specific queries for precision.** `user:admin` matches only the
  `user` field; bare `admin` matches any field.
- **Punctuation does not matter in search.** The `|` and `:` in the message
  format are display-only and stripped during tokenization.
- **Wildcards need the `*` character.** Partial words like `adm` will not match
  `admin` — use `adm*` instead.
- **Check available fields.** Click on an event in Timesketch to see all indexed
  fields — these are all queryable with `field:value` syntax.
- **Timestamps are UTC.** Factor in timezone offsets when correlating with local
  time evidence.

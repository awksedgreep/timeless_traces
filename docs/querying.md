# Querying

This guide covers the Elixir query API, all available filters, trace lookup, and query optimization.

## Basic queries

```elixir
# All spans (default limit 100, newest first)
{:ok, result} = TimelessTraces.query()

# Error spans
{:ok, result} = TimelessTraces.query(status: :error)

# Server spans from a specific service
{:ok, result} = TimelessTraces.query(kind: :server, service: "api-gateway")

# Slow spans (> 100ms)
{:ok, result} = TimelessTraces.query(min_duration: 100_000_000)
```

## Result struct

Queries return a `TimelessTraces.Result` struct:

```elixir
%TimelessTraces.Result{
  entries: [%TimelessTraces.Span{}, ...],
  total: 42,      # total matching spans (before pagination)
  limit: 100,     # max entries returned
  offset: 0       # entries skipped
}
```

## Span struct

Each span in `entries` is a `TimelessTraces.Span`:

| Field | Type | Description |
|-------|------|-------------|
| `trace_id` | string | 32-character hex trace ID |
| `span_id` | string | 16-character hex span ID |
| `parent_span_id` | string or nil | Parent span ID (nil for root spans) |
| `name` | string | Operation name (e.g. `"GET /users"`) |
| `kind` | atom | `:internal`, `:server`, `:client`, `:producer`, `:consumer` |
| `start_time` | integer | Start time in nanoseconds |
| `end_time` | integer | End time in nanoseconds |
| `duration_ns` | integer | Duration in nanoseconds |
| `status` | atom | `:ok`, `:error`, `:unset` |
| `status_message` | string or nil | Error description |
| `attributes` | map | Span attributes (string keys) |
| `events` | list | Span events |
| `resource` | map | OTel resource attributes (string keys) |
| `instrumentation_scope` | map or nil | `%{name: "...", version: "..."}` |

## Filters

| Filter | Type | Description |
|--------|------|-------------|
| `:name` | string | Case-insensitive substring match on span name |
| `:service` | string | Exact match on `service.name` in attributes or resource |
| `:kind` | atom | `:internal`, `:server`, `:client`, `:producer`, `:consumer` |
| `:status` | atom | `:ok`, `:error`, `:unset` |
| `:min_duration` | integer | Minimum duration in nanoseconds |
| `:max_duration` | integer | Maximum duration in nanoseconds |
| `:since` | DateTime or integer | Start time lower bound (DateTime or unix nanoseconds) |
| `:until` | DateTime or integer | Start time upper bound (DateTime or unix nanoseconds) |
| `:trace_id` | string | Exact match on trace ID |
| `:attributes` | map | All key/value pairs must match (keys and values compared as strings) |
| `:limit` | integer | Max results (default 100) |
| `:offset` | integer | Skip N results (default 0) |
| `:order` | atom | `:desc` (newest first, default) or `:asc` (oldest first) |

## Examples

### Time range queries

```elixir
# Spans from the last hour
one_hour_ago = DateTime.add(DateTime.utc_now(), -3600)
TimelessTraces.query(since: one_hour_ago)

# Spans in a specific window
TimelessTraces.query(
  since: ~U[2024-01-15 10:00:00Z],
  until: ~U[2024-01-15 11:00:00Z]
)

# Using nanosecond timestamps directly
TimelessTraces.query(since: 1705312200_000_000_000)
```

### Duration queries

```elixir
# Slow spans (> 500ms)
TimelessTraces.query(min_duration: 500_000_000)

# Spans in a duration range (100ms - 1s)
TimelessTraces.query(min_duration: 100_000_000, max_duration: 1_000_000_000)
```

### Attribute queries

```elixir
# Match specific attributes
TimelessTraces.query(attributes: %{"http.method" => "POST", "http.status_code" => "500"})
```

### Pagination

```elixir
# Page 1
{:ok, page1} = TimelessTraces.query(status: :error, limit: 50)

# Page 2
{:ok, page2} = TimelessTraces.query(status: :error, limit: 50, offset: 50)
```

### Combined filters

```elixir
TimelessTraces.query(
  service: "api-gateway",
  kind: :server,
  status: :error,
  min_duration: 100_000_000,
  since: DateTime.add(DateTime.utc_now(), -86400),
  limit: 20,
  order: :asc
)
```

## Trace lookup

Retrieve all spans in a trace, sorted by start time:

```elixir
{:ok, spans} = TimelessTraces.trace("abc123def456789012345678abcdef01")
```

This uses the trace index for fast lookup -- only blocks known to contain spans for that trace are read.

## Service and operation discovery

```elixir
# List all services
{:ok, services} = TimelessTraces.services()
# => {:ok, ["my_app", "api_gateway", "auth_service"]}

# List operations for a service
{:ok, ops} = TimelessTraces.operations("my_app")
# => {:ok, ["GET /users", "POST /orders", "DB query"]}
```

## Query optimization

### Use indexed filters

The following filters leverage the inverted term index, narrowing the set of blocks to read:

- `:service` -- matches `service:<name>` terms
- `:kind` -- matches `kind:<kind>` terms
- `:status` -- matches `status:<status>` terms
- `:name` -- matches `name:<name>` terms
- `:trace_id` -- uses the trace index directly

Time range filters (`:since`, `:until`) narrow blocks by timestamp metadata.

### Avoid full scans

Queries with no filters scan all blocks. On large datasets, always include at least one filter to leverage the index.

### Duration and attribute filters

`:min_duration`, `:max_duration`, and `:attributes` are applied as in-memory filters after block decompression. They don't reduce the number of blocks read. Combine them with indexed filters for best performance.

### Performance benchmarks

Query latency on 500K spans, 500 blocks (avg over 3 runs):

| Query | zstd | OpenZL | Speedup |
|-------|------|--------|---------|
| All spans (limit 100) | 945ms | 442ms | 2.1x |
| status=error | 289ms | 148ms | 2.0x |
| service filter | 318ms | 243ms | 1.3x |
| kind=server | 275ms | 225ms | 1.2x |
| Trace lookup | 5.5ms | 5.7ms | 1.0x |

OpenZL columnar format is faster at query time because columns can be selectively decompressed.

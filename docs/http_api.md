# HTTP API

TimelessTraces includes an optional HTTP interface with OTLP JSON ingest and Jaeger-compatible query endpoints. Enable it in your config:

```elixir
# Port 10428, no auth
config :timeless_traces, http: true

# Custom port with auth
config :timeless_traces, http: [port: 10428, bearer_token: "my-secret-token"]
```

Or add directly to a supervision tree:

```elixir
children = [
  {TimelessTraces.HTTP, port: 10428, bearer_token: "secret"}
]
```

## Endpoints summary

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/insert/opentelemetry/v1/traces` | OTLP JSON trace ingest |
| `GET` | `/select/jaeger/api/services` | List service names |
| `GET` | `/select/jaeger/api/services/:service/operations` | Operations for a service |
| `GET` | `/select/jaeger/api/traces` | Search traces |
| `GET` | `/select/jaeger/api/traces/:trace_id` | Get full trace |
| `GET` | `/health` | Health check (no auth required) |
| `GET` | `/api/v1/flush` | Force buffer flush |
| `POST` | `/api/v1/backup` | Online backup |

## Health check

Always accessible without authentication. Suitable for load balancer health checks.

```bash
curl http://localhost:10428/health
```

Response:

```json
{
  "status": "ok",
  "blocks": 42,
  "spans": 50000,
  "disk_size": 24000000
}
```

## Ingest

### POST /insert/opentelemetry/v1/traces

Accepts OTLP JSON format (the standard OpenTelemetry JSON export format). Maximum body size is 10 MB.

```bash
curl -X POST http://localhost:10428/insert/opentelemetry/v1/traces \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceSpans": [{
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "my-service"}}
        ]
      },
      "scopeSpans": [{
        "scope": {"name": "my-library", "version": "1.0.0"},
        "spans": [{
          "traceId": "abc123def456789012345678abcdef01",
          "spanId": "1234567890abcdef",
          "name": "GET /users",
          "kind": 2,
          "startTimeUnixNano": "1705312200000000000",
          "endTimeUnixNano": "1705312200050000000",
          "status": {"code": 1},
          "attributes": [
            {"key": "http.method", "value": {"stringValue": "GET"}},
            {"key": "http.status_code", "value": {"intValue": 200}}
          ]
        }]
      }]
    }]
  }'
```

**Span kind values:**

| Value | Kind |
|-------|------|
| 1 or `"SPAN_KIND_INTERNAL"` | internal |
| 2 or `"SPAN_KIND_SERVER"` | server |
| 3 or `"SPAN_KIND_CLIENT"` | client |
| 4 or `"SPAN_KIND_PRODUCER"` | producer |
| 5 or `"SPAN_KIND_CONSUMER"` | consumer |

**Status code values:**

| Value | Status |
|-------|--------|
| 0 or `"STATUS_CODE_UNSET"` | unset |
| 1 or `"STATUS_CODE_OK"` | ok |
| 2 or `"STATUS_CODE_ERROR"` | error |

**Attribute value types:**

Attributes use the OTLP key/value format. Supported value types: `stringValue`, `intValue`, `doubleValue`, `boolValue`.

**Timestamps:**

`startTimeUnixNano` and `endTimeUnixNano` are nanosecond unix timestamps, passed as strings or integers.

**Response:**

- `200` with `{"partialSuccess":{}}` on success
- `400` on invalid JSON or missing `resourceSpans`
- `413` if body exceeds 10 MB

## Query (Jaeger-compatible)

The query endpoints use Jaeger-compatible URL paths and response formats. This means you can point Jaeger UI directly at TimelessTraces.

### GET /select/jaeger/api/services

List all distinct service names.

```bash
curl http://localhost:10428/select/jaeger/api/services
```

Response:

```json
{
  "data": ["my-service", "api-gateway", "auth-service"]
}
```

### GET /select/jaeger/api/services/:service/operations

List operations for a specific service.

```bash
curl http://localhost:10428/select/jaeger/api/services/my-service/operations
```

Response:

```json
{
  "data": ["GET /users", "POST /orders", "DB query"]
}
```

### GET /select/jaeger/api/traces

Search traces with filters.

```bash
curl 'http://localhost:10428/select/jaeger/api/traces?service=my-service&operation=GET+/users&limit=20'
```

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `service` | string | Filter by service name |
| `operation` | string | Filter by operation (span name) |
| `start` | integer | Lower time bound (microseconds) |
| `end` | integer | Upper time bound (microseconds) |
| `limit` | integer | Max traces to return |
| `minDuration` | string | Min duration (e.g. `"100ms"`, `"2s"`, `"500us"`) |
| `maxDuration` | string | Max duration (e.g. `"1s"`) |

**Duration format:**

| Suffix | Unit |
|--------|------|
| `ms` | milliseconds |
| `us` | microseconds |
| `s` | seconds |
| (none) | nanoseconds |

**Response:**

Returns traces in Jaeger JSON format:

```json
{
  "data": [
    {
      "traceID": "abc123...",
      "spans": [
        {
          "traceID": "abc123...",
          "spanID": "1234567890abcdef",
          "operationName": "GET /users",
          "references": [],
          "startTime": 1705312200000,
          "duration": 50000,
          "tags": [
            {"key": "span.kind", "type": "string", "value": "server"},
            {"key": "otel.status_code", "type": "string", "value": "OK"}
          ],
          "logs": [],
          "processID": "p1"
        }
      ],
      "processes": {
        "p1": {"serviceName": "my-service", "tags": []}
      }
    }
  ]
}
```

Note: `startTime` and `duration` are in microseconds (Jaeger convention).

### GET /select/jaeger/api/traces/:trace_id

Get all spans for a specific trace.

```bash
curl http://localhost:10428/select/jaeger/api/traces/abc123def456789012345678abcdef01
```

Response format is the same as the search endpoint, with a single trace in the `data` array.

## Flush

### GET /api/v1/flush

Force flush the buffer, writing any pending spans to storage immediately.

```bash
curl http://localhost:10428/api/v1/flush
```

Response:

```json
{"status": "ok"}
```

## Backup

### POST /api/v1/backup

Create a consistent online backup.

```bash
# Backup to a specific directory
curl -X POST http://localhost:10428/api/v1/backup \
  -H 'Content-Type: application/json' \
  -d '{"path": "/tmp/span_backup"}'

# Backup to default location (data_dir/backups/timestamp)
curl -X POST http://localhost:10428/api/v1/backup
```

Response:

```json
{
  "status": "ok",
  "path": "/tmp/span_backup",
  "files": ["index.db", "blocks"],
  "total_bytes": 24000000
}
```

## Authentication

When `bearer_token` is configured, all endpoints except `/health` require authentication:

```bash
# Via Authorization header
curl -H "Authorization: Bearer my-secret-token" \
  'http://localhost:10428/select/jaeger/api/services'

# Via query parameter
curl 'http://localhost:10428/select/jaeger/api/services?token=my-secret-token'
```

Unauthenticated requests receive:
- `401 Unauthorized` if no token is provided
- `403 Forbidden` if the token is incorrect

Token comparison uses `Plug.Crypto.secure_compare` to prevent timing attacks.

## Jaeger UI integration

Point Jaeger UI at TimelessTraces by setting the query service URL:

```bash
# Run Jaeger UI with TimelessTraces as the backend
docker run -d \
  -e QUERY_BASE_PATH=/ \
  -e GRPC_STORAGE_SERVER=host.docker.internal:10428 \
  -p 16686:16686 \
  jaegertracing/jaeger-query:latest
```

Or configure Jaeger UI's JSON API endpoint to point at `http://localhost:10428/select/jaeger`.

## OTLP collector integration

Any OTLP-compatible collector or agent can forward traces to the HTTP ingest endpoint:

### OpenTelemetry Collector

```yaml
exporters:
  otlphttp:
    endpoint: http://localhost:10428/insert/opentelemetry
    tls:
      insecure: true

service:
  pipelines:
    traces:
      exporters: [otlphttp]
```

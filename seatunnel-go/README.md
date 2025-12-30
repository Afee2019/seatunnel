# SeaTunnel Go

A lightweight Go implementation of SeaTunnel CLI and core connectors.

## Overview

SeaTunnel Go is a high-performance, lightweight alternative to the Java-based SeaTunnel CLI. It provides:

- **Fast startup** - <100ms vs 10+ seconds for Java
- **Low memory footprint** - ~20MB vs 500MB+ for Java
- **Single binary deployment** - No JVM required
- **Compatible configuration** - Uses the same HOCON config format

## Features

### Implemented Connectors

**Sources:**
- `FakeSource` - Generate fake/test data with configurable schema

**Sinks:**
- `Console` - Output data to console/log
- `EmailSink` - Send data via email

### CLI Commands

```bash
# Run a job locally
seatunnel local -c config.conf

# Check configuration syntax
seatunnel check -c config.conf

# List available plugins
seatunnel list

# Show version
seatunnel version
```

## Building

```bash
cd seatunnel-go

# Download dependencies
go mod download

# Build
go build -o bin/seatunnel ./cmd/seatunnel

# Or with version info
go build -ldflags "-X main.version=2.3.13 -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o bin/seatunnel ./cmd/seatunnel
```

## Usage

### Basic Example

Create a config file `fake_to_console.conf`:

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    row.num = 10
    schema {
      fields {
        id = "bigint"
        name = "string"
        age = "int"
        score = "double"
      }
    }
  }
}

sink {
  Console {
    log.print.data = true
  }
}
```

Run the job:

```bash
./bin/seatunnel local -c fake_to_console.conf
```

### Email Sink Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 5
  }
}

sink {
  EmailSink {
    email_host = "smtp.gmail.com"
    email_smtp_port = 587
    email_from_address = "sender@example.com"
    email_to_address = "recipient@example.com"
    email_authorization_code = "your-app-password"
    email_message_headline = "SeaTunnel Report"
    email_content_type = "html"
  }
}
```

## Project Structure

```
seatunnel-go/
├── cmd/
│   └── seatunnel/          # CLI entry point
│       ├── main.go         # Main command
│       └── executor.go     # Local executor
│
├── pkg/
│   ├── api/                # Core interfaces
│   │   ├── types.go        # Data types
│   │   ├── row.go          # SeaTunnelRow
│   │   ├── source.go       # Source interface
│   │   ├── sink.go         # Sink interface
│   │   └── factory.go      # Factory interfaces
│   │
│   ├── config/             # Configuration
│   │   └── parser.go       # HOCON parser
│   │
│   ├── connectors/         # Connector implementations
│   │   ├── console/        # Console sink
│   │   ├── email/          # Email sink
│   │   └── fake/           # Fake source
│   │
│   └── registry/           # Plugin registry
│       └── registry.go
│
└── go.mod
```

## Supported Data Types

| Type | Description |
|------|-------------|
| `boolean` | Boolean |
| `tinyint` | 8-bit signed integer |
| `smallint` | 16-bit signed integer |
| `int` | 32-bit signed integer |
| `bigint` | 64-bit signed integer |
| `float` | 32-bit floating point |
| `double` | 64-bit floating point |
| `decimal(p,s)` | Decimal with precision and scale |
| `string` | String/VARCHAR |
| `bytes` | Binary data |
| `date` | Date without time |
| `time` | Time without date |
| `timestamp` | Date and time |
| `array<T>` | Array of type T |
| `map<K,V>` | Map with key type K and value type V |

## Configuration Options

### FakeSource

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `row.num` | long | 10 | Number of rows to generate |
| `split.num` | int | 1 | Number of splits |
| `schema.fields` | map | - | Field definitions |
| `string.length` | int | 50 | Default string length |
| `string.fake.mode` | enum | range | Mode: range, sentence, word, uuid, name, email, phone, address |
| `array.size` | int | 3 | Default array size |
| `map.size` | int | 3 | Default map size |

### Console Sink

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `log.print.data` | boolean | true | Whether to print data |
| `log.print.delay.ms` | int | 0 | Delay between prints |

### Email Sink

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `email_host` | string | Yes | SMTP server hostname |
| `email_smtp_port` | int | No (587) | SMTP port |
| `email_from_address` | string | Yes | Sender email |
| `email_to_address` | string | Yes | Recipients (comma-separated) |
| `email_authorization_code` | string | No | SMTP password |
| `email_message_headline` | string | No | Subject line |
| `email_content_type` | string | No (text) | Content type: text or html |

## Comparison with Java CLI

| Metric | Java CLI | Go CLI |
|--------|----------|--------|
| Startup time | 8-15 sec | <100ms |
| Memory usage | 500MB+ | ~20MB |
| Binary size | 200MB+ (with JRE) | ~20MB |
| Deployment | JAR + JVM | Single binary |
| Dependencies | Maven ecosystem | Go modules |

## Roadmap

### Phase 2: REST Gateway
- HTTP API server
- Prometheus metrics export
- gRPC bridge to Java backend

### Phase 3: More Connectors
- Redis connector
- HTTP connector
- Additional sources/sinks

## License

Apache License 2.0

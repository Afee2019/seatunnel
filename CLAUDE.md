# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache SeaTunnel is a distributed, high-performance data integration tool supporting batch and streaming data synchronization. It supports multiple execution engines: SeaTunnel Zeta Engine (native), Apache Flink, and Apache Spark.

## Build Commands

```bash
# Full build (skip tests)
./mvnw clean install -DskipTests

# Build distribution package
./mvnw clean package -pl seatunnel-dist -am -Dmaven.test.skip=true

# Build specific connector (example: redis)
./mvnw clean package -pl seatunnel-connectors-v2/connector-redis -am -DskipTests -T 1C

# Multi-threaded build
./mvnw -T 1C clean package

# Run unit tests only
./mvnw test

# Run integration tests (E2E tests)
./mvnw verify -DskipUT=true -DskipIT=false

# Code formatting (Spotless)
./mvnw spotless:apply

# Check code style without fixing
./mvnw spotless:check
```

## Test Execution

- Unit tests: Classes ending with `Test.java`, run with `-DskipUT=false`
- Integration tests: Classes ending with `IT.java`, run with `-DskipIT=false`
- E2E tests use TestContainers and require Docker
- Run specific test: `./mvnw test -pl <module> -Dtest=<TestClassName>`
- Examples can be run from `seatunnel-examples` module in IDE (e.g., `SeaTunnelEngineLocalExample`)

## Architecture

### Core Modules

| Module | Purpose |
|--------|---------|
| `seatunnel-api` | Connector V2 API: Source, Sink, Transform interfaces and table/catalog abstractions |
| `seatunnel-connectors-v2` | 60+ connector implementations (JDBC, Kafka, file systems, databases, etc.) |
| `seatunnel-engine` | SeaTunnel Zeta Engine: client, server, storage, serialization components |
| `seatunnel-translation` | Adapters for Flink and Spark engines |
| `seatunnel-transforms-v2` | Data transformation plugins |
| `seatunnel-formats` | Data format handlers (JSON, Avro, Parquet, etc.) |
| `seatunnel-e2e` | End-to-end integration tests |
| `seatunnel-core` | Engine starters (seatunnel-starter, flink-starter, spark-starter) |

### Connector Architecture

Connectors follow the Factory pattern with SPI discovery:

1. **Factory classes** (`TableSourceFactory`, `TableSinkFactory`): SPI entry points that create source/sink instances
2. **Source components**: `SeaTunnelSource` -> `SourceSplitEnumerator` (split generation) + `SourceReader` (data reading)
3. **Sink components**: `SeaTunnelSink` -> `SinkWriter` (data writing) + optional `SinkCommitter`

Key interfaces in `seatunnel-api`:
- `org.apache.seatunnel.api.source.SeaTunnelSource`
- `org.apache.seatunnel.api.sink.SeaTunnelSink`
- `org.apache.seatunnel.api.table.factory.TableSourceFactory`
- `org.apache.seatunnel.api.table.factory.TableSinkFactory`

### Job Configuration

Jobs use HOCON format (`.conf` files) with three sections:
```hocon
env {
  parallelism = 2
  job.mode = "BATCH"  # or "STREAMING"
}

source {
  SourceName {
    # source configuration
  }
}

sink {
  SinkName {
    # sink configuration
  }
}
```

## Code Style

- Uses Spotless with Google Java Format (AOSP style)
- Import order: `org.apache.seatunnel.shade`, `org.apache.seatunnel`, `org.apache`, `org`, ``, `javax`, `java`, `#`
- Shaded dependencies: Guava, Jetty, Hikari, Commons Lang3 must use shaded imports (`org.apache.seatunnel.shade.*`)
- Use Lombok annotations (`@Data`, `@Getter`, `@Setter`, `@Slf4j`)
- All files require Apache License header
- Pre-commit hook available: `cp tools/spotless_check/pre-commit.sh .git/hooks/pre-commit`

## Development Guidelines

- JDK 8 or 11 required; Scala 2.12
- New connectors should implement `TableSourceFactory`/`TableSinkFactory` with SPI registration
- E2E tests for connectors should minimize Docker image initialization and combine source/sink tests
- Properties should default to `private final`; prefer primitives over wrapper types
- Sink implementations must be serializable; use singleton pattern for non-serializable properties

## Port Allocation (端口分配)

| 端口 | 组件 | 说明 |
|------|------|------|
| 8214 | Web UI | Vue.js + Vite 开发服务器 |
| 8215 | Go Gateway | REST API 网关 |
| 8216 | Java Zeta Engine | REST API (主节点) |
| 8217+ | Java Zeta Engine | 集群扩展节点 |

配置文件位置:
- Web UI: `seatunnel-engine/seatunnel-engine-ui/.env.development`, `vite.config.ts`
- Go Gateway: `seatunnel-go/cmd/gateway/main.go`, `pkg/gateway/server.go`
- Java Engine: `seatunnel-engine/seatunnel-engine-common/src/main/resources/hazelcast.yaml`

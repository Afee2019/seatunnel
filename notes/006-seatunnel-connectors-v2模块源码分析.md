# SeaTunnel Connectors V2 模块源码分析

> 文档编号: 006
> 模块路径: seatunnel-connectors-v2/
> 更新日期: 2025-12-29

---

## 一、模块概述

`seatunnel-connectors-v2` 是 SeaTunnel 的连接器模块，包含 **60+** 个数据源连接器实现，覆盖关系型数据库、NoSQL、消息队列、数据仓库、数据湖、文件系统等多种数据源。

### 1.1 连接器分类

| 分类 | 连接器 |
|------|--------|
| **关系型数据库** | JDBC (MySQL, PostgreSQL, Oracle, SQL Server, DB2...) |
| **NoSQL** | MongoDB, Redis, Cassandra, HBase, Neo4j, HugeGraph |
| **消息队列** | Kafka, Pulsar, RabbitMQ, RocketMQ, ActiveMQ |
| **数据仓库** | ClickHouse, Doris, StarRocks, Databend, MaxCompute |
| **数据湖** | Iceberg, Hudi, Paimon |
| **文件系统** | Local, HDFS, S3, OSS, COS, FTP, SFTP |
| **CDC** | MySQL-CDC, PostgreSQL-CDC, Oracle-CDC, MongoDB-CDC |
| **搜索引擎** | Elasticsearch, Easysearch |
| **向量数据库** | Milvus, Qdrant |
| **其他** | HTTP, Email, DingTalk, Console, Fake |

### 1.2 模块结构

```
seatunnel-connectors-v2/
├── connector-common/           # 公共基础类
├── connector-fake/             # 测试用假数据源
├── connector-console/          # 控制台输出
├── connector-jdbc/             # JDBC (多数据库支持)
├── connector-kafka/            # Kafka
├── connector-cdc/              # CDC 连接器
│   ├── connector-cdc-base/     # CDC 基础框架
│   ├── connector-cdc-mysql/    # MySQL CDC
│   ├── connector-cdc-postgres/ # PostgreSQL CDC
│   └── ...
├── connector-file/             # 文件系统连接器
│   ├── connector-file-base/
│   ├── connector-file-local/
│   ├── connector-file-s3/
│   └── ...
├── connector-http/             # HTTP 连接器
│   ├── connector-http-base/
│   └── ...
└── [60+ 其他连接器]
```

---

## 二、connector-common 公共模块

### 2.1 包结构

```
org.apache.seatunnel.connectors.seatunnel.common/
├── source/                     # Source 基类
│   ├── AbstractSingleSplitSource.java    # 单分片 Source
│   ├── AbstractSingleSplitReader.java    # 单分片 Reader
│   ├── SingleSplitEnumerator.java        # 单分片 Enumerator
│   ├── reader/                           # Reader 框架
│   │   ├── SourceReaderBase.java
│   │   ├── RecordEmitter.java
│   │   ├── splitreader/
│   │   │   └── SplitReader.java
│   │   └── fetcher/
│   │       ├── SplitFetcher.java
│   │       └── SplitFetcherManager.java
│   └── arrow/                            # Arrow 格式支持
├── sink/                       # Sink 基类
│   ├── AbstractSimpleSink.java           # 简单 Sink
│   └── AbstractSinkWriter.java           # 抽象 Writer
└── util/                       # 工具类
```

### 2.2 AbstractSingleSplitSource - 单分片源

```java
// 文件: source/AbstractSingleSplitSource.java
// 职责: 简化不支持并行的数据源实现

public abstract class AbstractSingleSplitSource<T>
        implements SeaTunnelSource<T, SingleSplit, SingleSplitEnumeratorState> {

    // 创建 Reader（强制单并行度）
    @Override
    public final AbstractSingleSplitReader<T> createReader(SourceReader.Context readerContext) {
        checkArgument(readerContext.getIndexOfSubtask() == 0,
                "A single split source allows only one single reader");
        return createReader(new SingleSplitReaderContext(readerContext));
    }

    // 子类实现
    public abstract AbstractSingleSplitReader<T> createReader(
            SingleSplitReaderContext readerContext) throws Exception;

    // 使用默认的单分片 Enumerator
    @Override
    public final SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<SingleSplit> enumeratorContext) {
        return new SingleSplitEnumerator(enumeratorContext);
    }
}
```

**使用场景**: 简单数据源（如 HTTP API、单文件读取）

### 2.3 AbstractSimpleSink - 简单 Sink

```java
// 文件: sink/AbstractSimpleSink.java
// 职责: 简化不需要两阶段提交的 Sink 实现

public abstract class AbstractSimpleSink<T, StateT>
        implements SeaTunnelSink<T, StateT, Void, Void> {

    // 不需要 Committer
    @Override
    public Optional<SinkCommitter<Void>> createCommitter() {
        return Optional.empty();
    }

    // 不需要聚合 Committer
    @Override
    public Optional<SinkAggregatedCommitter<Void, Void>> createAggregatedCommitter() {
        return Optional.empty();
    }
}
```

**使用场景**: Console、日志输出、简单写入

### 2.4 SourceReaderBase - Reader 框架

```java
// 文件: source/reader/SourceReaderBase.java
// 职责: 提供多分片读取的通用框架

public abstract class SourceReaderBase<E, T, SplitT extends SourceSplit, SplitStateT>
        implements SourceReader<T, SplitT> {

    // 分片获取器管理
    private final SplitFetcherManager<E, SplitT> splitFetcherManager;

    // 记录发射器
    private final RecordEmitter<E, T, SplitStateT> recordEmitter;

    // 未完成的分片状态
    private final Map<String, SplitStateT> splitStates;

    @Override
    public void pollNext(Collector<T> output) throws Exception {
        // 1. 从队列获取数据
        RecordsWithSplitIds<E> recordsWithSplitIds = elementsQueue.poll();

        // 2. 发射记录
        recordEmitter.emitRecord(record, output, splitState);
    }
}
```

---

## 三、JDBC 连接器

### 3.1 模块结构

```
connector-jdbc/
└── src/main/java/.../jdbc/
    ├── config/                 # 配置类
    │   ├── JdbcSourceConfig.java
    │   ├── JdbcSinkConfig.java
    │   └── JdbcConnectionConfig.java
    ├── source/                 # Source 实现
    │   ├── JdbcSourceFactory.java
    │   ├── JdbcSource.java
    │   ├── JdbcSourceReader.java
    │   ├── JdbcSourceSplitEnumerator.java
    │   ├── JdbcSourceSplit.java
    │   └── ChunkSplitter.java  # 分片策略
    ├── sink/                   # Sink 实现
    │   ├── JdbcSinkFactory.java
    │   ├── JdbcSink.java
    │   └── JdbcSinkWriter.java
    ├── catalog/                # 各数据库 Catalog
    │   ├── mysql/
    │   ├── oracle/
    │   ├── psql/
    │   └── ...
    └── internal/
        └── dialect/            # 数据库方言
            ├── JdbcDialect.java
            ├── mysql/
            ├── oracle/
            └── ...
```

### 3.2 JdbcSourceFactory - SPI 入口

```java
// 文件: source/JdbcSourceFactory.java

@AutoService(Factory.class)  // Google AutoService 自动注册 SPI
public class JdbcSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Jdbc";  // 配置中使用的名称
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        // 1. 加载配置
        JdbcSourceConfig config = JdbcSourceConfig.of(context.getOptions());

        // 2. 加载数据库方言
        JdbcDialect jdbcDialect = JdbcDialectLoader.load(
                config.getJdbcConnectionConfig().getUrl(), ...);

        // 3. 返回 Source 实例
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new JdbcSource(config);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(JdbcSourceOptions.URL, JdbcSourceOptions.DRIVER)
                .optional(JdbcSourceOptions.USERNAME, JdbcSourceOptions.PASSWORD, ...)
                .build();
    }
}
```

### 3.3 JdbcSource - 数据源实现

```java
// 文件: source/JdbcSource.java

public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,       // 支持并行
                SupportColumnProjection { // 支持列裁剪

    private final JdbcSourceConfig jdbcSourceConfig;
    private final Map<TablePath, JdbcSourceTable> jdbcSourceTables;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;  // 有界数据源
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return jdbcSourceTables.values().stream()
                .map(JdbcSourceTable::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new JdbcSourceReader(readerContext, jdbcSourceConfig, tables);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, jdbcSourceTables, null);
    }
}
```

### 3.4 分片策略

```
ChunkSplitter (接口)
├── FixedChunkSplitter      # 固定分片数
├── DynamicChunkSplitter    # 动态分片（基于数据量）
└── CollationBasedSplitter  # 基于排序规则分片
```

**分片示例**:
```hocon
source {
  Jdbc {
    partition_column = "id"
    partition_num = 4
    partition_lower_bound = 1
    partition_upper_bound = 10000
  }
}
```

### 3.5 方言架构

```
JdbcDialect (接口)
├── MySqlDialect
├── OracleDialect
├── PostgresDialect
├── SqlServerDialect
├── DB2Dialect
├── SnowflakeDialect
└── ... (30+ 方言)

JdbcDialectFactory (SPI)
├── MySqlDialectFactory
├── OracleDialectFactory
└── ...
```

**方言职责**:
- SQL 语句生成（分页、Upsert 等）
- 类型映射（数据库类型 ↔ SeaTunnel 类型）
- 特殊语法处理

---

## 四、CDC 连接器

### 4.1 模块结构

```
connector-cdc/
├── connector-cdc-base/         # CDC 基础框架
│   ├── config/
│   │   ├── SourceConfig.java
│   │   ├── StartupConfig.java
│   │   └── StopConfig.java
│   ├── source/
│   │   ├── reader/
│   │   │   ├── IncrementalSourceReader.java
│   │   │   └── IncrementalSourceSplitReader.java
│   │   ├── split/
│   │   │   ├── SnapshotSplit.java
│   │   │   └── IncrementalSplit.java
│   │   └── enumerator/
│   │       └── IncrementalSourceEnumerator.java
│   └── dialect/
│       └── DataSourceDialect.java
├── connector-cdc-mysql/        # MySQL CDC
├── connector-cdc-postgres/     # PostgreSQL CDC
├── connector-cdc-oracle/       # Oracle CDC
├── connector-cdc-sqlserver/    # SQL Server CDC
├── connector-cdc-mongodb/      # MongoDB CDC
├── connector-cdc-opengauss/    # OpenGauss CDC
└── connector-cdc-tidb/         # TiDB CDC
```

### 4.2 CDC 工作原理

```
┌─────────────────────────────────────────────────────────────────────┐
│                       CDC 执行流程                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  启动模式 (StartupMode):                                             │
│  ├── INITIAL     # 先全量快照，再增量                                 │
│  ├── EARLIEST    # 从最早的 binlog 开始                              │
│  ├── LATEST      # 仅增量（从当前位置）                               │
│  └── SPECIFIC    # 指定位点开始                                       │
│                                                                      │
│  执行阶段:                                                           │
│  ┌─────────────────┐    ┌─────────────────┐                        │
│  │ Snapshot Phase  │ →  │ Incremental     │                        │
│  │ (全量快照)       │    │ Phase (增量)    │                        │
│  └─────────────────┘    └─────────────────┘                        │
│         │                       │                                    │
│         ▼                       ▼                                    │
│  ┌─────────────────┐    ┌─────────────────┐                        │
│  │ SnapshotSplit   │    │ IncrementalSplit│                        │
│  │ (表数据分片)     │    │ (binlog 流)     │                        │
│  └─────────────────┘    └─────────────────┘                        │
│                                                                      │
│  数据格式: SeaTunnelRow + RowKind (INSERT/UPDATE/DELETE)            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.3 IncrementalSourceReader

```java
// 文件: source/reader/IncrementalSourceReader.java
// 职责: 读取快照和增量数据

public class IncrementalSourceReader<T, C extends SourceConfig>
        extends SingleThreadMultiplexSourceReaderBase<...> {

    @Override
    public void pollNext(Collector<T> output) throws Exception {
        // 1. 处理快照分片
        if (currentSplit instanceof SnapshotSplit) {
            readSnapshotSplit(output);
        }
        // 2. 处理增量分片
        else if (currentSplit instanceof IncrementalSplit) {
            readIncrementalSplit(output);
        }
    }
}
```

### 4.4 MySQL CDC 配置示例

```hocon
source {
  MySQL-CDC {
    hostname = "localhost"
    port = 3306
    username = "root"
    password = "password"
    database-names = ["mydb"]
    table-names = ["mydb.orders", "mydb.users"]

    # 启动模式
    startup.mode = "initial"

    # 服务器 ID（集群唯一）
    server-id = "5400-5404"

    # 时区
    server-time-zone = "Asia/Shanghai"

    # 增量快照
    incremental.snapshot.enabled = true
    incremental.snapshot.chunk-size = 8096
  }
}
```

---

## 五、Kafka 连接器

### 5.1 模块结构

```
connector-kafka/
└── src/main/java/.../kafka/
    ├── config/
    │   ├── KafkaSourceOptions.java
    │   ├── KafkaSinkOptions.java
    │   ├── MessageFormat.java
    │   └── StartMode.java
    ├── source/
    │   ├── KafkaSourceFactory.java
    │   ├── KafkaSource.java
    │   ├── KafkaSourceReader.java
    │   ├── KafkaSourceSplitEnumerator.java
    │   ├── KafkaSourceSplit.java
    │   └── KafkaPartitionSplitReader.java
    ├── sink/
    │   ├── KafkaSinkFactory.java
    │   ├── KafkaSink.java
    │   ├── KafkaSinkWriter.java
    │   └── KafkaProducerSender.java
    └── serialize/
        ├── KafkaSerializationSchema.java
        └── KafkaDeserializationSchema.java
```

### 5.2 KafkaSource

```java
// 文件: source/KafkaSource.java

public class KafkaSource
        implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState>,
                SupportParallelism {

    @Override
    public Boundedness getBoundedness() {
        // Kafka 是无界数据源
        return Boundedness.UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, KafkaSourceSplit> createReader(Context context) {
        return new KafkaSourceReader(
                metadata, context, config, deserializationSchema);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> createEnumerator(
            Context<KafkaSourceSplit> context) {
        return new KafkaSourceSplitEnumerator(context, config);
    }
}
```

### 5.3 消息格式

```java
// 支持的消息格式
public enum MessageFormat {
    JSON,           // JSON 格式
    TEXT,           // 纯文本
    CANAL_JSON,     // Canal JSON（MySQL binlog）
    DEBEZIUM_JSON,  // Debezium JSON（CDC）
    COMPATIBLE_DEBEZIUM_JSON,
    COMPATIBLE_KAFKA_CONNECT_JSON,
    OGG_JSON,       // Oracle GoldenGate
    MAXWELL_JSON,   // Maxwell（MySQL binlog）
    AVRO,           // Avro 格式
    PROTOBUF        // Protobuf 格式
}
```

### 5.4 配置示例

```hocon
# Source
source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "my_topic"
    consumer.group = "seatunnel_group"
    start_mode = "earliest"
    format = "json"
    schema = {
      fields { id = "bigint", name = "string" }
    }
  }
}

# Sink
sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "output_topic"
    format = "json"
    semantics = "EXACTLY_ONCE"
    partition_key_fields = ["id"]
  }
}
```

---

## 六、文件连接器

### 6.1 模块结构

```
connector-file/
├── connector-file-base/        # 基础框架
│   ├── source/
│   │   ├── BaseFileSource.java
│   │   ├── BaseFileSourceReader.java
│   │   └── BaseFileSplit.java
│   ├── sink/
│   │   ├── BaseFileSink.java
│   │   └── BaseFileSinkWriter.java
│   └── config/
├── connector-file-local/       # 本地文件
├── connector-file-hadoop/      # HDFS
├── connector-file-s3/          # AWS S3
├── connector-file-oss/         # 阿里云 OSS
├── connector-file-cos/         # 腾讯云 COS
├── connector-file-obs/         # 华为云 OBS
├── connector-file-ftp/         # FTP
└── connector-file-sftp/        # SFTP
```

### 6.2 支持的文件格式

| 格式 | 读取 | 写入 | 说明 |
|------|------|------|------|
| JSON | Y | Y | JSON Lines 格式 |
| CSV | Y | Y | 逗号分隔值 |
| Text | Y | Y | 纯文本 |
| Parquet | Y | Y | 列式存储 |
| ORC | Y | Y | 优化行列存储 |
| Avro | Y | Y | 二进制格式 |
| Excel | Y | N | xlsx/xls |
| XML | Y | N | XML 格式 |

### 6.3 配置示例

```hocon
# 本地文件 Source
source {
  LocalFile {
    path = "/data/input/"
    file_format_type = "json"
    schema = { fields { id = "bigint", name = "string" } }
  }
}

# S3 Sink（带分区）
sink {
  S3File {
    path = "/output/data"
    bucket = "my-bucket"
    access_key = "xxx"
    secret_key = "xxx"
    file_format_type = "parquet"
    partition_by = ["dt", "hour"]
    partition_dir_expression = "${k0}=${v0}/${k1}=${v1}"
  }
}
```

---

## 七、简单连接器示例

### 7.1 FakeSource - 测试数据源

```java
// 文件: fake/source/FakeSource.java
// 职责: 生成测试数据

public class FakeSource
        implements SeaTunnelSource<SeaTunnelRow, FakeSourceSplit, FakeSourceState>,
                SupportParallelism {

    @Override
    public Boundedness getBoundedness() {
        // 根据作业模式决定
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, FakeSourceSplit> createReader(Context context) {
        return new FakeSourceReader(context, config, jobContext.getJobId());
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> createEnumerator(
            Context<FakeSourceSplit> context) {
        return new FakeSourceSplitEnumerator(context, config, Collections.emptySet());
    }
}
```

### 7.2 ConsoleSink - 控制台输出

```java
// 文件: console/sink/ConsoleSink.java
// 职责: 将数据打印到控制台

public class ConsoleSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink, SupportSchemaEvolutionSink {

    private final SeaTunnelRowType seaTunnelRowType;
    private final boolean isPrintData;
    private final int delayMs;

    @Override
    public ConsoleSinkWriter createWriter(SinkWriter.Context context) {
        return new ConsoleSinkWriter(seaTunnelRowType, context, isPrintData, delayMs);
    }

    @Override
    public String getPluginName() {
        return "Console";
    }

    // 支持 Schema 演进
    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN);
    }
}
```

---

## 八、连接器开发规范

### 8.1 目录结构

```
connector-xxx/
├── pom.xml
└── src/main/java/.../xxx/
    ├── config/
    │   ├── XxxSourceOptions.java     # Source 配置选项
    │   ├── XxxSinkOptions.java       # Sink 配置选项
    │   └── XxxConfig.java            # 配置类
    ├── source/
    │   ├── XxxSourceFactory.java     # SPI 入口
    │   ├── XxxSource.java            # Source 实现
    │   ├── XxxSourceReader.java      # Reader 实现
    │   ├── XxxSourceSplitEnumerator.java
    │   └── XxxSourceSplit.java
    ├── sink/
    │   ├── XxxSinkFactory.java       # SPI 入口
    │   ├── XxxSink.java              # Sink 实现
    │   └── XxxSinkWriter.java        # Writer 实现
    ├── catalog/                      # Catalog 实现（可选）
    ├── state/                        # 状态类
    └── exception/                    # 异常类
```

### 8.2 SPI 注册

使用 Google AutoService 自动生成 SPI 配置：

```java
@AutoService(Factory.class)
public class XxxSourceFactory implements TableSourceFactory {
    // ...
}
```

自动生成文件：
```
META-INF/services/org.apache.seatunnel.api.table.factory.Factory
```

### 8.3 配置选项定义

```java
public class XxxSourceOptions {
    public static final Option<String> HOST = Options.key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("Server host");

    public static final Option<Integer> PORT = Options.key("port")
            .intType()
            .defaultValue(8080)
            .withDescription("Server port");
}
```

### 8.4 必须实现的接口

**Source 连接器**:
- `TableSourceFactory`: SPI 入口
- `SeaTunnelSource`: 数据源接口
- `SourceSplitEnumerator`: 分片枚举器
- `SourceReader`: 数据读取器
- `SourceSplit`: 分片定义

**Sink 连接器**:
- `TableSinkFactory`: SPI 入口
- `SeaTunnelSink`: 数据写入接口
- `SinkWriter`: 数据写入器
- `SinkCommitter` (可选): 两阶段提交

---

## 九、连接器完整列表

### 9.1 数据库连接器

| 连接器 | Source | Sink | CDC | 说明 |
|--------|--------|------|-----|------|
| JDBC | Y | Y | - | 通用 JDBC |
| MySQL-CDC | Y | - | Y | MySQL 变更数据捕获 |
| PostgreSQL-CDC | Y | - | Y | PostgreSQL CDC |
| Oracle-CDC | Y | - | Y | Oracle CDC |
| SQL Server-CDC | Y | - | Y | SQL Server CDC |
| MongoDB | Y | Y | - | 文档数据库 |
| MongoDB-CDC | Y | - | Y | MongoDB CDC |
| Redis | Y | Y | - | 键值存储 |
| Cassandra | Y | Y | - | 宽列数据库 |
| HBase | Y | Y | - | Hadoop 数据库 |
| Neo4j | Y | Y | - | 图数据库 |
| InfluxDB | Y | Y | - | 时序数据库 |
| TDengine | Y | Y | - | 时序数据库 |

### 9.2 消息队列连接器

| 连接器 | Source | Sink | 说明 |
|--------|--------|------|------|
| Kafka | Y | Y | 分布式消息队列 |
| Pulsar | Y | Y | 云原生消息队列 |
| RabbitMQ | Y | Y | AMQP 消息队列 |
| RocketMQ | Y | Y | 阿里消息队列 |
| ActiveMQ | Y | Y | Java 消息队列 |
| Amazon SQS | Y | Y | AWS 消息队列 |

### 9.3 数据仓库连接器

| 连接器 | Source | Sink | 说明 |
|--------|--------|------|------|
| ClickHouse | Y | Y | OLAP 数据库 |
| Doris | Y | Y | MPP 数据库 |
| StarRocks | Y | Y | 实时 OLAP |
| Databend | Y | Y | 云数据仓库 |
| MaxCompute | Y | Y | 阿里云数仓 |
| Snowflake | Y | Y | 云数据仓库 |
| Druid | Y | Y | 实时分析 |

### 9.4 数据湖连接器

| 连接器 | Source | Sink | 说明 |
|--------|--------|------|------|
| Iceberg | Y | Y | Apache Iceberg |
| Hudi | Y | Y | Apache Hudi |
| Paimon | Y | Y | Apache Paimon |
| Hive | Y | Y | Hadoop 数仓 |

### 9.5 文件系统连接器

| 连接器 | Source | Sink | 说明 |
|--------|--------|------|------|
| LocalFile | Y | Y | 本地文件 |
| HDFS | Y | Y | Hadoop 文件系统 |
| S3 | Y | Y | AWS S3 |
| OSS | Y | Y | 阿里云 OSS |
| COS | Y | Y | 腾讯云 COS |
| OBS | Y | Y | 华为云 OBS |
| FTP | Y | Y | FTP 服务器 |
| SFTP | Y | Y | SSH 文件传输 |

### 9.6 其他连接器

| 连接器 | Source | Sink | 说明 |
|--------|--------|------|------|
| Elasticsearch | Y | Y | 搜索引擎 |
| HTTP | Y | Y | REST API |
| Email | - | Y | 邮件发送 |
| DingTalk | - | Y | 钉钉通知 |
| Console | - | Y | 控制台输出 |
| FakeSource | Y | - | 测试数据生成 |
| Assert | - | Y | 数据断言测试 |
| Milvus | Y | Y | 向量数据库 |
| Qdrant | Y | Y | 向量数据库 |

---

## 十、总结

### 10.1 架构特点

1. **SPI 机制**: 通过 `Factory` 接口和 AutoService 实现插件发现
2. **抽象基类**: `AbstractSingleSplitSource`、`AbstractSimpleSink` 简化开发
3. **方言模式**: JDBC 连接器支持 30+ 数据库方言
4. **统一接口**: 所有连接器遵循相同的 API 规范
5. **模块化**: 每个连接器独立模块，按需引入

### 10.2 关键类

| 类 | 功能 |
|----|------|
| `TableSourceFactory` | Source SPI 入口 |
| `TableSinkFactory` | Sink SPI 入口 |
| `AbstractSingleSplitSource` | 单分片 Source 基类 |
| `AbstractSimpleSink` | 简单 Sink 基类 |
| `SourceReaderBase` | 多分片 Reader 框架 |
| `JdbcDialect` | JDBC 数据库方言接口 |

### 10.3 开发要点

1. 实现 `Factory` 接口并使用 `@AutoService` 注解
2. 定义 `OptionRule` 描述配置选项
3. 正确处理并行度和分片
4. 实现检查点（支持 Exactly-Once）
5. 处理 Schema 演进（CDC 场景）

---

*文档结束*

# SeaTunnel 源码分析总结索引
> 文档编号: 017
> 更新日期: 2025-12-30
> 文档总数: 17 篇
> 总计大小: 520KB+
---

## 一、文档概览

本系列文档对 Apache SeaTunnel 2.3.x 版本的源码进行了全面分析，涵盖项目结构、核心模块、连接器、转换器、引擎适配等各个方面。

### 1.1 文档列表

| 编号 | 文档名称 | 大小 | 核心内容 |
|------|----------|------|----------|
| 001 | [项目目录结构详解](001-项目目录结构详解.md) | 23KB | 项目结构、模块划分、技术栈 |
| 002 | [SeaTunnel使用指南](002-SeaTunnel使用指南.md) | 19KB | 安装部署、命令行、配置语法 |
| 003 | [seatunnel-core模块源码分析](003-seatunnel-core模块源码分析.md) | 31KB | 启动器、命令解析、引擎入口 |
| 004 | [seatunnel-engine模块源码分析](004-seatunnel-engine模块源码分析.md) | 47KB | Zeta引擎、作业调度、Checkpoint |
| 005 | [seatunnel-api模块源码分析](005-seatunnel-api模块源码分析.md) | 40KB | Source/Sink/Transform API |
| 006 | [seatunnel-connectors-v2模块源码分析](006-seatunnel-connectors-v2模块源码分析.md) | 27KB | 60+连接器、JDBC/CDC/Kafka |
| 007 | [seatunnel-transforms-v2模块源码分析](007-seatunnel-transforms-v2模块源码分析.md) | 29KB | 21种转换器、SQL/LLM/动态编译 |
| 008 | [seatunnel-formats模块源码分析](008-seatunnel-formats模块源码分析.md) | 23KB | JSON/Avro/Protobuf/CDC格式 |
| 009 | [seatunnel-translation模块源码分析](009-seatunnel-translation模块源码分析.md) | 31KB | Flink/Spark适配层 |
| 010 | [seatunnel-e2e模块源码分析](010-seatunnel-e2e模块源码分析.md) | 27KB | E2E测试框架、TestContainers |
| 011 | [seatunnel-common模块源码分析](011-seatunnel-common模块源码分析.md) | 28KB | 工具类、异常体系、常量 |
| 012 | [seatunnel-config模块源码分析](012-seatunnel-config模块源码分析.md) | 32KB | HOCON解析、SQL配置转换 |
| 013 | [seatunnel-shade模块源码分析](013-seatunnel-shade模块源码分析.md) | 32KB | 依赖隔离、12个Shade模块 |
| 014 | [seatunnel-plugin-discovery模块源码分析](014-seatunnel-plugin-discovery模块源码分析.md) | 39KB | SPI插件发现、动态加载 |
| 015 | [seatunnel-dist模块源码分析](015-seatunnel-dist模块源码分析.md) | 28KB | 打包配置、Docker构建 |
| 016 | [seatunnel-examples模块源码分析](016-seatunnel-examples模块源码分析.md) | 26KB | IDE运行示例、调试入口 |
| 018 | [SeaTunnel代码审查与改进建议](018-SeaTunnel代码审查与改进建议.md) | 55KB+ | 安全漏洞、并发问题、架构优化 |

### 1.2 文档分类

```
┌─────────────────────────────────────────────────────────────┐
│                    SeaTunnel 源码分析文档                    │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌────────────┬────────┴────────┬────────────┐
        │            │                 │            │
        ▼            ▼                 ▼            ▼
   ┌─────────┐  ┌─────────┐      ┌─────────┐  ┌─────────┐
   │入门文档  │  │核心模块  │      │辅助模块  │  │质量审查  │
   │001-002  │  │003-009  │      │010-016  │  │  018    │
   └─────────┘  └─────────┘      └─────────┘  └─────────┘
```

---

## 二、项目架构总览

### 2.1 核心架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SeaTunnel 整体架构                           │
└─────────────────────────────────────────────────────────────────────┘

                           ┌──────────────┐
                           │   用户配置    │
                           │ (HOCON/SQL)  │
                           └──────┬───────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        seatunnel-core (003)                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │seatunnel-   │  │seatunnel-   │  │seatunnel-   │                  │
│  │starter      │  │flink-starter│  │spark-starter│                  │
│  │(Zeta引擎)   │  │(Flink适配)  │  │(Spark适配)  │                  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                  │
└─────────┼────────────────┼────────────────┼─────────────────────────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│seatunnel-engine │ │  translation-   │ │  translation-   │
│     (004)       │ │  flink (009)    │ │  spark (009)    │
│  Zeta 原生引擎  │ │  Flink 适配层   │ │  Spark 适配层   │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        seatunnel-api (005)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │SeaTunnelSource│  │SeaTunnelSink │  │SeaTunnelTransform│           │
│  │   (Source)   │  │    (Sink)    │  │  (Transform) │               │
│  └──────────────┘  └──────────────┘  └──────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│connectors-v2    │ │transforms-v2    │ │   formats       │
│    (006)        │ │    (007)        │ │    (008)        │
│  60+ 连接器     │ │  21 种转换器    │ │  7 种格式       │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

### 2.2 模块依赖关系

```
seatunnel-dist (015)                    # 打包分发
    │
    ├── seatunnel-core (003)            # 启动入口
    │   ├── seatunnel-starter           # Zeta 启动器
    │   ├── seatunnel-flink-starter     # Flink 启动器
    │   └── seatunnel-spark-starter     # Spark 启动器
    │
    ├── seatunnel-engine (004)          # Zeta 引擎
    │   ├── seatunnel-engine-server     # 服务端
    │   ├── seatunnel-engine-client     # 客户端
    │   └── seatunnel-engine-storage    # 状态存储
    │
    ├── seatunnel-translation (009)     # 引擎适配
    │   ├── translation-flink           # Flink 适配
    │   └── translation-spark           # Spark 适配
    │
    ├── seatunnel-connectors-v2 (006)   # 连接器
    │   ├── connector-jdbc
    │   ├── connector-kafka
    │   ├── connector-cdc-*
    │   └── ...（60+）
    │
    ├── seatunnel-transforms-v2 (007)   # 转换器
    │
    ├── seatunnel-formats (008)         # 数据格式
    │
    ├── seatunnel-api (005)             # 核心 API
    │
    ├── seatunnel-common (011)          # 公共工具
    │
    ├── seatunnel-config (012)          # 配置解析
    │
    ├── seatunnel-shade (013)           # 依赖隔离
    │
    └── seatunnel-plugin-discovery (014)# 插件发现
```

---

## 三、核心模块详解

### 3.1 seatunnel-api (005)

**核心 API 模块，定义 SeaTunnel 的编程模型。**

| 组件 | 接口 | 说明 |
|------|------|------|
| Source | `SeaTunnelSource` | 数据源接口 |
| | `SourceSplitEnumerator` | 分片枚举器 |
| | `SourceReader` | 数据读取器 |
| Sink | `SeaTunnelSink` | 数据目标接口 |
| | `SinkWriter` | 数据写入器 |
| | `SinkCommitter` | 两阶段提交 |
| Transform | `SeaTunnelTransform` | 数据转换接口 |
| Table | `CatalogTable` | 表元数据 |
| | `TableSchema` | 表结构定义 |
| Factory | `TableSourceFactory` | Source 工厂 |
| | `TableSinkFactory` | Sink 工厂 |

**关键设计模式**：
- Factory + SPI：插件发现与创建
- 模板方法：统一的生命周期管理
- 两阶段提交：Exactly-Once 语义

### 3.2 seatunnel-engine (004)

**Zeta 引擎，SeaTunnel 的原生分布式执行引擎。**

```
┌─────────────────────────────────────────────────────────────┐
│                      Zeta Engine 架构                        │
└─────────────────────────────────────────────────────────────┘

┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Client     │───▶│    Master    │◀──▶│    Worker    │
│  (提交作业)  │    │  (调度协调)  │    │  (执行任务)  │
└──────────────┘    └──────┬───────┘    └──────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  Hazelcast   │
                    │  (分布式通信) │
                    └──────────────┘
```

**核心组件**：
- `CoordinatorService`：作业协调服务
- `TaskExecutionService`：任务执行服务
- `CheckpointManager`：检查点管理
- `ResourceManager`：资源管理
- `SlotService`：槽位分配

### 3.3 seatunnel-connectors-v2 (006)

**60+ 连接器，覆盖主流数据源。**

| 分类 | 连接器数量 | 代表连接器 |
|------|------------|------------|
| 关系数据库 | 15+ | JDBC, ClickHouse, Doris, StarRocks |
| 消息队列 | 6 | Kafka, Pulsar, RocketMQ, RabbitMQ |
| 文件系统 | 10+ | HDFS, S3, OSS, FTP, LocalFile |
| NoSQL | 8 | MongoDB, Redis, Cassandra, HBase |
| CDC | 7 | MySQL-CDC, Postgres-CDC, Oracle-CDC |
| 数据湖 | 3 | Iceberg, Hudi, Paimon |
| 搜索引擎 | 3 | Elasticsearch, Easysearch, Typesense |
| 向量数据库 | 2 | Milvus, Qdrant |

**连接器开发模式**：
```java
// 1. 实现 Factory
public class JdbcSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() { return "Jdbc"; }

    @Override
    public SeaTunnelSource createSource(TableSourceFactoryContext context) {
        return new JdbcSource(context.getOptions());
    }
}

// 2. SPI 注册
// META-INF/services/org.apache.seatunnel.api.table.factory.Factory
org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory
```

### 3.4 seatunnel-transforms-v2 (007)

**21 种数据转换器。**

| 分类 | 转换器 | 说明 |
|------|--------|------|
| 字段操作 | Filter, Copy, Rename, Replace | 字段过滤/复制/重命名 |
| SQL | Sql | 内置 Zeta SQL 引擎 |
| 数据处理 | Split, JsonPath, FieldMapper | 字段拆分/JSON解析 |
| AI/NLP | LLM, Embedding | 大模型/向量化 |
| 动态编译 | DynamicCompile | Java/Groovy/Scala 运行时编译 |

**Transform 继承体系**：
```
SeaTunnelTransform (接口)
    │
    ├── AbstractSeaTunnelTransform (抽象基类)
    │   │
    │   ├── SingleFieldOutputTransform (单字段输出)
    │   │   ├── CopyFieldTransform
    │   │   └── ReplaceTransform
    │   │
    │   └── MultipleFieldOutputTransform (多字段输出)
    │       ├── SplitTransform
    │       └── JsonPathTransform
    │
    └── FilterRowTransform (行过滤)
```

### 3.5 seatunnel-translation (009)

**引擎适配层，将 SeaTunnel API 翻译为 Flink/Spark API。**

```
┌─────────────────────────────────────────────────────────────┐
│                  SeaTunnel Connector V2 API                  │
│        (SeaTunnelSource / SeaTunnelSink / Transform)        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Translation Layer                         │
│  ┌─────────────────────┐    ┌─────────────────────┐         │
│  │  ParallelSource     │    │  CoordinatedSource  │         │
│  │  (批处理模式)       │    │  (CDC/流处理模式)   │         │
│  └─────────────────────┘    └─────────────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│   Flink Translation     │    │   Spark Translation     │
│  ┌───────────────────┐  │    │  ┌───────────────────┐  │
│  │FlinkSource        │  │    │  │SparkDataSource    │  │
│  │FlinkSink          │  │    │  │SparkDataWriter    │  │
│  └───────────────────┘  │    │  └───────────────────┘  │
└─────────────────────────┘    └─────────────────────────┘
              │                               │
              ▼                               ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│      Flink Engine       │    │      Spark Engine       │
│   (1.13 / 1.15 / 2.0)   │    │    (2.4 / 3.3)          │
└─────────────────────────┘    └─────────────────────────┘
```

---

## 四、辅助模块详解

### 4.1 seatunnel-common (011)

**公共工具类和异常体系。**

| 组件 | 类 | 说明 |
|------|-----|------|
| 常量 | `Constants` | 全局常量定义 |
| | `PluginType` | 插件类型枚举 |
| 异常 | `SeaTunnelRuntimeException` | 结构化异常 |
| | `CommonError` | 异常工厂 |
| 工具 | `JsonUtils` | Jackson JSON 工具 |
| | `RetryUtils` | 指数退避重试 |
| | `DateTimeUtils` | 日期时间解析 |
| | `FileUtils` | 文件操作工具 |

### 4.2 seatunnel-config (012)

**配置解析模块。**

| 子模块 | 功能 |
|--------|------|
| config-shade | Typesafe Config 的 Shade 封装 |
| config-base | 基础配置接口（预留） |
| config-sql | SQL DDL 到 HOCON 配置转换 |

**配置语法支持**：
```hocon
# HOCON 格式
env {
  job.mode = "BATCH"
  parallelism = 2
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    query = "SELECT * FROM users"
  }
}
```

```sql
-- SQL 格式
SET 'job.mode' = 'BATCH';

CREATE TABLE mysql_source (...) WITH ('connector' = 'Jdbc', ...);
INSERT INTO console_sink SELECT * FROM mysql_source;
```

### 4.3 seatunnel-shade (013)

**依赖隔离模块，避免与 Flink/Spark 的依赖冲突。**

| Shade 模块 | 原始依赖 | 用途 |
|------------|----------|------|
| seatunnel-guava | com.google.guava | 集合工具 |
| seatunnel-jackson | com.fasterxml.jackson | JSON 处理 |
| seatunnel-hazelcast | com.hazelcast | Zeta 引擎核心 |
| seatunnel-hikari | com.zaxxer:HikariCP | JDBC 连接池 |
| seatunnel-jetty9 | org.eclipse.jetty | REST API |
| seatunnel-hadoop3 | org.apache.hadoop | HDFS 支持 |
| seatunnel-arrow | org.apache.arrow | 列式数据 |

**Shade 后的包名**：
```
com.google.guava → org.apache.seatunnel.shade.com.google
com.fasterxml.jackson → org.apache.seatunnel.shade.com.fasterxml.jackson
```

### 4.4 seatunnel-plugin-discovery (014)

**SPI 插件发现机制。**

```
┌─────────────────────────────────────────────────────────────┐
│                      插件发现流程                            │
└─────────────────────────────────────────────────────────────┘

用户配置: source { Jdbc { ... } }
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  1. 解析 PluginIdentifier("seatunnel", "source", "Jdbc")    │
└─────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  2. 查询 plugin-mapping.properties                          │
│     seatunnel.source.Jdbc = connector-jdbc                  │
└─────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  3. 查找 connectors/connector-jdbc-*.jar                    │
└─────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  4. ServiceLoader.load(SeaTunnelSource.class)               │
└─────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  5. 返回 JdbcSource 实例                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 五、数据流与执行流程

### 5.1 作业提交流程

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ 用户配置  │───▶│ 配置解析  │───▶│ 插件发现  │───▶│ DAG 构建  │
│ (HOCON)  │    │ (Config) │    │ (SPI)    │    │ (作业图) │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
                                                      │
                                                      ▼
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ 作业完成  │◀───│ 任务执行  │◀───│ 资源分配  │◀───│ 作业提交  │
│          │    │ (Task)   │    │ (Slot)   │    │ (Submit) │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
```

### 5.2 数据处理流程

```
┌─────────────────────────────────────────────────────────────┐
│                       Source                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │ Enumerator  │───▶│   Split     │───▶│   Reader    │      │
│  │ (分片生成)   │    │  (数据分片) │    │  (数据读取) │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼ SeaTunnelRow
┌─────────────────────────────────────────────────────────────┐
│                      Transform                               │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │   Filter    │───▶│    SQL      │───▶│   Custom    │      │
│  │  (字段过滤) │    │ (SQL转换)   │    │  (自定义)   │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼ SeaTunnelRow
┌─────────────────────────────────────────────────────────────┐
│                        Sink                                  │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │   Writer    │───▶│  Committer  │───▶│   Target    │      │
│  │  (数据写入) │    │ (两阶段提交)│    │  (目标存储) │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

---

## 六、关键设计模式

### 6.1 Factory + SPI 模式

```java
// Factory 接口
public interface TableSourceFactory extends Factory {
    String factoryIdentifier();
    SeaTunnelSource createSource(TableSourceFactoryContext context);
}

// SPI 注册
// META-INF/services/org.apache.seatunnel.api.table.factory.Factory
org.apache.seatunnel.connectors.jdbc.source.JdbcSourceFactory
org.apache.seatunnel.connectors.kafka.source.KafkaSourceFactory

// 运行时发现
ServiceLoader<Factory> factories = ServiceLoader.load(Factory.class);
```

### 6.2 模板方法模式

```java
// 抽象基类定义骨架
public abstract class AbstractSeaTunnelTransform implements SeaTunnelTransform {
    @Override
    public final SeaTunnelRow transform(SeaTunnelRow row) {
        // 1. 前置处理（可选）
        beforeTransform(row);
        // 2. 核心转换（子类实现）
        SeaTunnelRow result = doTransform(row);
        // 3. 后置处理（可选）
        afterTransform(result);
        return result;
    }

    protected abstract SeaTunnelRow doTransform(SeaTunnelRow row);
}
```

### 6.3 两阶段提交

```java
// Sink 两阶段提交保证 Exactly-Once
public interface SinkCommitter<CommitInfoT> {
    // 阶段1：准备提交
    List<CommitInfoT> commit(List<CommitInfoT> commitInfos);

    // 阶段2：完成提交（Checkpoint 成功后）
    void abort(List<CommitInfoT> commitInfos);
}
```

### 6.4 适配器模式

```java
// SeaTunnel Source 适配到 Flink Source
public class FlinkSource implements Source<SeaTunnelRow, ...> {
    private final SeaTunnelSource source;

    @Override
    public SourceReader createReader(...) {
        return new FlinkSourceReader(source.createReader(...));
    }
}
```

---

## 七、快速参考

### 7.1 常用命令

```bash
# 构建项目
./mvnw clean install -DskipTests

# 构建分发包
./mvnw clean package -pl seatunnel-dist -am -DskipTests

# 运行作业（Zeta 本地模式）
./bin/seatunnel.sh -c config/job.conf

# 运行作业（Flink）
./bin/start-seatunnel-flink-15-connector-v2.sh -c config/job.conf

# 运行作业（Spark）
./bin/start-seatunnel-spark-3-connector-v2.sh -c config/job.conf
```

### 7.2 配置文件模板

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"  # 或 "STREAMING"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "password"
    query = "SELECT * FROM users"
    plugin_output = "jdbc_source"
  }
}

transform {
  Filter {
    plugin_input = "jdbc_source"
    plugin_output = "filtered"
    fields = ["id", "name"]
  }
}

sink {
  Console {
    plugin_input = "filtered"
  }
}
```

### 7.3 目录结构

```
SEATUNNEL_HOME/
├── bin/                    # 启动脚本
├── config/                 # 配置文件
├── connectors/             # 连接器 JAR
│   └── plugin-mapping.properties
├── lib/                    # 公共库
├── starter/                # 引擎启动器
└── plugins/                # 自定义依赖
```

---

## 八、学习路径推荐

### 8.1 入门阶段

1. **[001-项目目录结构详解](001-项目目录结构详解.md)** - 了解项目整体结构
2. **[002-SeaTunnel使用指南](002-SeaTunnel使用指南.md)** - 学习基本使用方法
3. **[016-seatunnel-examples模块源码分析](016-seatunnel-examples模块源码分析.md)** - 运行示例代码

### 8.2 核心阶段

4. **[005-seatunnel-api模块源码分析](005-seatunnel-api模块源码分析.md)** - 理解核心 API
5. **[003-seatunnel-core模块源码分析](003-seatunnel-core模块源码分析.md)** - 理解启动流程
6. **[006-seatunnel-connectors-v2模块源码分析](006-seatunnel-connectors-v2模块源码分析.md)** - 学习连接器开发

### 8.3 进阶阶段

7. **[004-seatunnel-engine模块源码分析](004-seatunnel-engine模块源码分析.md)** - 深入 Zeta 引擎
8. **[009-seatunnel-translation模块源码分析](009-seatunnel-translation模块源码分析.md)** - 理解引擎适配
9. **[007-seatunnel-transforms-v2模块源码分析](007-seatunnel-transforms-v2模块源码分析.md)** - 学习转换器开发

### 8.4 深入阶段

10. **[014-seatunnel-plugin-discovery模块源码分析](014-seatunnel-plugin-discovery模块源码分析.md)** - 理解插件机制
11. **[013-seatunnel-shade模块源码分析](013-seatunnel-shade模块源码分析.md)** - 理解依赖隔离
12. **[012-seatunnel-config模块源码分析](012-seatunnel-config模块源码分析.md)** - 理解配置解析

---

## 九、常见问题索引

| 问题 | 相关文档 |
|------|----------|
| 如何安装部署 SeaTunnel？ | [002-SeaTunnel使用指南](002-SeaTunnel使用指南.md) |
| 如何开发新的连接器？ | [006-seatunnel-connectors-v2模块源码分析](006-seatunnel-connectors-v2模块源码分析.md) |
| 如何开发新的转换器？ | [007-seatunnel-transforms-v2模块源码分析](007-seatunnel-transforms-v2模块源码分析.md) |
| Zeta 引擎如何调度作业？ | [004-seatunnel-engine模块源码分析](004-seatunnel-engine模块源码分析.md) |
| 如何在 IDE 中调试？ | [016-seatunnel-examples模块源码分析](016-seatunnel-examples模块源码分析.md) |
| 依赖冲突如何解决？ | [013-seatunnel-shade模块源码分析](013-seatunnel-shade模块源码分析.md) |
| 插件如何被发现和加载？ | [014-seatunnel-plugin-discovery模块源码分析](014-seatunnel-plugin-discovery模块源码分析.md) |
| 如何支持新的数据格式？ | [008-seatunnel-formats模块源码分析](008-seatunnel-formats模块源码分析.md) |
| E2E 测试如何编写？ | [010-seatunnel-e2e模块源码分析](010-seatunnel-e2e模块源码分析.md) |
| 分发包如何构建？ | [015-seatunnel-dist模块源码分析](015-seatunnel-dist模块源码分析.md) |
| 项目有哪些安全漏洞？ | [018-SeaTunnel代码审查与改进建议](018-SeaTunnel代码审查与改进建议.md) |
| 有哪些并发和性能问题？ | [018-SeaTunnel代码审查与改进建议](018-SeaTunnel代码审查与改进建议.md) |
| 项目架构如何优化？ | [018-SeaTunnel代码审查与改进建议](018-SeaTunnel代码审查与改进建议.md) |
| 依赖版本需要更新吗？ | [018-SeaTunnel代码审查与改进建议](018-SeaTunnel代码审查与改进建议.md) |

---

## 十、附录

### 10.1 技术栈总结

| 分类 | 技术 |
|------|------|
| 语言 | Java 8/11, Scala 2.12 |
| 构建 | Maven 3.6+ |
| 配置 | HOCON (Typesafe Config) |
| 分布式 | Hazelcast 5.1 |
| 日志 | Log4j2, SLF4J |
| 序列化 | Kryo, Jackson |
| 测试 | JUnit 5, TestContainers |
| 引擎 | Flink 1.13-2.0, Spark 2.4-3.3 |

### 10.2 版本信息

| 项目 | 版本 |
|------|------|
| SeaTunnel | 2.3.x |
| Java | 8 / 11 |
| Flink | 1.13.6 / 1.15.3 / 2.0.0 |
| Spark | 2.4.0 / 3.3.0 |
| Hadoop | 3.1.4 |
| Hazelcast | 5.1 |

### 10.3 文档统计

| 指标 | 数值 |
|------|------|
| 文档总数 | 16 篇 |
| 总计行数 | 约 15,000 行 |
| 总计大小 | 463 KB |
| 覆盖模块 | 16 个 |
| 创建日期 | 2025-12-28 ~ 2025-12-29 |

---

*本索引文档持续更新，如有新增模块分析，请在此文档中添加相应条目。*

# Apache SeaTunnel 使用指南

> 文档编号: 002
> 版本: 2.3.13-SNAPSHOT
> 更新日期: 2025-12-29

---

## 目录

1. [快速入门](#一快速入门)
2. [安装部署](#二安装部署)
3. [作业配置详解](#三作业配置详解)
4. [数据源连接器](#四数据源连接器)
5. [数据转换](#五数据转换)
6. [运维管理](#六运维管理)
7. [常见问题排查](#七常见问题排查)

---

## 一、快速入门

### 1.1 环境要求

| 组件 | 要求 |
|------|------|
| JDK | 8 或 11（推荐 JDK 8） |
| 操作系统 | Linux、macOS、Windows |
| 内存 | 最低 4GB，推荐 8GB+ |

### 1.2 目录结构

```
apache-seatunnel-2.3.13-SNAPSHOT/
├── bin/                    # 启动脚本
│   ├── seatunnel.sh        # 本地模式运行作业
│   ├── seatunnel-cluster.sh # 启动集群服务
│   └── stop-seatunnel-cluster.sh
├── config/                 # 配置文件
│   ├── seatunnel.yaml      # 引擎主配置
│   ├── hazelcast.yaml      # 集群配置
│   └── *.config.template   # 作业配置模板
├── connectors/             # 连接器 JAR 包（82个）
├── lib/                    # 核心依赖库
└── starter/                # 引擎启动器
```

### 1.3 第一个作业

创建配置文件 `config/my_first_job.conf`:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake_data"
    row.num = 10
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "fake_data"
  }
}
```

运行作业:

```bash
# 设置 JDK 8
export JAVA_HOME=/path/to/jdk8

# 本地模式运行
./bin/seatunnel.sh --config config/my_first_job.conf -e local
```

---

## 二、安装部署

### 2.1 本地模式

最简单的运行方式，适合开发测试:

```bash
./bin/seatunnel.sh --config <配置文件路径> -e local
```

### 2.2 集群模式

#### 2.2.1 单机集群

```bash
# 启动服务
./bin/seatunnel-cluster.sh -d

# 提交作业
./bin/seatunnel.sh --config config/my_job.conf

# 停止服务
./bin/stop-seatunnel-cluster.sh
```

#### 2.2.2 多节点集群

**步骤 1: 配置 `config/hazelcast.yaml`**

```yaml
hazelcast:
  cluster-name: seatunnel
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - 192.168.1.101
          - 192.168.1.102
          - 192.168.1.103
    port:
      auto-increment: false
      port: 5801
```

**步骤 2: 在每个节点启动**

```bash
./bin/seatunnel-cluster.sh -d
```

**步骤 3: 配置客户端 `config/hazelcast-client.yaml`**

```yaml
hazelcast-client:
  cluster-name: seatunnel
  network:
    cluster-members:
      - 192.168.1.101:5801
      - 192.168.1.102:5801
```

### 2.3 引擎配置 (seatunnel.yaml)

```yaml
seatunnel:
  engine:
    # 类加载器缓存模式
    classloader-cache-mode: true
    # 历史作业保留时间（分钟）
    history-job-expire-minutes: 1440
    # 备份数量
    backup-count: 1

    # 动态 Slot
    slot-service:
      dynamic-slot: true

    # 检查点配置
    checkpoint:
      interval: 10000          # 检查点间隔（毫秒）
      timeout: 60000           # 超时时间
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          namespace: /tmp/seatunnel/checkpoint
          storage.type: hdfs
          fs.defaultFS: file:///tmp/

    # REST API
    http:
      enable-http: true
      port: 8080
```

### 2.4 JVM 配置

编辑 `config/jvm_options`:

```
-Xms2g
-Xmx2g
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp/seatunnel-heap-dump.hprof
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
```

### 2.5 与其他引擎集成

#### Flink 集成

```bash
./bin/start-seatunnel-flink-15-connector-v2.sh \
  --config config/my_job.conf
```

#### Spark 集成

```bash
./bin/start-seatunnel-spark-3-connector-v2.sh \
  --config config/my_job.conf \
  --master yarn \
  --deploy-mode cluster
```

---

## 三、作业配置详解

### 3.1 配置结构

SeaTunnel 使用 HOCON 格式配置，包含四个部分:

```hocon
env {
  # 全局环境配置
}

source {
  # 数据源配置（支持多个）
}

transform {
  # 数据转换配置（可选，支持多个）
}

sink {
  # 数据目标配置（支持多个）
}
```

### 3.2 env 配置项

```hocon
env {
  # 作业模式: BATCH（批处理）或 STREAMING（流处理）
  job.mode = "BATCH"

  # 作业名称
  job.name = "my_sync_job"

  # 全局并行度
  parallelism = 4

  # 检查点间隔（流处理模式必需）
  checkpoint.interval = 10000

  # 检查点超时
  checkpoint.timeout = 60000

  # 作业失败后是否自动重启
  job.retry.times = 3

  # 读取限流（行/秒）
  read_limit.rows_per_second = 10000

  # 写入限流（字节/秒）
  read_limit.bytes_per_second = 10485760
}
```

### 3.3 数据类型

SeaTunnel 支持的数据类型:

| 类型 | 说明 | 示例 |
|------|------|------|
| `boolean` | 布尔值 | true, false |
| `tinyint` | 1字节整数 | -128 ~ 127 |
| `smallint` | 2字节整数 | -32768 ~ 32767 |
| `int` | 4字节整数 | |
| `bigint` | 8字节整数 | |
| `float` | 单精度浮点 | |
| `double` | 双精度浮点 | |
| `decimal(p,s)` | 精确小数 | decimal(10,2) |
| `string` | 字符串 | |
| `bytes` | 字节数组 | |
| `date` | 日期 | 2024-01-01 |
| `time` | 时间 | 12:30:00 |
| `timestamp` | 时间戳 | |
| `array<T>` | 数组 | array<int> |
| `map<K,V>` | 映射 | map<string,int> |
| `row` | 嵌套结构 | |

### 3.4 Schema 定义

```hocon
schema = {
  fields {
    id = "bigint"
    name = "string"
    price = "decimal(10,2)"
    tags = "array<string>"
    metadata = "map<string,string>"
    address = {
      city = "string"
      zip = "string"
    }
  }
}
```

### 3.5 数据流连接

使用 `plugin_output` 和 `plugin_input` 连接数据流:

```hocon
source {
  MySQL-CDC {
    plugin_output = "cdc_data"
    # ...
  }
}

transform {
  Filter {
    plugin_input = "cdc_data"
    plugin_output = "filtered_data"
    # ...
  }
}

sink {
  Elasticsearch {
    plugin_input = "filtered_data"
    # ...
  }
}
```

---

## 四、数据源连接器

### 4.1 连接器概览

SeaTunnel 提供 **82 个**连接器，覆盖：

| 分类 | 连接器 |
|------|--------|
| 关系型数据库 | MySQL, PostgreSQL, Oracle, SQL Server, DB2, SQLite... |
| NoSQL | MongoDB, Redis, Cassandra, HBase, Neo4j... |
| 消息队列 | Kafka, Pulsar, RabbitMQ, RocketMQ... |
| 数据仓库 | ClickHouse, Doris, StarRocks, Hive... |
| 数据湖 | Iceberg, Hudi, Paimon... |
| 文件系统 | Local, HDFS, S3, OSS, COS, FTP... |
| CDC | MySQL-CDC, PostgreSQL-CDC, Oracle-CDC, MongoDB-CDC... |
| 其他 | Elasticsearch, HTTP, Email, DingTalk... |

### 4.2 JDBC 连接器

#### Source 配置

```hocon
source {
  Jdbc {
    plugin_output = "mysql_data"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/mydb"
    user = "root"
    password = "password"

    # 方式1: 指定表
    table = "users"

    # 方式2: 自定义查询
    query = "SELECT id, name, age FROM users WHERE status = 1"

    # 分区读取（提高性能）
    partition_column = "id"
    partition_num = 4
    partition_lower_bound = 1
    partition_upper_bound = 10000

    # 批量大小
    fetch_size = 1000
  }
}
```

#### Sink 配置

```hocon
sink {
  Jdbc {
    plugin_input = "source_data"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/target_db"
    user = "root"
    password = "password"
    table = "target_table"

    # 批量写入
    batch_size = 1000
    batch_interval_ms = 1000

    # 主键冲突处理
    primary_keys = ["id"]
    support_upsert_by_query_primary_key_exist = true
  }
}
```

### 4.3 MySQL CDC 连接器

```hocon
source {
  MySQL-CDC {
    plugin_output = "cdc_stream"

    # 连接配置
    hostname = "localhost"
    port = 3306
    username = "root"
    password = "password"

    # 库表配置（支持正则）
    database-names = ["mydb"]
    table-names = ["mydb.orders", "mydb.users"]

    # 启动模式
    # initial: 先全量后增量（默认）
    # earliest: 从最早binlog开始
    # latest: 仅增量
    # specific: 指定位点
    startup.mode = "initial"

    # 服务器ID（集群部署时每个节点需唯一）
    server-id = "5400-5404"

    # 时区
    server-time-zone = "Asia/Shanghai"
  }
}
```

### 4.4 Kafka 连接器

#### Source

```hocon
source {
  Kafka {
    plugin_output = "kafka_data"
    bootstrap.servers = "localhost:9092"
    topic = "my_topic"
    consumer.group = "seatunnel_group"

    # 起始位置: earliest, latest, specific
    start_mode = "earliest"

    # 数据格式
    format = "json"
    schema = {
      fields {
        id = "bigint"
        name = "string"
        timestamp = "timestamp"
      }
    }
  }
}
```

#### Sink

```hocon
sink {
  Kafka {
    plugin_input = "source_data"
    bootstrap.servers = "localhost:9092"
    topic = "output_topic"

    # 序列化格式: json, text, avro
    format = "json"

    # 分区策略
    partition_key_fields = ["id"]

    # 语义: AT_LEAST_ONCE, EXACTLY_ONCE
    semantics = "EXACTLY_ONCE"
  }
}
```

### 4.5 Elasticsearch 连接器

```hocon
sink {
  Elasticsearch {
    plugin_input = "source_data"
    hosts = ["http://localhost:9200"]
    index = "my_index"

    # 认证
    username = "elastic"
    password = "password"

    # 主键（用于更新/删除）
    primary_keys = ["id"]

    # 批量写入
    max_batch_size = 1000
    max_retry_count = 3
  }
}
```

### 4.6 文件连接器

#### 本地文件 Source

```hocon
source {
  LocalFile {
    plugin_output = "file_data"
    path = "/data/input/"
    file_format_type = "json"

    # 可选: json, csv, parquet, orc, text
    schema = {
      fields {
        id = "bigint"
        name = "string"
      }
    }
  }
}
```

#### S3 Sink

```hocon
sink {
  S3File {
    plugin_input = "source_data"
    path = "/output/data"
    bucket = "my-bucket"

    access_key = "your-access-key"
    secret_key = "your-secret-key"

    file_format_type = "parquet"

    # 分区
    partition_by = ["dt", "hour"]
    partition_dir_expression = "${k0}=${v0}/${k1}=${v1}"
  }
}
```

### 4.7 多表同步

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "cdc_tables"
    hostname = "localhost"
    port = 3306
    username = "root"
    password = "password"

    # 同步多个表
    database-names = ["db1", "db2"]
    table-names = ["db1.table1", "db1.table2", "db2.table3"]
  }
}

sink {
  Jdbc {
    plugin_input = "cdc_tables"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://target:3306"
    user = "root"
    password = "password"

    # 自动根据源表名写入对应目标表
    generate_sink_sql = true
    database = "target_db"
  }
}
```

---

## 五、数据转换

### 5.1 SQL 转换

最强大的转换方式，支持 SQL 语法:

```hocon
transform {
  Sql {
    plugin_input = "source_data"
    plugin_output = "transformed_data"

    query = """
      SELECT
        id,
        UPPER(name) as name,
        age + 1 as age,
        CASE WHEN gender = 'M' THEN '男' ELSE '女' END as gender,
        NOW() as process_time
      FROM source_data
      WHERE age > 18
    """
  }
}
```

支持的 SQL 函数：
- 字符串: `UPPER`, `LOWER`, `TRIM`, `SUBSTRING`, `CONCAT`, `REGEXP_REPLACE`
- 数学: `ABS`, `CEIL`, `FLOOR`, `ROUND`, `PI`
- 日期: `NOW`, `CURRENT_DATE`, `DATE_FORMAT`
- 条件: `CASE WHEN`, `COALESCE`, `NULLIF`

### 5.2 字段过滤 (Filter)

```hocon
transform {
  Filter {
    plugin_input = "source_data"
    plugin_output = "filtered_data"

    # 保留的字段
    include_fields = ["id", "name", "age"]

    # 或排除的字段
    # exclude_fields = ["password", "secret"]
  }
}
```

### 5.3 字段重命名 (Rename)

```hocon
transform {
  FieldMapper {
    plugin_input = "source_data"
    plugin_output = "renamed_data"

    field_mapper = {
      user_id = "id"
      user_name = "name"
      user_age = "age"
    }
  }
}
```

### 5.4 字段替换 (Replace)

```hocon
transform {
  Replace {
    plugin_input = "source_data"
    plugin_output = "replaced_data"

    replace_field = "phone"
    pattern = "(\\d{3})\\d{4}(\\d{4})"
    replacement = "$1****$2"
  }
}
```

### 5.5 字段分割 (Split)

```hocon
transform {
  Split {
    plugin_input = "source_data"
    plugin_output = "split_data"

    split_field = "full_address"
    separator = ","
    output_fields = ["province", "city", "district"]
  }
}
```

### 5.6 Copy 转换

```hocon
transform {
  Copy {
    plugin_input = "source_data"
    plugin_output = "copied_data"

    fields {
      name_backup = "name"
      id_string = "id"
    }
  }
}
```

### 5.7 JSON Path 提取

```hocon
transform {
  JsonPath {
    plugin_input = "source_data"
    plugin_output = "extracted_data"

    columns = [
      { src_field = "json_data", path = "$.user.name", dest_field = "user_name" },
      { src_field = "json_data", path = "$.user.age", dest_field = "user_age", dest_type = "int" }
    ]
  }
}
```

### 5.8 转换链

多个转换串联执行:

```hocon
transform {
  # 第一步：过滤字段
  Filter {
    plugin_input = "source_data"
    plugin_output = "step1"
    include_fields = ["id", "name", "age", "phone"]
  }

  # 第二步：SQL 处理
  Sql {
    plugin_input = "step1"
    plugin_output = "step2"
    query = "SELECT id, UPPER(name) as name, age, phone FROM step1 WHERE age >= 18"
  }

  # 第三步：脱敏
  Replace {
    plugin_input = "step2"
    plugin_output = "final_data"
    replace_field = "phone"
    pattern = "(\\d{3})\\d{4}(\\d{4})"
    replacement = "$1****$2"
  }
}
```

---

## 六、运维管理

### 6.1 REST API

SeaTunnel 提供 REST API 用于作业管理（默认端口 8080）:

#### 查看集群信息

```bash
curl http://localhost:8080/hazelcast/rest/cluster
```

#### 查看运行中的作业

```bash
curl http://localhost:8080/running-jobs
```

#### 查看作业详情

```bash
curl http://localhost:8080/job-info/<job_id>
```

#### 取消作业

```bash
curl -X POST http://localhost:8080/cancel-job/<job_id>
```

#### 查看系统监控信息

```bash
curl http://localhost:8080/overview
```

### 6.2 日志配置

编辑 `config/log4j2.properties`:

```properties
# 日志级别
rootLogger.level = INFO

# 应用日志
appender.rolling.type = RollingFile
appender.rolling.fileName = ${sys:seatunnel.logs.path}/seatunnel.log
appender.rolling.filePattern = ${sys:seatunnel.logs.path}/seatunnel-%d{yyyy-MM-dd}-%i.log.gz
appender.rolling.policies.size.size = 100MB
appender.rolling.strategy.max = 10
```

### 6.3 检查点存储

#### 本地文件存储（开发测试）

```yaml
checkpoint:
  storage:
    type: localfile
    max-retained: 3
    plugin-config:
      namespace: /tmp/seatunnel/checkpoint
```

#### HDFS 存储（生产环境）

```yaml
checkpoint:
  storage:
    type: hdfs
    max-retained: 3
    plugin-config:
      namespace: /seatunnel/checkpoint
      storage.type: hdfs
      fs.defaultFS: hdfs://namenode:8020
      # Kerberos 认证（可选）
      # kerberos.principal: seatunnel@REALM
      # kerberos.keytab: /path/to/keytab
```

#### S3 存储

```yaml
checkpoint:
  storage:
    type: hdfs
    plugin-config:
      namespace: /seatunnel/checkpoint
      storage.type: s3
      fs.defaultFS: s3a://bucket-name/
      fs.s3a.access.key: your-access-key
      fs.s3a.secret.key: your-secret-key
      fs.s3a.endpoint: s3.amazonaws.com
```

### 6.4 监控指标

启用 Prometheus 指标:

```yaml
seatunnel:
  engine:
    telemetry:
      metric:
        enabled: true
```

访问 `http://localhost:8080/metrics` 获取指标。

### 6.5 作业失败自动重启

```hocon
env {
  job.retry.times = 3
  job.retry.delay = 10000  # 毫秒
}
```

---

## 七、常见问题排查

### 7.1 作业启动失败

**问题**: `ClassNotFoundException` 或 `NoClassDefFoundError`

**解决**:
1. 确认连接器 JAR 在 `connectors/` 目录
2. 检查依赖是否完整
3. 清理缓存: 删除 `~/.cache/seatunnel/`

### 7.2 内存不足

**问题**: `OutOfMemoryError`

**解决**:
1. 调整 `config/jvm_options` 中的堆内存
2. 减少并行度
3. 启用限流:
   ```hocon
   env {
     read_limit.rows_per_second = 5000
   }
   ```

### 7.3 检查点失败

**问题**: `Checkpoint timeout` 或存储写入失败

**解决**:
1. 增加超时时间:
   ```yaml
   checkpoint:
     timeout: 120000
   ```
2. 检查存储路径权限
3. 确认存储服务可用

### 7.4 CDC 连接问题

**问题**: 无法连接 MySQL binlog

**解决**:
1. 确认 MySQL 开启 binlog:
   ```sql
   SHOW VARIABLES LIKE 'log_bin';
   ```
2. 检查 binlog 格式:
   ```sql
   SHOW VARIABLES LIKE 'binlog_format';  -- 需要 ROW
   ```
3. 确认用户权限:
   ```sql
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user';
   ```

### 7.5 Kafka 消费问题

**问题**: 消费延迟或重复消费

**解决**:
1. 检查消费组是否被其他程序占用
2. 调整 `start_mode` 配置
3. 确认 Kafka 分区数与并行度匹配

### 7.6 作业卡住不动

**问题**: 作业长时间无进度

**排查**:
1. 查看日志:
   ```bash
   tail -f logs/seatunnel.log
   ```
2. 检查 REST API:
   ```bash
   curl http://localhost:8080/running-jobs
   ```
3. 检查数据源是否有数据
4. 确认网络连接正常

### 7.7 类型转换错误

**问题**: `DataTypeConvertException`

**解决**:
1. 检查 schema 定义与实际数据类型匹配
2. 使用 SQL 转换进行类型转换:
   ```hocon
   transform {
     Sql {
       query = "SELECT CAST(id AS BIGINT) as id, name FROM source"
     }
   }
   ```

---

## 附录

### A. 命令行参数

```bash
./bin/seatunnel.sh [options]

参数:
  --config, -c      配置文件路径（必需）
  -e, --deploy-mode 部署模式: local, cluster
  --name, -n        作业名称
  --variable, -i    变量替换，如 -i key1=value1 -i key2=value2
  --help, -h        显示帮助
```

### B. 配置变量替换

配置文件支持变量:

```hocon
source {
  Jdbc {
    url = "${DB_URL}"
    user = "${DB_USER}"
    password = "${DB_PASSWORD}"
  }
}
```

运行时传入:

```bash
./bin/seatunnel.sh --config job.conf \
  -i DB_URL="jdbc:mysql://localhost:3306/db" \
  -i DB_USER="root" \
  -i DB_PASSWORD="secret"
```

### C. 支持的连接器列表

**Source 连接器** (数据读取):
- 数据库: JDBC, MySQL-CDC, PostgreSQL-CDC, Oracle-CDC, MongoDB-CDC, SQL Server-CDC
- 消息队列: Kafka, Pulsar, RabbitMQ, RocketMQ
- 文件: LocalFile, HDFS, S3, OSS, FTP, SFTP
- 其他: Elasticsearch, HTTP, FakeSource...

**Sink 连接器** (数据写入):
- 数据库: JDBC, ClickHouse, Doris, StarRocks, Hive
- 消息队列: Kafka, Pulsar, RabbitMQ
- 搜索引擎: Elasticsearch
- 文件: LocalFile, HDFS, S3, OSS
- 其他: Console, Email, DingTalk...

---

*文档结束*

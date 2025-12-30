# SeaTunnel Examples 模块源码分析
> 文档编号: 016
> 模块路径: seatunnel-examples/
> 更新日期: 2025-12-29
---

## 一、模块概述

### 1.1 模块定位

`seatunnel-examples` 模块是 SeaTunnel 的**示例代码模块**，提供：

1. **IDE 运行示例** - 在 IDE 中直接运行 SeaTunnel 作业
2. **各引擎示例** - Zeta/Flink/Spark 三种引擎的使用示例
3. **调试入口** - 方便开发者调试连接器和转换器
4. **快速验证** - 验证配置文件和连接器功能

### 1.2 模块结构

```
seatunnel-examples/
├── pom.xml                              # 父POM
├── seatunnel-engine-examples/           # Zeta 引擎示例
│   ├── pom.xml
│   └── src/main/
│       ├── java/.../engine/
│       │   ├── SeaTunnelEngineLocalExample.java      # 本地模式
│       │   ├── SeaTunnelEngineClusterServerExample.java  # 集群服务端
│       │   └── SeaTunnelEngineClusterClientExample.java  # 集群客户端
│       └── resources/examples/
│           └── fake_to_console.conf
├── seatunnel-flink-examples/            # Flink 引擎示例
│   ├── pom.xml
│   ├── seatunnel-flink-13-example/      # Flink 1.13
│   ├── seatunnel-flink-15-example/      # Flink 1.15
│   └── seatunnel-flink-20-example/      # Flink 2.0
└── seatunnel-spark-connector-v2-example/ # Spark 引擎示例
    ├── pom.xml
    └── src/main/
        ├── java/.../spark/v2/
        │   ├── SeaTunnelApiExample.java
        │   └── ExampleUtils.java
        └── resources/examples/
            └── spark.batch.conf
```

### 1.3 文件统计

| 子模块 | Java文件 | 配置文件 | 说明 |
|--------|----------|----------|------|
| seatunnel-engine-examples | 3 | 1 | Zeta 引擎 |
| seatunnel-flink-13-example | 2 | 2 | Flink 1.13 |
| seatunnel-flink-15-example | 2 | 2 | Flink 1.15 |
| seatunnel-flink-20-example | 2 | 2 | Flink 2.0 |
| seatunnel-spark-connector-v2-example | 2 | 1 | Spark 2.4 |
| **合计** | **11** | **8** | |

---

## 二、Zeta 引擎示例

### 2.1 依赖配置

```xml
<dependencies>
    <!-- 核心启动器 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-starter</artifactId>
    </dependency>

    <!-- 转换插件 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-transforms-v2</artifactId>
    </dependency>

    <!-- Hadoop 支持 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-hadoop3-3.1.4-uber</artifactId>
    </dependency>

    <!-- 连接器 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>connector-fake</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>connector-console</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>connector-assert</artifactId>
    </dependency>
</dependencies>
```

### 2.2 本地模式示例

`SeaTunnelEngineLocalExample.java` - 在单 JVM 中运行作业：

```java
public class SeaTunnelEngineLocalExample {

    static {
        // 配置 Log4j2 继承 ThreadContext
        System.setProperty("log4j2.isThreadContextMapInheritable", "true");
    }

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        // 默认配置文件路径
        String configurePath = args.length > 0 ? args[0] : "/examples/fake_to_console.conf";
        String configFile = getTestConfigFile(configurePath);

        // 构建客户端命令参数
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setCheckConfig(false);
        clientCommandArgs.setJobName(Paths.get(configFile).getFileName().toString());

        // 设置为本地模式
        clientCommandArgs.setMasterType(MasterType.LOCAL);

        // 运行作业
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelEngineLocalExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
```

**使用方式**：
1. 在 IDE 中直接运行 `main` 方法
2. 或通过命令行参数指定配置文件：`/path/to/your.conf`

### 2.3 集群服务端示例

`SeaTunnelEngineClusterServerExample.java` - 启动 Zeta 集群节点：

```java
public class SeaTunnelEngineClusterServerExample {

    static {
        System.setProperty("log4j2.isThreadContextMapInheritable", "true");
    }

    public static void main(String[] args) throws CommandException {
        // 构建服务端命令参数
        ServerCommandArgs serverCommandArgs = new ServerCommandArgs();

        // 启动集群节点
        SeaTunnel.run(serverCommandArgs.buildCommand());
    }
}
```

**说明**：启动后会创建一个 Hazelcast 集群节点，等待客户端提交作业。

### 2.4 集群客户端示例

`SeaTunnelEngineClusterClientExample.java` - 向集群提交作业：

```java
public class SeaTunnelEngineClusterClientExample {

    public static void main(String[] args) throws Exception {
        String id = "834720088434147329";  // 作业 ID
        String configurePath = "/examples/fake_to_console.conf";

        // 提交作业
        submit(configurePath, id);

        // 其他操作示例
        // list();           // 列出所有作业
        // savepoint(id);    // 创建保存点
        // restore(configurePath, id);  // 从保存点恢复
        // cancel(id);       // 取消作业
    }

    // 提交作业
    public static void submit(String configurePath, String id)
            throws FileNotFoundException, URISyntaxException {
        String configFile = SeaTunnelEngineLocalExample.getTestConfigFile(configurePath);

        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setCheckConfig(false);
        clientCommandArgs.setJobName(Paths.get(configFile).getFileName().toString());
        clientCommandArgs.setAsync(true);           // 异步提交
        clientCommandArgs.setCustomJobId(id);       // 自定义作业 ID

        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    // 列出作业
    public static void list() {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setListJob(true);
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    // 创建保存点
    public static void savepoint(String id) {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setSavePointJobId(id);
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    // 从保存点恢复
    public static void restore(String configurePath, String id)
            throws FileNotFoundException, URISyntaxException {
        String configFile = SeaTunnelEngineLocalExample.getTestConfigFile(configurePath);

        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setRestoreJobId(id);
        clientCommandArgs.setAsync(true);

        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    // 取消作业
    public static void cancel(String id) {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setCancelJobId(Collections.singletonList(id));
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }
}
```

### 2.5 示例配置文件

`fake_to_console.conf` - 最简单的批处理作业：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
}

sink {
  console {
    plugin_input = "fake"
  }
}
```

---

## 三、Flink 引擎示例

### 3.1 模块结构

```
seatunnel-flink-examples/
├── pom.xml                      # 父POM，公共依赖
├── seatunnel-flink-13-example/  # Flink 1.13 示例
├── seatunnel-flink-15-example/  # Flink 1.15 示例
└── seatunnel-flink-20-example/  # Flink 2.0 示例
```

### 3.2 公共依赖

```xml
<dependencies>
    <!-- HOCON 配置解析 -->
    <dependency>
        <groupId>com.typesafe</groupId>
        <artifactId>config</artifactId>
    </dependency>

    <!-- 转换插件 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-transforms-v2</artifactId>
    </dependency>

    <!-- 连接器 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>connector-fake</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>connector-console</artifactId>
    </dependency>
</dependencies>
```

### 3.3 Flink 1.15 特定依赖

```xml
<dependencies>
    <!-- SeaTunnel Flink 1.15 启动器 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-flink-15-starter</artifactId>
    </dependency>

    <!-- Flink 核心依赖 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-java</artifactId>
        <version>${flink.1.15.3.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>${flink.1.15.3.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>${flink.1.15.3.version}</version>
    </dependency>

    <!-- Flink Table API -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-api-java</artifactId>
        <version>${flink.1.15.3.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-api-java-bridge</artifactId>
        <version>${flink.1.15.3.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-planner-loader</artifactId>
        <version>${flink.1.15.3.version}</version>
    </dependency>

    <!-- Flink Web UI -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-runtime-web</artifactId>
        <version>${flink.1.15.3.version}</version>
    </dependency>
</dependencies>
```

### 3.4 批处理示例

`SeaTunnelBatchJobExample.java`：

```java
public class SeaTunnelBatchJobExample {

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        // 默认使用批处理配置
        String configurePath = args.length > 0 ? args[0] : "/examples/fake_to_console_batch.conf";
        String configFile = getTestConfigFile(configurePath);

        // 构建 Flink 命令参数
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);

        // 运行作业
        SeaTunnel.run(flinkCommandArgs.buildCommand());
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelBatchJobExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
```

### 3.5 流处理示例

`SeaTunnelStreamingJobExample.java`：

```java
public class SeaTunnelStreamingJobExample {

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        // 默认使用流处理配置
        String configurePath = args.length > 0 ? args[0] : "/examples/fake_to_console_streaming.conf";
        String configFile = getTestConfigFile(configurePath);

        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);

        SeaTunnel.run(flinkCommandArgs.buildCommand());
    }
}
```

### 3.6 Flink 配置文件

**fake_to_console_batch.conf**（批处理）：
```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  console {
    plugin_input = "fake"
  }
}
```

**fake_to_console_streaming.conf**（流处理）：
```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  console {
    plugin_input = "fake"
  }
}
```

---

## 四、Spark 引擎示例

### 4.1 依赖配置

```xml
<dependencies>
    <!-- SeaTunnel Spark 2 启动器 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-spark-2-starter</artifactId>
    </dependency>

    <!-- 转换插件 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-transforms-v2</artifactId>
    </dependency>

    <!-- 连接器 -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>connector-fake</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>connector-console</artifactId>
    </dependency>

    <!-- Spark 核心依赖 -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_${scala.binary.version}</artifactId>
        <version>${spark.2.4.0.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_${scala.binary.version}</artifactId>
        <version>${spark.2.4.0.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-streaming_${scala.binary.version}</artifactId>
        <version>${spark.2.4.0.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-hive_${scala.binary.version}</artifactId>
        <version>${spark.2.4.0.version}</version>
    </dependency>
</dependencies>
```

### 4.2 示例代码

**SeaTunnelApiExample.java**：

```java
public class SeaTunnelApiExample {

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        String configurePath = args.length > 0 ? args[0] : "/examples/spark.batch.conf";
        ExampleUtils.builder(configurePath);
    }
}
```

**ExampleUtils.java**：

```java
public class ExampleUtils {

    public static void builder(String configurePath)
            throws FileNotFoundException, URISyntaxException, CommandException {
        String configFile = getTestConfigFile(configurePath);

        // 构建 Spark 命令参数
        SparkCommandArgs sparkCommandArgs = new SparkCommandArgs();
        sparkCommandArgs.setConfigFile(configFile);
        sparkCommandArgs.setCheckConfig(false);
        sparkCommandArgs.setVariables(null);
        sparkCommandArgs.setDeployMode(DeployMode.CLIENT);  // 客户端模式

        // 运行作业
        SeaTunnel.run(sparkCommandArgs.buildCommand());
    }

    private static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelApiExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
```

### 4.3 Spark 配置文件

`spark.batch.conf` - 包含 Spark 特定配置：

```hocon
env {
  job.name = "SeaTunnel"
  spark.executor.instances = 1
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
}

source {
  FakeSource {
    row.num = 16
    parallelism = 2
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    plugin_output = "fake"
  }
}

transform {
  sql {
    plugin_input = "fake"
    query = "select c_map,c_array,c_string,c_boolean,c_tinyint,c_smallint,c_int,c_bigint,c_float,c_double,c_null,c_bytes,c_date,c_timestamp from dual"
    plugin_output = "sql"
  }
}

sink {
  Console {
    parallelism = 2
  }
}
```

---

## 五、核心 API 说明

### 5.1 SeaTunnel 入口类

所有示例最终都调用 `SeaTunnel.run()` 方法：

```java
// 核心入口
SeaTunnel.run(Command command);
```

### 5.2 命令参数类

| 引擎 | 参数类 | 主要配置 |
|------|--------|----------|
| Zeta | `ClientCommandArgs` | configFile, masterType, async, jobId |
| Zeta | `ServerCommandArgs` | 集群配置 |
| Flink | `FlinkCommandArgs` | configFile, variables |
| Spark | `SparkCommandArgs` | configFile, deployMode, variables |

### 5.3 MasterType 枚举

```java
public enum MasterType {
    LOCAL,    // 本地模式（单 JVM）
    CLUSTER   // 集群模式（连接远程集群）
}
```

### 5.4 DeployMode 枚举

```java
public enum DeployMode {
    CLIENT,   // 客户端模式（Driver 在本地）
    CLUSTER   // 集群模式（Driver 在集群）
}
```

---

## 六、运行指南

### 6.1 在 IDE 中运行

1. **导入项目**
   ```bash
   cd seatunnel
   ./mvnw clean install -DskipTests
   ```

2. **选择示例模块**
   - Zeta: `seatunnel-engine-examples`
   - Flink: `seatunnel-flink-15-example`
   - Spark: `seatunnel-spark-connector-v2-example`

3. **运行 main 方法**
   - 右键点击示例类 → Run

### 6.2 Zeta 引擎本地模式

```java
// 直接运行
SeaTunnelEngineLocalExample.main(new String[]{});

// 指定配置文件
SeaTunnelEngineLocalExample.main(new String[]{"/path/to/config.conf"});
```

### 6.3 Zeta 引擎集群模式

```java
// 步骤 1: 启动服务端
SeaTunnelEngineClusterServerExample.main(new String[]{});

// 步骤 2: 提交作业（另一个进程）
SeaTunnelEngineClusterClientExample.main(new String[]{});
```

### 6.4 Flink 引擎

```java
// 批处理
SeaTunnelBatchJobExample.main(new String[]{});

// 流处理
SeaTunnelStreamingJobExample.main(new String[]{});
```

### 6.5 Spark 引擎

```java
// 运行示例
SeaTunnelApiExample.main(new String[]{});
```

---

## 七、添加自定义连接器

### 7.1 添加依赖

在 `pom.xml` 中添加连接器依赖：

```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-jdbc</artifactId>
    <version>${project.version}</version>
</dependency>

<!-- JDBC 驱动 -->
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>8.0.27</version>
</dependency>
```

### 7.2 创建配置文件

```hocon
# src/main/resources/examples/jdbc_to_console.conf
env {
  parallelism = 1
  job.mode = "BATCH"
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

sink {
  Console {
    plugin_input = "jdbc_source"
  }
}
```

### 7.3 运行自定义作业

```java
public class JdbcExample {
    public static void main(String[] args) throws Exception {
        String configurePath = "/examples/jdbc_to_console.conf";
        String configFile = SeaTunnelEngineLocalExample.getTestConfigFile(configurePath);

        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setMasterType(MasterType.LOCAL);

        SeaTunnel.run(clientCommandArgs.buildCommand());
    }
}
```

---

## 八、调试技巧

### 8.1 日志配置

在 `src/main/resources/log4j2.properties` 中配置日志级别：

```properties
rootLogger.level = INFO
rootLogger.appenderRef.console.ref = consoleAppender

# 调试连接器
logger.connector.name = org.apache.seatunnel.connectors
logger.connector.level = DEBUG

# 调试引擎
logger.engine.name = org.apache.seatunnel.engine
logger.engine.level = DEBUG
```

### 8.2 断点调试

1. 在连接器代码中设置断点
2. 运行示例的 `main` 方法（Debug 模式）
3. 程序会在断点处暂停

### 8.3 查看数据流

使用 Console Sink 查看中间数据：

```hocon
sink {
  Console {
    plugin_input = "your_source"
  }
}
```

### 8.4 常见问题

**ClassNotFoundException**：
- 检查依赖是否正确添加
- 确保 Maven 已刷新

**配置文件找不到**：
- 检查文件路径是否正确
- 确保文件在 `src/main/resources` 目录下

**端口冲突**：
- Zeta 集群模式使用 5801 端口
- Flink Web UI 使用 8081 端口
- 检查端口占用情况

---

## 九、与其他模块的关系

### 9.1 依赖图

```
seatunnel-examples
├── seatunnel-engine-examples
│   └── seatunnel-starter (Zeta 引擎)
│       └── seatunnel-engine-server
│
├── seatunnel-flink-examples
│   └── seatunnel-flink-*-starter
│       └── seatunnel-translation-flink
│
└── seatunnel-spark-connector-v2-example
    └── seatunnel-spark-*-starter
        └── seatunnel-translation-spark
```

### 9.2 调用链

```
Example.main()
    │
    ▼
SeaTunnel.run(Command)
    │
    ▼
Command.execute()
    │
    ├── Zeta: SeaTunnelClient / SeaTunnelServer
    ├── Flink: FlinkStarter
    └── Spark: SparkStarter
```

---

## 十、总结

### 10.1 模块特点

| 特点 | 说明 |
|------|------|
| 多引擎支持 | Zeta/Flink 1.13-2.0/Spark 2.4 |
| IDE 友好 | 直接运行 main 方法 |
| 配置灵活 | 支持自定义配置文件路径 |
| 调试方便 | 可设置断点调试连接器 |

### 10.2 示例清单

| 示例类 | 引擎 | 模式 |
|--------|------|------|
| SeaTunnelEngineLocalExample | Zeta | 本地单机 |
| SeaTunnelEngineClusterServerExample | Zeta | 集群服务端 |
| SeaTunnelEngineClusterClientExample | Zeta | 集群客户端 |
| SeaTunnelBatchJobExample | Flink | 批处理 |
| SeaTunnelStreamingJobExample | Flink | 流处理 |
| SeaTunnelApiExample | Spark | 批处理 |

### 10.3 快速开始

1. 构建项目：`./mvnw clean install -DskipTests`
2. 在 IDE 中打开 `seatunnel-engine-examples`
3. 运行 `SeaTunnelEngineLocalExample.main()`
4. 查看控制台输出

---

## 附录A：配置文件模板

### A.1 基础批处理

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "bigint"
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  Filter {
    plugin_input = "fake"
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

### A.2 流处理（带 Checkpoint）

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
  checkpoint.timeout = 60000
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        id = "bigint"
        name = "string"
        timestamp = "timestamp"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "fake"
  }
}
```

---

## 附录B：命令参数详解

### ClientCommandArgs（Zeta）

| 参数 | 类型 | 说明 |
|------|------|------|
| configFile | String | 配置文件路径 |
| checkConfig | boolean | 是否只检查配置 |
| jobName | String | 作业名称 |
| masterType | MasterType | LOCAL/CLUSTER |
| async | boolean | 异步提交 |
| customJobId | String | 自定义作业 ID |
| listJob | boolean | 列出作业 |
| cancelJobId | List<String> | 取消作业 |
| savePointJobId | String | 保存点作业 ID |
| restoreJobId | String | 恢复作业 ID |

### FlinkCommandArgs

| 参数 | 类型 | 说明 |
|------|------|------|
| configFile | String | 配置文件路径 |
| checkConfig | boolean | 是否只检查配置 |
| variables | List<String> | 变量替换 |

### SparkCommandArgs

| 参数 | 类型 | 说明 |
|------|------|------|
| configFile | String | 配置文件路径 |
| checkConfig | boolean | 是否只检查配置 |
| variables | List<String> | 变量替换 |
| deployMode | DeployMode | CLIENT/CLUSTER |

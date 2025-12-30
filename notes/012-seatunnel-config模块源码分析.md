# SeaTunnel Config 模块源码分析
> 文档编号: 012
> 模块路径: seatunnel-config/
> 更新日期: 2025-12-29
---

## 一、模块概述

### 1.1 模块定位

`seatunnel-config` 模块是 SeaTunnel 的配置处理核心模块，基于 `com.typesafe.config` (HOCON) 进行增强和封装，提供：

1. **HOCON 配置解析** - 支持标准 HOCON 语法的配置文件解析
2. **SQL 配置转换** - 将 SQL DDL 语法转换为 HOCON 配置
3. **Shade 封装** - 防止依赖冲突的包重定向

### 1.2 子模块结构

```
seatunnel-config/
├── pom.xml                      # 父POM
├── README.md                    # 模块说明
├── seatunnel-config-base/       # 基础配置接口（空模块，预留）
├── seatunnel-config-shade/      # Typesafe Config 的 Shade 封装
└── seatunnel-config-sql/        # SQL 到 HOCON 配置转换
```

### 1.3 文件统计

| 子模块              | Java文件数 | 主要功能           |
|---------------------|------------|-------------------|
| seatunnel-config-base  | 0       | 预留接口模块       |
| seatunnel-config-shade | 18      | HOCON 解析增强     |
| seatunnel-config-sql   | 18      | SQL 配置解析       |
| **合计**            | **36**     |                   |

---

## 二、Config Shade 子模块

### 2.1 模块说明

`seatunnel-config-shade` 模块基于 `com.typesafe:config` 进行 Shade（包重定向），将原始包名：
```
com.typesafe.config.*
```
重定向为：
```
org.apache.seatunnel.shade.com.typesafe.config.*
```

这样做的目的是避免与其他使用 Typesafe Config 的项目产生依赖冲突。

### 2.2 Maven Shade 配置

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <relocations>
                    <relocation>
                        <pattern>com.typesafe.config</pattern>
                        <shadedPattern>org.apache.seatunnel.shade.com.typesafe.config</shadedPattern>
                    </relocation>
                </relocations>
            </configuration>
        </execution>
    </executions>
</plugin>
```

### 2.3 核心类：ConfigParseOptions

`ConfigParseOptions` 是 HOCON 解析的配置选项类，使用不可变对象模式：

```java
package org.apache.seatunnel.shade.com.typesafe.config;

public final class ConfigParseOptions {
    // 路径分隔符常量
    public static final String PATH_TOKEN_SEPARATOR = "->";

    // 配置属性
    final ConfigSyntax syntax;          // 语法类型（JSON/CONF/PROPERTIES）
    final String originDescription;     // 来源描述
    final boolean allowMissing;         // 是否允许文件缺失
    final ConfigIncluder includer;      // include 解析器
    final ClassLoader classLoader;      // 类加载器

    // 静态工厂方法
    public static ConfigParseOptions defaults() {
        return new ConfigParseOptions(null, null, true, null, null);
    }

    // Builder 风格的修改方法（返回新对象）
    public ConfigParseOptions setSyntax(ConfigSyntax syntax) {
        return new ConfigParseOptions(syntax, this.originDescription,
            this.allowMissing, this.includer, this.classLoader);
    }

    public ConfigParseOptions setOriginDescription(String originDescription) {
        return new ConfigParseOptions(this.syntax, originDescription,
            this.allowMissing, this.includer, this.classLoader);
    }

    public ConfigParseOptions setAllowMissing(boolean allowMissing) {
        return new ConfigParseOptions(this.syntax, this.originDescription,
            allowMissing, this.includer, this.classLoader);
    }

    public ConfigParseOptions setIncluder(ConfigIncluder includer) {
        return new ConfigParseOptions(this.syntax, this.originDescription,
            this.allowMissing, includer, this.classLoader);
    }

    public ConfigParseOptions setClassLoader(ClassLoader classLoader) {
        return new ConfigParseOptions(this.syntax, this.originDescription,
            this.allowMissing, this.includer, classLoader);
    }
}
```

### 2.4 ConfigSyntax 枚举

```java
public enum ConfigSyntax {
    JSON,       // 标准 JSON 格式
    CONF,       // HOCON 格式
    PROPERTIES  // Java Properties 格式
}
```

### 2.5 路径解析增强

SeaTunnel 增强了路径解析功能，支持 `->` 分隔符：

```java
// 原始 Typesafe Config 使用 . 作为路径分隔符
config.getString("source.jdbc.url")

// SeaTunnel 扩展支持 -> 分隔符（用于特殊场景）
public static final String PATH_TOKEN_SEPARATOR = "->";
```

---

## 三、Config SQL 子模块

### 3.1 模块说明

`seatunnel-config-sql` 模块提供 SQL DDL 语法到 HOCON 配置的转换能力，让用户可以使用熟悉的 SQL 语法定义数据同步任务。

### 3.2 依赖配置

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-config-shade</artifactId>
    </dependency>
    <dependency>
        <groupId>com.github.jsqlparser</groupId>
        <artifactId>jsqlparser</artifactId>
    </dependency>
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
    </dependency>
</dependencies>
```

### 3.3 SQL 配置语法示例

```sql
-- 环境配置
SET 'job.mode' = 'BATCH';
SET 'parallelism' = '2';

-- 定义 Source（CREATE TABLE）
CREATE TABLE source_table (
    id INT,
    name STRING,
    age INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/test',
    'table' = 'users',
    'user' = 'root',
    'password' = 'password'
);

-- 定义 Sink（CREATE TABLE）
CREATE TABLE sink_table (
    id INT,
    name STRING,
    age INT
) WITH (
    'connector' = 'console'
);

-- 定义数据流（INSERT INTO SELECT）
INSERT INTO sink_table
SELECT id, name, age FROM source_table WHERE age > 18;
```

### 3.4 核心类：SqlConfigBuilder

`SqlConfigBuilder` 是 SQL 配置解析的核心类，将 SQL 语法转换为 `SeaTunnelConfig` 对象：

```java
package org.apache.seatunnel.config.sql;

@Slf4j
public class SqlConfigBuilder {

    private String sqlContent;

    // 静态工厂方法
    public static SqlConfigBuilder of(Path sqlFilePath) throws IOException {
        String content = new String(Files.readAllBytes(sqlFilePath), StandardCharsets.UTF_8);
        return of(content);
    }

    public static SqlConfigBuilder of(String sqlContent) {
        SqlConfigBuilder builder = new SqlConfigBuilder();
        builder.sqlContent = sqlContent;
        return builder;
    }

    // 构建配置
    public SeaTunnelConfig build() {
        SeaTunnelConfig config = new SeaTunnelConfig();

        // 分割 SQL 语句
        List<String> statements = splitStatements(sqlContent);

        for (String statement : statements) {
            statement = statement.trim();
            if (statement.isEmpty()) continue;

            // 解析不同类型的 SQL 语句
            if (isSetStatement(statement)) {
                parseSetStatement(statement, config);
            } else if (isCreateTableStatement(statement)) {
                parseCreateTableStatement(statement, config);
            } else if (isCreateAsStatement(statement)) {
                parseCreateAsStatement(statement, config);
            } else if (isInsertStatement(statement)) {
                parseInsertStatement(statement, config);
            }
        }

        return config;
    }

    // SET 语句解析 -> env 配置
    private void parseSetStatement(String statement, SeaTunnelConfig config) {
        // SET 'key' = 'value';
        Pattern pattern = Pattern.compile(
            "SET\\s+'([^']+)'\\s*=\\s*'([^']*)'",
            Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = pattern.matcher(statement);
        if (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2);
            config.getEnvConfigs().add(key + " = \"" + value + "\"");
        }
    }

    // CREATE TABLE 语句解析 -> source/sink 配置
    private void parseCreateTableStatement(String statement, SeaTunnelConfig config) {
        try {
            Statement stmt = CCJSqlParserUtil.parse(statement);
            if (stmt instanceof CreateTable) {
                CreateTable createTable = (CreateTable) stmt;

                String tableName = createTable.getTable().getName();
                List<ColumnDefinition> columns = createTable.getColumnDefinitions();
                Map<String, String> options = parseWithOptions(createTable);

                // 根据 connector 类型判断是 source 还是 sink
                String connector = options.get("connector");
                if (connector != null) {
                    TableConfig tableConfig = buildTableConfig(tableName, columns, options);
                    // 存储到 config 中
                }
            }
        } catch (JSQLParserException e) {
            log.error("Failed to parse CREATE TABLE: {}", statement, e);
        }
    }

    // INSERT INTO SELECT 语句解析 -> 数据流定义
    private void parseInsertStatement(String statement, SeaTunnelConfig config) {
        try {
            Statement stmt = CCJSqlParserUtil.parse(statement);
            if (stmt instanceof Insert) {
                Insert insert = (Insert) stmt;

                String targetTable = insert.getTable().getName();
                Select select = insert.getSelect();

                // 解析 SELECT 子句获取源表和转换逻辑
                if (select != null) {
                    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                    // 解析 FROM、WHERE、SELECT 列表等
                }
            }
        } catch (JSQLParserException e) {
            log.error("Failed to parse INSERT: {}", statement, e);
        }
    }
}
```

### 3.5 数据模型

#### SeaTunnelConfig

```java
package org.apache.seatunnel.config.sql.model;

@Data
public class SeaTunnelConfig {
    private List<String> envConfigs = new ArrayList<>();
    private List<SourceConfig> sourceConfigs = new ArrayList<>();
    private List<TransformConfig> transformConfigs = new ArrayList<>();
    private List<SinkConfig> sinkConfigs = new ArrayList<>();
}
```

#### SourceConfig

```java
@Data
public class SourceConfig {
    private String tableName;
    private String connector;
    private Map<String, String> options = new LinkedHashMap<>();
    private List<ColumnConfig> columns = new ArrayList<>();
}
```

#### SinkConfig

```java
@Data
public class SinkConfig {
    private String tableName;
    private String connector;
    private Map<String, String> options = new LinkedHashMap<>();
    private String sourceTable;  // INSERT INTO 的目标表
}
```

#### TransformConfig

```java
@Data
public class TransformConfig {
    private String tableName;
    private String type;  // SQL, Filter, etc.
    private String query;
    private Map<String, String> options = new LinkedHashMap<>();
}
```

#### ColumnConfig

```java
@Data
public class ColumnConfig {
    private String name;
    private String type;
    private boolean nullable = true;
    private String defaultValue;
    private String comment;
}
```

### 3.6 ConfigTemplate：HOCON 生成器

`ConfigTemplate` 将 `SeaTunnelConfig` 对象转换为 HOCON 格式字符串：

```java
package org.apache.seatunnel.config.sql;

public class ConfigTemplate {

    private final SeaTunnelConfig config;

    public ConfigTemplate(SeaTunnelConfig config) {
        this.config = config;
    }

    // 生成完整 HOCON 配置
    public String generate() {
        StringBuilder sb = new StringBuilder();

        // env 块
        sb.append(globalConfig());
        sb.append("\n");

        // source 块
        sb.append(sourceItems());
        sb.append("\n");

        // transform 块（如果存在）
        if (!config.getTransformConfigs().isEmpty()) {
            sb.append(transformItems());
            sb.append("\n");
        }

        // sink 块
        sb.append(sinkItems());

        return sb.toString();
    }

    // 生成 env 配置块
    public String globalConfig() {
        StringBuilder sb = new StringBuilder();
        sb.append("env {\n");
        for (String envConfig : config.getEnvConfigs()) {
            sb.append("  ").append(envConfig).append("\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    // 生成 source 配置块
    public String sourceItems() {
        StringBuilder sb = new StringBuilder();
        sb.append("source {\n");

        for (SourceConfig source : config.getSourceConfigs()) {
            sb.append("  ").append(source.getConnector()).append(" {\n");
            sb.append("    result_table_name = \"").append(source.getTableName()).append("\"\n");

            for (Map.Entry<String, String> entry : source.getOptions().entrySet()) {
                if (!"connector".equals(entry.getKey())) {
                    sb.append("    ").append(entry.getKey())
                      .append(" = \"").append(entry.getValue()).append("\"\n");
                }
            }

            sb.append("  }\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    // 生成 transform 配置块
    public String transformItems() {
        StringBuilder sb = new StringBuilder();
        sb.append("transform {\n");

        for (TransformConfig transform : config.getTransformConfigs()) {
            sb.append("  ").append(transform.getType()).append(" {\n");
            sb.append("    source_table_name = \"").append(transform.getTableName()).append("\"\n");

            if (transform.getQuery() != null) {
                sb.append("    query = \"").append(escapeString(transform.getQuery())).append("\"\n");
            }

            for (Map.Entry<String, String> entry : transform.getOptions().entrySet()) {
                sb.append("    ").append(entry.getKey())
                  .append(" = \"").append(entry.getValue()).append("\"\n");
            }

            sb.append("  }\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    // 生成 sink 配置块
    public String sinkItems() {
        StringBuilder sb = new StringBuilder();
        sb.append("sink {\n");

        for (SinkConfig sink : config.getSinkConfigs()) {
            sb.append("  ").append(sink.getConnector()).append(" {\n");

            if (sink.getSourceTable() != null) {
                sb.append("    source_table_name = \"").append(sink.getSourceTable()).append("\"\n");
            }

            for (Map.Entry<String, String> entry : sink.getOptions().entrySet()) {
                if (!"connector".equals(entry.getKey())) {
                    sb.append("    ").append(entry.getKey())
                      .append(" = \"").append(entry.getValue()).append("\"\n");
                }
            }

            sb.append("  }\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    private String escapeString(String str) {
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n");
    }
}
```

---

## 四、HOCON 配置格式详解

### 4.1 HOCON 简介

HOCON (Human-Optimized Config Object Notation) 是 Typesafe 开发的配置格式，是 JSON 的超集，具有以下特点：

1. **更简洁** - 可省略引号、逗号、根大括号
2. **支持注释** - `#` 和 `//` 风格注释
3. **支持 include** - 引入其他配置文件
4. **支持变量替换** - `${variable}` 语法
5. **支持环境变量** - `${?ENV_VAR}` 语法

### 4.2 SeaTunnel 配置结构

```hocon
# 环境配置
env {
  parallelism = 2
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

# 数据源配置
source {
  Jdbc {
    result_table_name = "source_table"
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "password"
    query = "SELECT * FROM users"
  }
}

# 数据转换配置（可选）
transform {
  Filter {
    source_table_name = "source_table"
    result_table_name = "filtered_table"
    fields = ["id", "name"]
  }
}

# 数据目标配置
sink {
  Console {
    source_table_name = "filtered_table"
  }
}
```

### 4.3 配置解析流程

```
                    ┌─────────────────┐
                    │  配置文件       │
                    │ (*.conf/*.sql)  │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
     ┌────────────────┐           ┌─────────────────┐
     │ HOCON 格式     │           │ SQL 格式        │
     │ Typesafe Config│           │ SqlConfigBuilder│
     └───────┬────────┘           └────────┬────────┘
             │                             │
             │                             ▼
             │                    ┌─────────────────┐
             │                    │ SeaTunnelConfig │
             │                    └────────┬────────┘
             │                             │
             │                             ▼
             │                    ┌─────────────────┐
             │                    │ ConfigTemplate  │
             │                    └────────┬────────┘
             │                             │
             └──────────────┬──────────────┘
                            │
                            ▼
                   ┌────────────────┐
                   │ Config 对象    │
                   │ (Typesafe)     │
                   └────────────────┘
```

---

## 五、SQL 配置语法详解

### 5.1 SET 语句（环境配置）

```sql
-- 基本设置
SET 'job.mode' = 'BATCH';
SET 'parallelism' = '4';
SET 'checkpoint.interval' = '10000';

-- 作业名称
SET 'job.name' = 'my_sync_job';
```

### 5.2 CREATE TABLE（Source/Sink 定义）

```sql
-- Source 定义
CREATE TABLE mysql_source (
    id BIGINT,
    name STRING,
    age INT,
    create_time TIMESTAMP
) WITH (
    'connector' = 'Jdbc',
    'url' = 'jdbc:mysql://localhost:3306/test',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'user' = 'root',
    'password' = 'password',
    'query' = 'SELECT * FROM users'
);

-- Sink 定义
CREATE TABLE console_sink (
    id BIGINT,
    name STRING,
    age INT
) WITH (
    'connector' = 'Console'
);
```

### 5.3 INSERT INTO SELECT（数据流定义）

```sql
-- 简单复制
INSERT INTO console_sink
SELECT id, name, age FROM mysql_source;

-- 带过滤条件
INSERT INTO console_sink
SELECT id, name, age FROM mysql_source
WHERE age >= 18;

-- 带字段转换
INSERT INTO console_sink
SELECT
    id,
    UPPER(name) as name,
    age + 1 as age
FROM mysql_source;
```

### 5.4 CREATE AS（Transform 定义）

```sql
-- 创建转换表
CREATE TABLE filtered_data AS
SELECT id, name, age
FROM mysql_source
WHERE age >= 18;

-- 使用转换后的表
INSERT INTO console_sink
SELECT * FROM filtered_data;
```

### 5.5 完整示例

```sql
-- MySQL 到 Console 的数据同步任务
-- 配置环境
SET 'job.mode' = 'BATCH';
SET 'parallelism' = '2';
SET 'job.name' = 'mysql_to_console';

-- 定义 MySQL 数据源
CREATE TABLE mysql_users (
    id BIGINT PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(200),
    status INT,
    created_at TIMESTAMP
) WITH (
    'connector' = 'Jdbc',
    'url' = 'jdbc:mysql://localhost:3306/mydb',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'user' = 'root',
    'password' = '123456',
    'table' = 'users'
);

-- 定义控制台输出
CREATE TABLE console_output (
    id BIGINT,
    username VARCHAR(100),
    email VARCHAR(200)
) WITH (
    'connector' = 'Console'
);

-- 定义数据流：过滤活跃用户并输出
INSERT INTO console_output
SELECT id, username, email
FROM mysql_users
WHERE status = 1;
```

### 5.6 生成的 HOCON 配置

```hocon
env {
  job.mode = "BATCH"
  parallelism = "2"
  job.name = "mysql_to_console"
}

source {
  Jdbc {
    result_table_name = "mysql_users"
    url = "jdbc:mysql://localhost:3306/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    table = "users"
  }
}

transform {
  Sql {
    source_table_name = "mysql_users"
    result_table_name = "console_output"
    query = "SELECT id, username, email FROM mysql_users WHERE status = 1"
  }
}

sink {
  Console {
    source_table_name = "console_output"
  }
}
```

---

## 六、类型映射

### 6.1 SQL 类型到 SeaTunnel 类型

| SQL 类型        | SeaTunnel 类型 |
|-----------------|----------------|
| INT / INTEGER   | INT            |
| BIGINT          | BIGINT         |
| SMALLINT        | SMALLINT       |
| TINYINT         | TINYINT        |
| FLOAT           | FLOAT          |
| DOUBLE          | DOUBLE         |
| DECIMAL         | DECIMAL        |
| VARCHAR / CHAR  | STRING         |
| STRING / TEXT   | STRING         |
| BOOLEAN         | BOOLEAN        |
| DATE            | DATE           |
| TIME            | TIME           |
| TIMESTAMP       | TIMESTAMP      |
| BINARY / BYTES  | BYTES          |

### 6.2 类型解析代码

```java
public class TypeMapper {

    public static String toSeaTunnelType(String sqlType) {
        String upperType = sqlType.toUpperCase().trim();

        // 处理带精度的类型
        if (upperType.contains("(")) {
            upperType = upperType.substring(0, upperType.indexOf("("));
        }

        switch (upperType) {
            case "INT":
            case "INTEGER":
                return "INT";
            case "BIGINT":
                return "BIGINT";
            case "SMALLINT":
                return "SMALLINT";
            case "TINYINT":
                return "TINYINT";
            case "FLOAT":
            case "REAL":
                return "FLOAT";
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return "DOUBLE";
            case "DECIMAL":
            case "NUMERIC":
                return "DECIMAL";
            case "VARCHAR":
            case "CHAR":
            case "STRING":
            case "TEXT":
                return "STRING";
            case "BOOLEAN":
            case "BOOL":
                return "BOOLEAN";
            case "DATE":
                return "DATE";
            case "TIME":
                return "TIME";
            case "TIMESTAMP":
            case "DATETIME":
                return "TIMESTAMP";
            case "BINARY":
            case "VARBINARY":
            case "BYTES":
            case "BLOB":
                return "BYTES";
            default:
                return "STRING";
        }
    }
}
```

---

## 七、设计模式分析

### 7.1 Builder 模式

`SqlConfigBuilder` 使用 Builder 模式构建配置：

```java
// 链式调用
SeaTunnelConfig config = SqlConfigBuilder.of(sqlContent)
    .build();

// 从文件构建
SeaTunnelConfig config = SqlConfigBuilder.of(Paths.get("job.sql"))
    .build();
```

### 7.2 不可变对象模式

`ConfigParseOptions` 使用不可变对象模式：

```java
// 每次修改返回新对象
ConfigParseOptions options = ConfigParseOptions.defaults()
    .setSyntax(ConfigSyntax.CONF)
    .setAllowMissing(false)
    .setOriginDescription("job config");
```

### 7.3 工厂方法模式

静态工厂方法创建对象：

```java
// ConfigParseOptions
ConfigParseOptions options = ConfigParseOptions.defaults();

// SqlConfigBuilder
SqlConfigBuilder builder = SqlConfigBuilder.of(content);
```

### 7.4 模板方法模式

`ConfigTemplate` 使用模板方法生成配置：

```java
public String generate() {
    StringBuilder sb = new StringBuilder();
    sb.append(globalConfig());   // 模板方法
    sb.append(sourceItems());    // 模板方法
    sb.append(transformItems()); // 模板方法
    sb.append(sinkItems());      // 模板方法
    return sb.toString();
}
```

---

## 八、使用示例

### 8.1 HOCON 配置解析

```java
import org.apache.seatunnel.shade.com.typesafe.config.*;

// 从文件解析
Config config = ConfigFactory.parseFile(new File("job.conf"));

// 从字符串解析
Config config = ConfigFactory.parseString(hoconContent);

// 获取配置值
String jobMode = config.getString("env.job.mode");
int parallelism = config.getInt("env.parallelism");
List<? extends Config> sources = config.getConfigList("source");
```

### 8.2 SQL 配置转换

```java
import org.apache.seatunnel.config.sql.*;
import org.apache.seatunnel.config.sql.model.*;

// 从 SQL 文件构建配置
Path sqlFile = Paths.get("job.sql");
SeaTunnelConfig config = SqlConfigBuilder.of(sqlFile).build();

// 生成 HOCON 配置
ConfigTemplate template = new ConfigTemplate(config);
String hoconConfig = template.generate();

// 保存或使用 HOCON 配置
Files.write(Paths.get("job.conf"), hoconConfig.getBytes());
```

### 8.3 配置校验

```java
import org.apache.seatunnel.shade.com.typesafe.config.*;

Config config = ConfigFactory.parseFile(new File("job.conf"));

// 检查必需配置
if (!config.hasPath("source")) {
    throw new IllegalArgumentException("Missing source configuration");
}

if (!config.hasPath("sink")) {
    throw new IllegalArgumentException("Missing sink configuration");
}

// 获取并校验 env 配置
Config envConfig = config.getConfig("env");
String jobMode = envConfig.hasPath("job.mode")
    ? envConfig.getString("job.mode")
    : "BATCH";
```

---

## 九、与其他模块的关系

### 9.1 模块依赖图

```
┌─────────────────────┐
│   seatunnel-core    │
│  (SeaTunnelStarter) │
└──────────┬──────────┘
           │ 使用
           ▼
┌─────────────────────┐     ┌─────────────────────┐
│  seatunnel-config   │────▶│  seatunnel-common   │
│   (Config 解析)     │     │    (工具类)         │
└──────────┬──────────┘     └─────────────────────┘
           │
           ▼
┌─────────────────────┐
│   Typesafe Config   │
│   (HOCON 解析库)    │
└─────────────────────┘
```

### 9.2 调用关系

1. **seatunnel-core** 调用 `seatunnel-config` 解析作业配置
2. **seatunnel-engine** 使用解析后的 Config 对象创建作业
3. **connectors** 从 Config 中读取连接器配置参数

### 9.3 配置流转

```
用户配置文件 (*.conf / *.sql)
        │
        ▼
    Config 解析
        │
        ▼
   Typesafe Config 对象
        │
        ▼
   JobConfig / EnvConfig
        │
        ▼
   Source/Transform/Sink 实例化
```

---

## 十、最佳实践

### 10.1 配置文件组织

```
config/
├── common.conf          # 公共配置
├── env/
│   ├── dev.conf         # 开发环境
│   ├── test.conf        # 测试环境
│   └── prod.conf        # 生产环境
└── jobs/
    ├── mysql_to_hdfs.conf
    ├── kafka_to_es.conf
    └── cdc_sync.conf
```

### 10.2 使用 include

```hocon
# job.conf
include "common.conf"
include "env/prod.conf"

source {
  Jdbc {
    url = ${jdbc.url}  # 引用公共配置
    user = ${jdbc.user}
    password = ${jdbc.password}
    query = "SELECT * FROM orders"
  }
}
```

### 10.3 使用环境变量

```hocon
env {
  parallelism = ${?PARALLELISM}  # 可选环境变量
}

source {
  Jdbc {
    url = ${DB_URL}              # 必需环境变量
    user = ${DB_USER}
    password = ${DB_PASSWORD}
  }
}
```

### 10.4 配置校验清单

| 检查项         | 说明                              |
|----------------|-----------------------------------|
| source 不为空  | 至少定义一个数据源                |
| sink 不为空    | 至少定义一个数据目标              |
| connector 有效 | 检查连接器名称是否正确            |
| 必需参数完整   | 各连接器的必需参数不能缺失        |
| 类型匹配       | Schema 类型与数据源实际类型匹配   |

---

## 十一、总结

### 11.1 模块特点

| 特点           | 说明                                       |
|----------------|--------------------------------------------|
| 灵活性         | 同时支持 HOCON 和 SQL 两种配置语法         |
| 兼容性         | 基于成熟的 Typesafe Config 库              |
| 可扩展性       | 支持 include、变量替换、环境变量           |
| Shade 隔离     | 避免与其他项目的依赖冲突                   |

### 11.2 核心组件

| 组件              | 功能                        |
|-------------------|-----------------------------|
| ConfigParseOptions| HOCON 解析配置选项          |
| SqlConfigBuilder  | SQL 到配置的转换器          |
| SeaTunnelConfig   | 配置数据模型                |
| ConfigTemplate    | HOCON 配置生成器            |

### 11.3 设计优势

1. **学习曲线低** - SQL 语法对开发者友好
2. **配置可读性高** - HOCON 格式简洁易懂
3. **强类型支持** - 配置值有明确的类型
4. **依赖隔离** - Shade 机制避免冲突
5. **灵活组合** - 支持 include 和变量替换

---

## 附录A：完整类清单

### seatunnel-config-shade

| 类名                | 说明                |
|---------------------|---------------------|
| ConfigParseOptions  | 解析选项配置        |
| ConfigSyntax        | 语法类型枚举        |
| ConfigIncluder      | Include 解析器接口  |
| Config              | 配置对象接口        |
| ConfigFactory       | 配置工厂类          |
| ConfigValue         | 配置值接口          |
| ConfigList          | 配置列表接口        |
| ConfigObject        | 配置对象接口        |
| ConfigException     | 配置异常类          |

### seatunnel-config-sql

| 类名              | 说明                 |
|-------------------|----------------------|
| SqlConfigBuilder  | SQL 配置构建器       |
| ConfigTemplate    | HOCON 模板生成器     |
| SeaTunnelConfig   | 配置数据模型         |
| SourceConfig      | Source 配置模型      |
| SinkConfig        | Sink 配置模型        |
| TransformConfig   | Transform 配置模型   |
| ColumnConfig      | 列配置模型           |
| TypeMapper        | 类型映射工具         |

---

## 附录B：配置语法对比

### HOCON vs JSON vs Properties

| 特性         | HOCON | JSON  | Properties |
|--------------|-------|-------|------------|
| 注释支持     | ✅    | ❌    | ✅         |
| 省略引号     | ✅    | ❌    | ✅         |
| 省略逗号     | ✅    | ❌    | N/A        |
| 多行字符串   | ✅    | ❌    | ❌         |
| Include      | ✅    | ❌    | ❌         |
| 变量替换     | ✅    | ❌    | ❌         |
| 环境变量     | ✅    | ❌    | ❌         |
| 数组支持     | ✅    | ✅    | ❌         |
| 嵌套结构     | ✅    | ✅    | ❌（需.分隔）|

### SQL 语法优势

| 优势         | 说明                              |
|--------------|-----------------------------------|
| 熟悉度高     | 数据工程师普遍熟悉 SQL 语法       |
| 语义清晰     | CREATE TABLE 明确表示数据源定义   |
| 转换直观     | SELECT 直接表达数据转换逻辑       |
| 工具支持     | 可复用 SQL IDE 和校验工具         |

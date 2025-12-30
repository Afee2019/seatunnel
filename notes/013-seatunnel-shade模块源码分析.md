# SeaTunnel Shade 模块源码分析
> 文档编号: 013
> 模块路径: seatunnel-shade/
> 更新日期: 2025-12-29
---

## 一、模块概述

### 1.1 模块定位

`seatunnel-shade` 模块是 SeaTunnel 的**依赖隔离模块**，通过 Maven Shade 插件对第三方依赖进行包重定向（Package Relocation），解决以下问题：

1. **依赖冲突** - 避免与 Flink/Spark 等引擎自带的依赖版本冲突
2. **类加载隔离** - 确保 SeaTunnel 使用的依赖与用户应用依赖互不干扰
3. **版本控制** - 锁定特定版本的依赖，避免运行时兼容性问题

### 1.2 Shade 原理

Maven Shade 插件通过修改字节码中的包名，将依赖类重定向到新的命名空间：

```
原始包名                    →  重定向后包名
com.google.guava           →  org.apache.seatunnel.shade.com.google
com.fasterxml.jackson      →  org.apache.seatunnel.shade.com.fasterxml.jackson
org.eclipse.jetty          →  org.apache.seatunnel.shade.org.eclipse
```

### 1.3 子模块结构

```
seatunnel-shade/
├── pom.xml                           # 父POM
├── seatunnel-arrow/                  # Apache Arrow 向量计算库
├── seatunnel-commons-lang3/          # Apache Commons Lang3
├── seatunnel-guava/                  # Google Guava 工具库
├── seatunnel-hadoop3-3.1.4-uber/     # Hadoop 3.1.4 Uber包
├── seatunnel-hadoop-aws/             # Hadoop AWS 支持
├── seatunnel-hazelcast/              # Hazelcast 分布式计算
│   ├── seatunnel-hazelcast-base/     # Hazelcast 基础包
│   └── seatunnel-hazelcast-shade/    # Hazelcast Shade 包
├── seatunnel-hikari/                 # HikariCP 连接池
├── seatunnel-jackson/                # Jackson JSON 处理
├── seatunnel-janino/                 # Janino Java 编译器
├── seatunnel-jetty9-9.4.56/          # Jetty 9 Web 服务器
├── seatunnel-scala-compiler/         # Scala 编译器
└── seatunnel-thrift-service/         # Thrift 服务（Doris连接器用）
```

### 1.4 模块统计

| 子模块 | 原始依赖 | 版本 | 主要用途 |
|--------|----------|------|----------|
| seatunnel-guava | com.google.guava:guava | 动态 | 集合工具、缓存 |
| seatunnel-jackson | com.fasterxml.jackson | 动态 | JSON 序列化 |
| seatunnel-hikari | com.zaxxer:HikariCP | 4.0.3 | JDBC 连接池 |
| seatunnel-jetty9 | org.eclipse.jetty | 9.4.56 | REST API 服务 |
| seatunnel-hazelcast | com.hazelcast:hazelcast | 5.1 | Zeta 引擎分布式 |
| seatunnel-hadoop3 | org.apache.hadoop:hadoop-client | 3.1.4 | HDFS 文件操作 |
| seatunnel-hadoop-aws | org.apache.hadoop:hadoop-aws | 动态 | S3 文件操作 |
| seatunnel-arrow | org.apache.arrow | 15.0.1 | 列式数据处理 |
| seatunnel-janino | org.codehaus.janino:janino | 3.0.11 | 动态 Java 编译 |
| seatunnel-scala-compiler | org.scala-lang:scala-compiler | 2.13.11 | 动态 Scala 编译 |
| seatunnel-commons-lang3 | org.apache.commons:commons-lang3 | 动态 | 通用工具类 |
| seatunnel-thrift-service | org.apache.doris:thrift-service | 1.0.0 | Doris 连接器 |

---

## 二、全局 Shade 配置

### 2.1 Shade 包前缀

在根 `pom.xml` 中定义了统一的 Shade 包前缀：

```xml
<properties>
    <seatunnel.shade.package>org.apache.seatunnel.shade</seatunnel.shade.package>
</properties>
```

所有 Shade 模块都使用此前缀进行包重定向：
- `com.google` → `org.apache.seatunnel.shade.com.google`
- `com.fasterxml.jackson` → `org.apache.seatunnel.shade.com.fasterxml.jackson`

### 2.2 通用过滤器配置

所有 Shade 模块都排除签名文件，避免 JAR 签名验证失败：

```xml
<filters>
    <filter>
        <artifact>*:*</artifact>
        <excludes>
            <exclude>META-INF/*.SF</exclude>
            <exclude>META-INF/*.DSA</exclude>
            <exclude>META-INF/*.RSA</exclude>
        </excludes>
    </filter>
</filters>
```

### 2.3 Optional Classifier

每个 Shade 模块都生成带 `optional` classifier 的 JAR：

```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>build-helper-maven-plugin</artifactId>
    <executions>
        <execution>
            <id>attach-artifacts</id>
            <goals>
                <goal>attach-artifact</goal>
            </goals>
            <phase>package</phase>
            <configuration>
                <artifacts>
                    <artifact>
                        <file>${basedir}/target/seatunnel-xxx.jar</file>
                        <type>jar</type>
                        <classifier>optional</classifier>
                    </artifact>
                </artifacts>
            </configuration>
        </execution>
    </executions>
</plugin>
```

---

## 三、各子模块详解

### 3.1 seatunnel-guava

**功能**: 封装 Google Guava 工具库

**原始依赖**:
```xml
<dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>${guava.version}</version>
</dependency>
```

**重定向规则**:
```xml
<relocation>
    <pattern>com.google</pattern>
    <shadedPattern>${seatunnel.shade.package}.com.google</shadedPattern>
</relocation>
```

**使用方式**:
```java
// 原始导入
// import com.google.common.collect.Lists;

// Shade 后导入
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

List<String> list = Lists.newArrayList("a", "b", "c");
```

**冲突场景**: Flink/Spark 自带不同版本的 Guava，直接使用可能导致 `NoSuchMethodError`

---

### 3.2 seatunnel-jackson

**功能**: 封装 Jackson JSON 处理库

**原始依赖**:
```xml
<dependencies>
    <dependency>
        <groupId>com.fasterxml.jackson.dataformat</groupId>
        <artifactId>jackson-dataformat-properties</artifactId>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.datatype</groupId>
        <artifactId>jackson-datatype-jsr310</artifactId>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-core</artifactId>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
    </dependency>
</dependencies>
```

**重定向规则**:
```xml
<relocation>
    <pattern>com.fasterxml.jackson</pattern>
    <shadedPattern>${seatunnel.shade.package}.com.fasterxml.jackson</shadedPattern>
</relocation>
```

**使用方式**:
```java
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

ObjectMapper mapper = new ObjectMapper();
JsonNode node = mapper.readTree(jsonString);
```

**包含模块**:
- jackson-core
- jackson-databind
- jackson-dataformat-properties
- jackson-datatype-jsr310 (Java 8 日期时间支持)

---

### 3.3 seatunnel-hikari

**功能**: 封装 HikariCP 高性能 JDBC 连接池

**原始依赖**:
```xml
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>4.0.3</version>
</dependency>
```

**重定向规则**:
```xml
<relocation>
    <pattern>com.zaxxer.hikari</pattern>
    <shadedPattern>${seatunnel.shade.package}.com.zaxxer.hikari</shadedPattern>
</relocation>
```

**使用方式**:
```java
import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariConfig;
import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;

HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:mysql://localhost:3306/test");
config.setUsername("root");
config.setPassword("password");
HikariDataSource dataSource = new HikariDataSource(config);
```

**冲突场景**: Spark 自带 HikariCP，版本可能与 SeaTunnel 需求不兼容

---

### 3.4 seatunnel-jetty9-9.4.56

**功能**: 封装 Jetty 9 Web 服务器，用于 SeaTunnel Engine REST API

**原始依赖**:
```xml
<dependencies>
    <dependency>
        <groupId>org.eclipse.jetty</groupId>
        <artifactId>jetty-server</artifactId>
        <version>${jetty.version}</version>
    </dependency>
    <dependency>
        <groupId>org.eclipse.jetty</groupId>
        <artifactId>jetty-servlet</artifactId>
        <version>${jetty.version}</version>
    </dependency>
</dependencies>
```

**重定向规则**:
```xml
<relocation>
    <pattern>org.eclipse</pattern>
    <shadedPattern>${seatunnel.shade.package}.org.eclipse</shadedPattern>
</relocation>
```

**使用方式**:
```java
import org.apache.seatunnel.shade.org.eclipse.jetty.server.Server;
import org.apache.seatunnel.shade.org.eclipse.jetty.servlet.ServletContextHandler;

Server server = new Server(8080);
ServletContextHandler handler = new ServletContextHandler();
server.setHandler(handler);
server.start();
```

---

### 3.5 seatunnel-hazelcast

**功能**: 封装 Hazelcast 分布式计算框架，是 SeaTunnel Zeta 引擎的核心依赖

**模块结构**:
```
seatunnel-hazelcast/
├── pom.xml                       # 父POM
├── seatunnel-hazelcast-base/     # 基础包（minimizeJar）
└── seatunnel-hazelcast-shade/    # Shade 包（引用 base）
```

**seatunnel-hazelcast-base 配置**:
```xml
<properties>
    <hazelcast.version>5.1</hazelcast.version>
</properties>

<dependencies>
    <dependency>
        <groupId>com.hazelcast</groupId>
        <artifactId>hazelcast</artifactId>
        <version>${hazelcast.version}</version>
    </dependency>
</dependencies>

<configuration>
    <minimizeJar>true</minimizeJar>  <!-- 最小化JAR，移除未使用的类 -->
    <filters>
        <filter>
            <artifact>com.typesafe:config</artifact>
            <excludes>
                <!-- 排除某些类以避免冲突 -->
                <exclude>com/hazelcast/internal/cluster/impl/MembershipManager.class</exclude>
                <exclude>com/hazelcast/internal/cluster/impl/MemberMap.class</exclude>
                <exclude>com/hazelcast/internal/cluster/impl/ClusterServiceImpl.class</exclude>
                <exclude>com/hazelcast/cluster/impl/MemberImpl.class</exclude>
            </excludes>
        </filter>
    </filters>
</configuration>
```

**特殊处理**: Hazelcast 模块没有进行包重定向，而是使用 `minimizeJar` 减小体积，并排除某些类以避免冲突

---

### 3.6 seatunnel-hadoop3-3.1.4-uber

**功能**: 封装 Hadoop 3.1.4 客户端，用于 HDFS 文件操作

**原始依赖**:
```xml
<dependencies>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>3.1.4</version>
    </dependency>
    <dependency>
        <groupId>org.xerial.snappy</groupId>
        <artifactId>snappy-java</artifactId>
        <version>1.1.10.4</version>
    </dependency>
</dependencies>
```

**重定向规则**:
```xml
<relocations>
    <!-- Commons IO -->
    <relocation>
        <pattern>org.apache.commons.io</pattern>
        <shadedPattern>shade.org.apache.commons.io</shadedPattern>
    </relocation>
    <!-- Commons Lang3 -->
    <relocation>
        <pattern>org.apache.commons.lang3</pattern>
        <shadedPattern>shade.org.apache.commons.lang3</shadedPattern>
    </relocation>
    <!-- Guava（部分包） -->
    <relocation>
        <pattern>com.google.common</pattern>
        <shadedPattern>${seatunnel.shade.package}.hadoop.com.google.common</shadedPattern>
        <includes>
            <include>com.google.common.base.*</include>
            <include>com.google.common.cache.*</include>
            <include>com.google.common.collect.*</include>
        </includes>
    </relocation>
    <!-- Jackson -->
    <relocation>
        <pattern>com.fasterxml.jackson</pattern>
        <shadedPattern>${seatunnel.shade.package}.hadoop.com.fasterxml.jackson</shadedPattern>
    </relocation>
</relocations>
```

**注意**: 此模块是 "Uber" 包，包含 Hadoop 客户端及其所有传递依赖

---

### 3.7 seatunnel-hadoop-aws

**功能**: 封装 Hadoop AWS 支持，用于 S3 文件操作

**原始依赖**:
```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aws</artifactId>
    <version>${hadoop-aws.version}</version>
</dependency>
```

**重定向规则**:
```xml
<relocation>
    <pattern>com.google.common</pattern>
    <shadedPattern>${seatunnel.shade.package}.com.google.common</shadedPattern>
    <includes>
        <include>com.google.common.base.*</include>
        <include>com.google.common.cache.*</include>
        <include>com.google.common.collect.*</include>
    </includes>
</relocation>
```

---

### 3.8 seatunnel-arrow

**功能**: 封装 Apache Arrow 向量计算库

**原始依赖**:
```xml
<dependencies>
    <dependency>
        <groupId>org.apache.arrow</groupId>
        <artifactId>arrow-vector</artifactId>
        <version>15.0.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow</groupId>
        <artifactId>arrow-memory-netty</artifactId>
        <version>15.0.1</version>
    </dependency>
</dependencies>
```

**重定向规则**:
```xml
<relocations>
    <relocation>
        <pattern>org.apache.arrow</pattern>
        <shadedPattern>${seatunnel.shade.package}.org.apache.arrow</shadedPattern>
    </relocation>
    <relocation>
        <pattern>io.netty</pattern>
        <shadedPattern>${seatunnel.shade.package}.io.netty</shadedPattern>
    </relocation>
    <relocation>
        <pattern>com.google.flatbuffers</pattern>
        <shadedPattern>${seatunnel.shade.package}.com.google.flatbuffers</shadedPattern>
    </relocation>
    <relocation>
        <pattern>com.fasterxml.jackson</pattern>
        <shadedPattern>${seatunnel.shade.package}.com.fasterxml.jackson</shadedPattern>
    </relocation>
</relocations>
```

**用途**: Arrow 用于高性能列式数据处理，与 Spark/Flink 的数据交换

---

### 3.9 seatunnel-janino

**功能**: 封装 Janino Java 运行时编译器

**原始依赖**:
```xml
<dependency>
    <groupId>org.codehaus.janino</groupId>
    <artifactId>janino</artifactId>
    <version>3.0.11</version>
    <optional>true</optional>
</dependency>
```

**重定向规则**:
```xml
<relocation>
    <pattern>org.codehaus</pattern>
    <shadedPattern>${seatunnel.shade.package}.org.codehaus</shadedPattern>
</relocation>
```

**用途**: 用于动态编译 Java 代码（DynamicCompileTransform）

---

### 3.10 seatunnel-scala-compiler

**功能**: 封装 Scala 编译器

**原始依赖**:
```xml
<dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-compiler</artifactId>
    <version>2.13.11</version>
</dependency>
```

**重定向规则**:
```xml
<relocations>
    <!-- 只 shade 编译器工具类，避免 scala.reflect -->
    <relocation>
        <pattern>scala.tools.nsc</pattern>
        <shadedPattern>${seatunnel.shade.package}.scala.tools.nsc</shadedPattern>
    </relocation>
    <relocation>
        <pattern>scala.tools.util</pattern>
        <shadedPattern>${seatunnel.shade.package}.scala.tools.util</shadedPattern>
    </relocation>
</relocations>
```

**注意**: 只 Shade `scala.tools` 包，避免与 Scala 反射机制冲突

**用途**: 用于动态编译 Scala 代码（DynamicCompileTransform）

---

### 3.11 seatunnel-commons-lang3

**功能**: 封装 Apache Commons Lang3 工具库

**原始依赖**:
```xml
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-lang3</artifactId>
    <version>${commons-lang3.version}</version>
</dependency>
```

**重定向规则**:
```xml
<relocation>
    <pattern>org.apache.commons.lang3</pattern>
    <shadedPattern>${seatunnel.shade.package}.org.apache.commons.lang3</shadedPattern>
</relocation>
```

**使用方式**:
```java
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

boolean isEmpty = StringUtils.isEmpty(str);
```

---

### 3.12 seatunnel-thrift-service

**功能**: 封装 Doris Thrift 服务

**原始依赖**:
```xml
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>thrift-service</artifactId>
    <version>1.0.0</version>
</dependency>
```

**重定向规则**:
```xml
<relocation>
    <pattern>org.apache.thrift</pattern>
    <shadedPattern>${seatunnel.shade.package}.org.apache.thrift</shadedPattern>
</relocation>
```

**用途**: Doris 连接器使用 Thrift 协议通信

---

## 四、Shade 技术原理

### 4.1 Maven Shade 插件工作流程

```
┌─────────────────────────────────────────────────────────────┐
│                    Maven Package Phase                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  1. 收集所有依赖 JAR 文件                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  2. 解压所有 JAR 到临时目录                                  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. 应用 Relocation 规则                                     │
│     - 重命名 .class 文件路径                                 │
│     - 修改字节码中的包引用                                   │
│     - 更新 META-INF/services SPI 文件                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. 应用 Filter 规则                                         │
│     - 排除签名文件                                           │
│     - 排除指定资源                                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  5. 应用 Transformer                                         │
│     - 合并 META-INF/services                                 │
│     - 处理 LICENSE/NOTICE                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  6. 打包为最终 Uber JAR                                      │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 字节码重写示例

**原始字节码**:
```
CONSTANT_Utf8: com/google/common/collect/Lists
INVOKESTATIC com/google/common/collect/Lists.newArrayList()
```

**重写后字节码**:
```
CONSTANT_Utf8: org/apache/seatunnel/shade/com/google/common/collect/Lists
INVOKESTATIC org/apache/seatunnel/shade/com/google/common/collect/Lists.newArrayList()
```

### 4.3 SPI 文件处理

原始 `META-INF/services/com.google.common.SomeService`:
```
com.google.common.impl.ServiceImpl
```

Shade 后 `META-INF/services/org.apache.seatunnel.shade.com.google.common.SomeService`:
```
org.apache.seatunnel.shade.com.google.common.impl.ServiceImpl
```

---

## 五、依赖隔离架构

### 5.1 类加载器层次

```
┌─────────────────────────────────────────────────────────────┐
│                    Bootstrap ClassLoader                     │
│                     (JDK 核心类)                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Platform ClassLoader                      │
│                     (JDK 扩展类)                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Application ClassLoader                   │
│              (用户应用 + Flink/Spark 依赖)                   │
│                                                              │
│   com.google.guava:30.0                                     │
│   com.fasterxml.jackson:2.12                                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   SeaTunnel ClassLoader                      │
│                 (SeaTunnel Shade 依赖)                       │
│                                                              │
│   org.apache.seatunnel.shade.com.google:33.0               │
│   org.apache.seatunnel.shade.com.fasterxml.jackson:2.15    │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 隔离效果

| 场景 | 无 Shade | 有 Shade |
|------|----------|----------|
| Flink 使用 Guava 27 | NoSuchMethodError | 正常运行 |
| Spark 使用 Jackson 2.11 | IncompatibleClassChangeError | 正常运行 |
| 用户应用使用旧版 Commons | ClassCastException | 正常运行 |

---

## 六、使用指南

### 6.1 在代码中使用 Shade 依赖

```java
// 错误：使用原始包名
import com.google.common.collect.Lists;  // 可能与 Flink 冲突

// 正确：使用 Shade 包名
import org.apache.seatunnel.shade.com.google.common.collect.Lists;
```

### 6.2 Maven 依赖配置

```xml
<!-- 使用 Shade 版本的 Guava -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-guava</artifactId>
    <version>${project.version}</version>
</dependency>

<!-- 使用 Shade 版本的 Jackson -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-jackson</artifactId>
    <version>${project.version}</version>
</dependency>
```

### 6.3 IDE 自动补全

由于包名被重定向，IDE 可能无法自动识别类。解决方案：

1. **添加源码 JAR** - Shade 模块会生成 `-sources.jar`
2. **手动配置** - 在 IDE 中添加 Shade JAR 为库

---

## 七、连接器中的 Shade

### 7.1 连接器级别的 Shade

部分连接器有自己的 Shade 配置，避免连接器间的依赖冲突：

**connector-iceberg 示例**:
```xml
<relocations>
    <relocation>
        <pattern>org.apache.avro</pattern>
        <shadedPattern>${seatunnel.shade.package}.${connector.name}.org.apache.avro</shadedPattern>
    </relocation>
    <relocation>
        <pattern>org.apache.orc</pattern>
        <shadedPattern>${seatunnel.shade.package}.${connector.name}.org.apache.orc</shadedPattern>
    </relocation>
    <relocation>
        <pattern>org.apache.parquet</pattern>
        <shadedPattern>${seatunnel.shade.package}.${connector.name}.org.apache.parquet</shadedPattern>
    </relocation>
</relocations>
```

**connector-hive 示例**:
```xml
<relocations>
    <relocation>
        <pattern>org.apache.avro</pattern>
        <shadedPattern>${seatunnel.shade.package}.${connector.name}.org.apache.avro</shadedPattern>
    </relocation>
</relocations>
```

### 7.2 连接器 Shade 命名规范

```
org.apache.seatunnel.shade.{connector-name}.{original-package}
```

例如:
- `org.apache.seatunnel.shade.iceberg.org.apache.avro`
- `org.apache.seatunnel.shade.hive.org.apache.parquet`

---

## 八、常见问题与解决

### 8.1 NoSuchMethodError

**症状**:
```
java.lang.NoSuchMethodError: com.google.common.collect.Lists.partition()
```

**原因**: 使用了未 Shade 的 Guava，与 Flink/Spark 版本冲突

**解决**: 改用 Shade 版本
```java
import org.apache.seatunnel.shade.com.google.common.collect.Lists;
```

### 8.2 ClassNotFoundException

**症状**:
```
java.lang.ClassNotFoundException: org.apache.seatunnel.shade.com.google.common.collect.Lists
```

**原因**: 缺少 Shade JAR 依赖

**解决**: 添加 Maven 依赖
```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-guava</artifactId>
</dependency>
```

### 8.3 签名验证失败

**症状**:
```
java.lang.SecurityException: Invalid signature file digest for Manifest main attributes
```

**原因**: Shade 后签名文件未被正确处理

**解决**: 确保 Shade 配置排除签名文件
```xml
<filter>
    <artifact>*:*</artifact>
    <excludes>
        <exclude>META-INF/*.SF</exclude>
        <exclude>META-INF/*.DSA</exclude>
        <exclude>META-INF/*.RSA</exclude>
    </excludes>
</filter>
```

### 8.4 反射调用失败

**症状**: 使用反射创建 Shade 类实例失败

**原因**: 反射使用原始类名

**解决**: 使用 Shade 后的完整类名
```java
// 错误
Class.forName("com.google.common.collect.Lists");

// 正确
Class.forName("org.apache.seatunnel.shade.com.google.common.collect.Lists");
```

---

## 九、设计模式分析

### 9.1 外观模式（Facade）

Shade 模块作为外观，隐藏了底层依赖的复杂性：

```
┌─────────────────┐
│   SeaTunnel     │
│   Application   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Shade Module   │  ← 外观
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌──────┐  ┌──────┐
│Guava │  │Jackson│
└──────┘  └──────┘
```

### 9.2 适配器模式（Adapter）

Shade 后的类作为适配器，适配到 SeaTunnel 的命名空间：

```java
// 原始 API
com.google.common.collect.Lists.newArrayList()

// 适配后 API（相同功能，不同命名空间）
org.apache.seatunnel.shade.com.google.common.collect.Lists.newArrayList()
```

### 9.3 模块化设计

每个 Shade 子模块独立打包，可按需引入：

```
seatunnel-shade
├── seatunnel-guava         ← 独立模块
├── seatunnel-jackson       ← 独立模块
├── seatunnel-hikari        ← 独立模块
└── ...
```

---

## 十、总结

### 10.1 模块特点

| 特点 | 说明 |
|------|------|
| 依赖隔离 | 避免与 Flink/Spark 等引擎的依赖冲突 |
| 版本锁定 | 锁定特定版本，确保运行时兼容性 |
| 按需引入 | 各子模块独立，可按需选择 |
| 统一命名 | 统一使用 `org.apache.seatunnel.shade` 前缀 |

### 10.2 核心子模块

| 子模块 | 用途 | 使用场景 |
|--------|------|----------|
| seatunnel-guava | 集合工具 | 全局使用 |
| seatunnel-jackson | JSON 处理 | 序列化/反序列化 |
| seatunnel-hazelcast | 分布式计算 | Zeta 引擎 |
| seatunnel-hikari | 连接池 | JDBC 连接器 |
| seatunnel-hadoop3 | HDFS 操作 | 文件连接器 |
| seatunnel-jetty9 | REST API | 引擎 Web 服务 |

### 10.3 最佳实践

1. **始终使用 Shade 依赖** - 在 SeaTunnel 代码中使用 Shade 版本
2. **正确配置 Maven** - 引入 Shade 模块而非原始依赖
3. **注意反射调用** - 使用 Shade 后的完整类名
4. **检查 IDE 配置** - 确保 IDE 能识别 Shade 类

---

## 附录A：Shade 包映射表

| 原始包 | Shade 后包 |
|--------|------------|
| `com.google` | `org.apache.seatunnel.shade.com.google` |
| `com.fasterxml.jackson` | `org.apache.seatunnel.shade.com.fasterxml.jackson` |
| `com.zaxxer.hikari` | `org.apache.seatunnel.shade.com.zaxxer.hikari` |
| `org.eclipse.jetty` | `org.apache.seatunnel.shade.org.eclipse` |
| `org.apache.arrow` | `org.apache.seatunnel.shade.org.apache.arrow` |
| `org.codehaus.janino` | `org.apache.seatunnel.shade.org.codehaus` |
| `scala.tools.nsc` | `org.apache.seatunnel.shade.scala.tools.nsc` |
| `org.apache.commons.lang3` | `org.apache.seatunnel.shade.org.apache.commons.lang3` |
| `org.apache.thrift` | `org.apache.seatunnel.shade.org.apache.thrift` |
| `io.netty` | `org.apache.seatunnel.shade.io.netty` |

---

## 附录B：Shade 模块依赖图

```
seatunnel-api
    │
    ├── seatunnel-guava
    ├── seatunnel-jackson
    └── seatunnel-commons-lang3

seatunnel-engine-server
    │
    ├── seatunnel-hazelcast
    └── seatunnel-jetty9

seatunnel-connectors-v2
    │
    ├── connector-jdbc
    │   └── seatunnel-hikari
    │
    ├── connector-file-*
    │   └── seatunnel-hadoop3
    │
    ├── connector-doris
    │   └── seatunnel-thrift-service
    │
    └── connector-*
        └── seatunnel-arrow

seatunnel-transforms-v2
    │
    ├── seatunnel-janino
    └── seatunnel-scala-compiler
```

# SeaTunnel Dist 模块源码分析
> 文档编号: 015
> 模块路径: seatunnel-dist/
> 更新日期: 2025-12-29
---

## 一、模块概述

### 1.1 模块定位

`seatunnel-dist` 模块是 SeaTunnel 的**发布打包模块**，负责：

1. **二进制包打包** - 将所有模块组装成可分发的 tar.gz 包
2. **源码包打包** - 打包完整源代码用于 Apache 发布
3. **Docker 镜像构建** - 构建官方 Docker 镜像
4. **依赖管理** - 管理所有连接器和驱动依赖

### 1.2 模块结构

```
seatunnel-dist/
├── pom.xml                              # Maven 打包配置（核心）
├── release-docs/                        # 发布文档
│   ├── LICENSE                          # Apache License
│   ├── NOTICE                           # 版权声明
│   └── licenses/                        # 第三方许可证
└── src/
    ├── main/
    │   ├── assembly/                    # Assembly 描述文件
    │   │   ├── assembly-bin.xml         # 二进制包（发布版）
    │   │   ├── assembly-bin-ci.xml      # 二进制包（CI版，包含所有连接器）
    │   │   └── assembly-src.xml         # 源码包
    │   └── docker/
    │       └── Dockerfile               # Docker 构建文件
    └── test/java/                       # 连接器规范检查测试
```

### 1.3 特殊配置

```xml
<properties>
    <!-- 禁用部署到中央仓库 -->
    <maven.deploy.skip>true</maven.deploy.skip>
</properties>
```

---

## 二、Maven Profiles

### 2.1 Profile 概览

| Profile | 激活条件 | 用途 |
|---------|----------|------|
| seatunnel | 默认激活 | CI 构建，包含所有连接器 |
| release | `-Drelease=true` | 正式发布，精简连接器 |
| docker | 默认激活 | Docker 镜像构建 |

### 2.2 seatunnel Profile（默认）

默认激活，用于 CI 构建和开发测试：

```xml
<profile>
    <id>seatunnel</id>
    <activation>
        <activeByDefault>true</activeByDefault>
        <property>
            <name>release</name>
            <value>false</value>
        </property>
    </activation>
    <dependencies>
        <!-- 包含所有 60+ 连接器 -->
        <dependency>
            <groupId>org.apache.seatunnel</groupId>
            <artifactId>connector-jdbc</artifactId>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.seatunnel</groupId>
            <artifactId>connector-kafka</artifactId>
            <scope>provided</scope>
        </dependency>
        <!-- ... 60+ 连接器 ... -->

        <!-- 所有 JDBC 驱动 -->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>8.0.27</version>
            <scope>provided</scope>
        </dependency>
        <!-- ... 20+ JDBC 驱动 ... -->
    </dependencies>
</profile>
```

### 2.3 release Profile

用于 Apache 正式发布，只包含基础连接器：

```xml
<profile>
    <id>release</id>
    <activation>
        <property>
            <name>release</name>
            <value>true</value>
        </property>
    </activation>
    <dependencies>
        <!-- 只包含 Demo 连接器 -->
        <dependency>
            <groupId>org.apache.seatunnel</groupId>
            <artifactId>connector-fake</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.seatunnel</groupId>
            <artifactId>connector-console</artifactId>
        </dependency>
        <!-- CDC 基础包 -->
        <dependency>
            <groupId>org.apache.seatunnel</groupId>
            <artifactId>connector-cdc-base</artifactId>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptors>
                        <!-- 使用发布版 assembly -->
                        <descriptor>src/main/assembly/assembly-bin.xml</descriptor>
                    </descriptors>
                </configuration>
            </plugin>
        </plugins>
    </build>
</profile>
```

### 2.4 docker Profile

构建 Docker 镜像：

```xml
<profile>
    <id>docker</id>
    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <executions>
                    <!-- docker build -->
                    <execution>
                        <id>docker-build</id>
                        <phase>package</phase>
                        <configuration>
                            <executable>docker</executable>
                            <arguments>
                                <argument>buildx</argument>
                                <argument>build</argument>
                                <argument>--load</argument>
                                <argument>-t</argument>
                                <argument>${docker.hub}/${docker.repo}:${docker.tag}</argument>
                                <argument>--build-arg</argument>
                                <argument>VERSION=${project.version}</argument>
                                <argument>--file=src/main/docker/Dockerfile</argument>
                            </arguments>
                        </configuration>
                    </execution>
                    <!-- docker push (多架构) -->
                    <execution>
                        <id>docker-push</id>
                        <phase>install</phase>
                        <configuration>
                            <arguments>
                                <argument>--platform</argument>
                                <argument>linux/amd64,linux/arm64</argument>
                                <argument>--push</argument>
                            </arguments>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</profile>
```

---

## 三、Assembly 配置

### 3.1 assembly-bin-ci.xml（CI 版本）

用于 CI 构建，包含所有连接器和驱动：

```xml
<assembly>
    <id>bin</id>
    <formats>
        <format>tar.gz</format>
    </formats>
    <includeBaseDirectory>true</includeBaseDirectory>

    <!-- 文件集：配置、脚本、文档 -->
    <fileSets>
        <!-- README、config、plugins 目录 -->
        <fileSet>
            <directory>../</directory>
            <includes>
                <include>README.md</include>
                <include>config/**</include>
                <include>plugins/**</include>
            </includes>
        </fileSet>

        <!-- bin 脚本 -->
        <fileSet>
            <directory>../bin</directory>
            <outputDirectory>/bin</outputDirectory>
            <fileMode>0755</fileMode>
        </fileSet>

        <!-- 各引擎启动脚本 -->
        <fileSet>
            <directory>../seatunnel-core/seatunnel-starter/src/main/bin</directory>
            <outputDirectory>/bin</outputDirectory>
            <fileMode>0755</fileMode>
        </fileSet>

        <!-- LICENSE、NOTICE -->
        <fileSet>
            <directory>release-docs</directory>
            <outputDirectory>.</outputDirectory>
        </fileSet>
    </fileSets>

    <!-- 单文件：plugin-mapping.properties -->
    <files>
        <file>
            <source>../plugin-mapping.properties</source>
            <outputDirectory>/connectors</outputDirectory>
        </file>
    </files>

    <!-- 依赖集 -->
    <dependencySets>
        <!-- 日志 JAR -->
        <dependencySet>
            <outputDirectory>/starter/logging</outputDirectory>
            <includes>
                <include>org.slf4j:slf4j-api:jar</include>
                <include>org.apache.logging.log4j:log4j-*:jar</include>
            </includes>
        </dependencySet>

        <!-- Starter JAR -->
        <dependencySet>
            <outputDirectory>/starter</outputDirectory>
            <includes>
                <include>org.apache.seatunnel:seatunnel-*-starter:jar</include>
            </includes>
        </dependencySet>

        <!-- 所有连接器 JAR -->
        <dependencySet>
            <outputDirectory>/connectors</outputDirectory>
            <includes>
                <include>org.apache.seatunnel:connector-*:jar</include>
            </includes>
            <excludes>
                <exclude>org.apache.seatunnel:connector-common</exclude>
                <exclude>org.apache.seatunnel:connector-file-base</exclude>
            </excludes>
        </dependencySet>

        <!-- JDBC 驱动、Hadoop、Hive JAR -->
        <dependencySet>
            <outputDirectory>/lib</outputDirectory>
            <includes>
                <include>mysql:mysql-connector-java:jar</include>
                <include>org.postgresql:postgresql:jar</include>
                <include>com.oracle.database.jdbc:ojdbc8:jar</include>
                <include>org.apache.seatunnel:seatunnel-hadoop3-3.1.4-uber:jar:*:optional</include>
                <include>org.apache.seatunnel:seatunnel-transforms-v2:jar</include>
                <!-- ... 更多驱动 ... -->
            </includes>
        </dependencySet>
    </dependencySets>
</assembly>
```

### 3.2 assembly-bin.xml（发布版本）

用于正式发布，只包含基础连接器：

```xml
<assembly>
    <id>bin</id>
    <!-- 基本结构与 CI 版相同 -->

    <dependencySets>
        <!-- 只包含 Demo 连接器 -->
        <dependencySet>
            <outputDirectory>/connectors</outputDirectory>
            <includes>
                <include>org.apache.seatunnel:connector-fake:jar</include>
                <include>org.apache.seatunnel:connector-console:jar</include>
                <include>org.apache.seatunnel:connector-cdc-base:jar</include>
            </includes>
        </dependencySet>

        <!-- Hadoop Uber JAR -->
        <dependencySet>
            <outputDirectory>/lib</outputDirectory>
            <includes>
                <include>org.apache.seatunnel:seatunnel-hadoop3-3.1.4-uber:jar:*:optional</include>
                <include>org.apache.seatunnel:seatunnel-hadoop-aws:jar:*:optional</include>
                <include>org.apache.seatunnel:seatunnel-transforms-v2:jar</include>
            </includes>
        </dependencySet>
    </dependencySets>
</assembly>
```

### 3.3 assembly-src.xml（源码包）

打包完整源代码：

```xml
<assembly>
    <id>src</id>
    <formats>
        <format>tar.gz</format>
    </formats>
    <baseDirectory>${project.build.finalName}-src</baseDirectory>

    <fileSets>
        <fileSet>
            <directory>../</directory>
            <includes>
                <include>**/*</include>
            </includes>
            <excludes>
                <!-- 排除构建产物 -->
                <exclude>**/target/**</exclude>
                <exclude>**/*.jar</exclude>
                <exclude>**/*.class</exclude>

                <!-- 排除 IDE 文件 -->
                <exclude>**/.idea/**</exclude>
                <exclude>**/*.iml</exclude>
                <exclude>**/.settings/**</exclude>

                <!-- 排除临时文件 -->
                <exclude>**/logs/**</exclude>
                <exclude>**/*.log</exclude>
            </excludes>
        </fileSet>
    </fileSets>
</assembly>
```

---

## 四、分发包目录结构

### 4.1 完整目录结构

```
apache-seatunnel-{version}/
├── LICENSE                          # Apache License 2.0
├── NOTICE                           # 版权声明
├── README.md                        # 项目说明
├── DISCLAIMER                       # Apache 免责声明（孵化期）
├── bin/                             # 启动脚本
│   ├── seatunnel.sh                 # Zeta 引擎启动（Unix）
│   ├── seatunnel.cmd                # Zeta 引擎启动（Windows）
│   ├── seatunnel-cluster.sh         # 集群启动
│   ├── stop-seatunnel-cluster.sh    # 集群停止
│   ├── install-plugin.sh            # 插件安装
│   ├── start-seatunnel-flink-13-connector-v2.sh
│   ├── start-seatunnel-flink-15-connector-v2.sh
│   ├── start-seatunnel-flink-20-connector-v2.sh
│   ├── start-seatunnel-spark-2-connector-v2.sh
│   └── start-seatunnel-spark-3-connector-v2.sh
├── config/                          # 配置文件
│   ├── seatunnel.yaml               # Zeta 引擎配置
│   ├── hazelcast.yaml               # Hazelcast 集群配置
│   ├── hazelcast-client.yaml        # Hazelcast 客户端配置
│   ├── log4j2.properties            # 日志配置
│   └── v2.batch.config.template     # 作业配置模板
├── connectors/                      # 连接器 JAR
│   ├── plugin-mapping.properties    # 插件映射配置
│   ├── connector-fake-*.jar
│   ├── connector-console-*.jar
│   ├── connector-jdbc-*.jar
│   ├── connector-kafka-*.jar
│   └── ...（60+ 连接器）
├── lib/                             # 公共库
│   ├── seatunnel-transforms-v2.jar  # 转换插件
│   ├── seatunnel-hadoop3-3.1.4-uber.jar  # Hadoop 客户端
│   ├── seatunnel-hadoop-aws.jar     # AWS S3 支持
│   ├── mysql-connector-java-*.jar   # MySQL 驱动
│   ├── postgresql-*.jar             # PostgreSQL 驱动
│   └── ...（20+ JDBC 驱动）
├── starter/                         # 引擎启动器
│   ├── logging/                     # 日志依赖
│   │   ├── slf4j-api-*.jar
│   │   ├── log4j-api-*.jar
│   │   ├── log4j-core-*.jar
│   │   └── log4j-slf4j-impl-*.jar
│   ├── seatunnel-starter.jar        # Zeta 引擎
│   ├── seatunnel-flink-13-starter.jar
│   ├── seatunnel-flink-15-starter.jar
│   ├── seatunnel-flink-20-starter.jar
│   ├── seatunnel-spark-2-starter.jar
│   └── seatunnel-spark-3-starter.jar
├── plugins/                         # 插件依赖目录（用户自定义）
│   └── .gitkeep
└── licenses/                        # 第三方许可证
    └── ...（95 个许可证文件）
```

### 4.2 目录用途说明

| 目录 | 用途 | 说明 |
|------|------|------|
| bin/ | 启动脚本 | 各引擎的启动、停止脚本 |
| config/ | 配置文件 | 引擎配置、日志配置、作业模板 |
| connectors/ | 连接器 | 所有 Source/Sink 连接器 JAR |
| lib/ | 公共库 | JDBC 驱动、Hadoop、转换插件 |
| starter/ | 启动器 | 各引擎的 Fat JAR |
| plugins/ | 自定义插件 | 用户放置额外依赖 |
| licenses/ | 许可证 | 第三方依赖的许可证文件 |

---

## 五、Docker 镜像构建

### 5.1 Dockerfile

```dockerfile
# 多阶段构建
FROM openjdk:8 as builder

ARG VERSION

# 解压分发包
COPY ./target/apache-seatunnel-${VERSION}-bin.tar.gz /opt/
RUN cd /opt && \
    tar -zxvf apache-seatunnel-${VERSION}-bin.tar.gz && \
    mv apache-seatunnel-${VERSION} seatunnel && \
    rm apache-seatunnel-${VERSION}-bin.tar.gz && \
    # 修改日志配置：输出到控制台
    sed -i 's/#rootLogger.appenderRef.consoleStdout.ref/rootLogger.appenderRef.consoleStdout.ref/' \
        seatunnel/config/log4j2.properties && \
    sed -i 's/#rootLogger.appenderRef.consoleStderr.ref/rootLogger.appenderRef.consoleStderr.ref/' \
        seatunnel/config/log4j2.properties && \
    sed -i 's/rootLogger.appenderRef.file.ref/#rootLogger.appenderRef.file.ref/' \
        seatunnel/config/log4j2.properties && \
    # 复制集群配置
    cp seatunnel/config/hazelcast-master.yaml seatunnel/config/hazelcast-worker.yaml

# 最终镜像
FROM openjdk:8
COPY --from=builder /opt/seatunnel /opt/seatunnel
WORKDIR /opt/seatunnel
```

### 5.2 Docker 构建命令

```bash
# 构建镜像
mvn package -pl seatunnel-dist -am -Pdocker

# 手动构建
docker buildx build \
    --load \
    -t apache/seatunnel:latest \
    --build-arg VERSION=2.3.x \
    --file=src/main/docker/Dockerfile \
    .

# 多架构构建并推送
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --push \
    -t apache/seatunnel:latest \
    --build-arg VERSION=2.3.x \
    --file=src/main/docker/Dockerfile \
    .
```

### 5.3 Docker 使用

```bash
# 运行作业
docker run --rm \
    apache/seatunnel:latest \
    bash ./bin/seatunnel.sh \
    -e local \
    -c config/v2.batch.config.template

# 挂载自定义配置
docker run --rm \
    -v /path/to/job.conf:/opt/seatunnel/config/job.conf \
    apache/seatunnel:latest \
    bash ./bin/seatunnel.sh \
    -c config/job.conf
```

---

## 六、依赖管理

### 6.1 连接器依赖（60+）

```xml
<!-- 消息队列 -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-kafka</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-pulsar</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-rocketmq</artifactId>
</dependency>

<!-- 数据库 -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-jdbc</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-clickhouse</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-doris</artifactId>
</dependency>

<!-- CDC -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-cdc-mysql</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-cdc-postgres</artifactId>
</dependency>

<!-- 文件 -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-file-hadoop</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-file-s3</artifactId>
</dependency>

<!-- 数据湖 -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-iceberg</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-hudi</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-paimon</artifactId>
</dependency>
```

### 6.2 JDBC 驱动依赖（20+）

| 数据库 | 驱动 | 版本 |
|--------|------|------|
| MySQL | mysql-connector-java | 8.0.27 |
| PostgreSQL | postgresql | 42.4.3 |
| Oracle | ojdbc8 | 12.2.0.1 |
| SQL Server | mssql-jdbc | 9.2.1.jre8 |
| SQLite | sqlite-jdbc | 3.39.3.0 |
| DB2 | db2jcc | db2jcc4 |
| 达梦 | DmJdbcDriver18 | 8.1.2.141 |
| SAP HANA | ngdbc | 2.23.10 |
| Teradata | terajdbc4 | 17.20.00.12 |
| Redshift | redshift-jdbc42 | 2.1.0.9 |
| Snowflake | snowflake-jdbc | 3.13.29 |
| TiDB | tikv-client-java | 3.3.5 |
| Presto | presto-jdbc | 0.279 |
| Trino | trino-jdbc | 460 |
| Phoenix | ali-phoenix-shaded-thin-client | 5.2.5-HBase-2.x |

### 6.3 Hadoop/Hive 依赖

```xml
<!-- Hadoop 3.1.4 Uber JAR -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-hadoop3-3.1.4-uber</artifactId>
    <classifier>optional</classifier>
</dependency>

<!-- Hadoop AWS 支持 -->
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-hadoop-aws</artifactId>
    <classifier>optional</classifier>
</dependency>

<!-- 阿里云 OSS -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aliyun</artifactId>
    <version>3.1.4</version>
</dependency>
<dependency>
    <groupId>com.aliyun.oss</groupId>
    <artifactId>aliyun-sdk-oss</artifactId>
    <version>3.4.1</version>
</dependency>

<!-- Hive -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-exec</artifactId>
    <version>3.1.3</version>
</dependency>
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-jdbc</artifactId>
    <version>3.1.3</version>
</dependency>
```

---

## 七、构建命令

### 7.1 完整构建（开发/CI）

```bash
# 构建包含所有连接器的分发包
./mvnw clean package -pl seatunnel-dist -am -DskipTests

# 生成的文件
target/
├── apache-seatunnel-{version}-bin.tar.gz  # 二进制包
└── apache-seatunnel-{version}/            # 解压后目录
```

### 7.2 发布构建

```bash
# 构建发布版本（只含基础连接器）
./mvnw clean package -pl seatunnel-dist -am -DskipTests -Drelease=true

# 生成的文件
target/
├── apache-seatunnel-{version}-bin.tar.gz  # 二进制包（精简）
└── apache-seatunnel-{version}-src.tar.gz  # 源码包
```

### 7.3 Docker 构建

```bash
# 构建 Docker 镜像
./mvnw clean package -pl seatunnel-dist -am -DskipTests -Pdocker

# 验证镜像
./mvnw verify -pl seatunnel-dist -Pdocker

# 推送镜像（多架构）
./mvnw install -pl seatunnel-dist -Pdocker
```

### 7.4 单独构建连接器

```bash
# 只构建 JDBC 连接器
./mvnw clean package -pl seatunnel-connectors-v2/connector-jdbc -am -DskipTests

# 复制到分发目录
cp seatunnel-connectors-v2/connector-jdbc/target/connector-jdbc-*.jar \
   seatunnel-dist/target/apache-seatunnel-*/connectors/
```

---

## 八、发布流程

### 8.1 Apache 发布步骤

1. **准备版本**
```bash
# 更新版本号
./mvnw versions:set -DnewVersion=2.3.x

# 构建发布包
./mvnw clean package -pl seatunnel-dist -am -Drelease=true -DskipTests
```

2. **生成签名**
```bash
cd target
gpg --armor --detach-sig apache-seatunnel-2.3.x-bin.tar.gz
gpg --armor --detach-sig apache-seatunnel-2.3.x-src.tar.gz

# 生成 SHA512
sha512sum apache-seatunnel-2.3.x-bin.tar.gz > apache-seatunnel-2.3.x-bin.tar.gz.sha512
sha512sum apache-seatunnel-2.3.x-src.tar.gz > apache-seatunnel-2.3.x-src.tar.gz.sha512
```

3. **上传到 SVN**
```bash
svn add apache-seatunnel-2.3.x-*
svn commit -m "Add SeaTunnel 2.3.x release"
```

### 8.2 发布文件清单

| 文件 | 说明 |
|------|------|
| apache-seatunnel-{version}-bin.tar.gz | 二进制分发包 |
| apache-seatunnel-{version}-bin.tar.gz.asc | GPG 签名 |
| apache-seatunnel-{version}-bin.tar.gz.sha512 | SHA512 校验和 |
| apache-seatunnel-{version}-src.tar.gz | 源码包 |
| apache-seatunnel-{version}-src.tar.gz.asc | GPG 签名 |
| apache-seatunnel-{version}-src.tar.gz.sha512 | SHA512 校验和 |

---

## 九、测试类

### 9.1 连接器规范检查

```java
// ConnectorSpecificationCheckTest.java
public class ConnectorSpecificationCheckTest {

    @Test
    void checkAllConnectorsHaveFactory() {
        // 检查所有连接器是否正确实现 Factory
        ServiceLoader<Factory> factories = ServiceLoader.load(Factory.class);
        // 验证 SPI 配置正确
    }

    @Test
    void checkAllConnectorsHaveOptionRule() {
        // 检查所有连接器是否定义了配置选项
    }
}
```

### 9.2 Transform 规范检查

```java
// TransformSpecificationCheckTest.java
public class TransformSpecificationCheckTest {

    @Test
    void checkAllTransformsHaveFactory() {
        // 检查所有转换器是否正确实现 Factory
    }
}
```

---

## 十、与其他模块的关系

### 10.1 依赖图

```
┌─────────────────────────────────────────────────────────────┐
│                       seatunnel-dist                         │
│                    (打包所有模块)                            │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ seatunnel-core│    │ connectors-v2 │    │ transforms-v2 │
│   (Starters)  │    │ (60+ 连接器)  │    │  (转换插件)   │
└───────────────┘    └───────────────┘    └───────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
                    ┌───────────────┐
                    │  seatunnel-api│
                    │   (核心 API)  │
                    └───────────────┘
```

### 10.2 打包流程

```
1. 编译所有模块
   ./mvnw compile

2. 打包各模块 JAR
   ./mvnw package

3. Assembly 插件收集依赖
   - 从各模块 target/ 收集 JAR
   - 从 Maven 仓库下载 JDBC 驱动
   - 复制配置文件和脚本

4. 生成分发包
   - tar.gz 二进制包
   - tar.gz 源码包
```

---

## 十一、总结

### 11.1 模块特点

| 特点 | 说明 |
|------|------|
| 多 Profile | 支持 CI、发布、Docker 等不同构建场景 |
| 完整打包 | 包含所有连接器、驱动、配置、脚本 |
| Docker 支持 | 多阶段构建、多架构支持 |
| Apache 合规 | LICENSE、NOTICE、第三方许可证 |

### 11.2 核心文件

| 文件 | 用途 |
|------|------|
| pom.xml | Maven 依赖和 Profile 配置 |
| assembly-bin-ci.xml | CI 版二进制包打包规则 |
| assembly-bin.xml | 发布版二进制包打包规则 |
| assembly-src.xml | 源码包打包规则 |
| Dockerfile | Docker 镜像构建 |

### 11.3 分发包内容

| 目录 | 数量 | 说明 |
|------|------|------|
| bin/ | 20 文件 | 启动脚本 |
| connectors/ | 80+ JAR | 连接器插件 |
| lib/ | 28 JAR | 公共库和驱动 |
| starter/ | 7 JAR | 引擎启动器 |
| licenses/ | 95 文件 | 第三方许可证 |

---

## 附录A：连接器完整列表（CI 版本）

| 类别 | 连接器 |
|------|--------|
| 消息队列 | Kafka, Pulsar, RocketMQ, RabbitMQ, ActiveMQ, AmazonSqs |
| 关系数据库 | JDBC, ClickHouse, Doris, StarRocks, MaxCompute, Databend |
| NoSQL | MongoDB, Redis, Cassandra, HBase, Neo4j, Elasticsearch |
| CDC | MySQL-CDC, Postgres-CDC, Oracle-CDC, SQLServer-CDC, MongoDB-CDC, TiDB-CDC |
| 文件系统 | HdfsFile, LocalFile, S3File, OssFile, CosFile, FtpFile, SftpFile |
| 数据湖 | Iceberg, Hudi, Paimon |
| 时序数据库 | IoTDB, InfluxDB, TDengine, Prometheus |
| 向量数据库 | Milvus, Qdrant |
| HTTP | Http-Base, GitHub, GitLab, Jira, Notion, GraphQL |
| 其他 | Fake, Console, Assert, Email, DingTalk, Slack, Sentry |

---

## 附录B：JDBC 驱动完整列表

| 数据库 | Maven 坐标 | 版本 |
|--------|------------|------|
| MySQL | mysql:mysql-connector-java | 8.0.27 |
| PostgreSQL | org.postgresql:postgresql | 42.4.3 |
| PostGIS | net.postgis:postgis-jdbc | 2.5.1 |
| Oracle | com.oracle.database.jdbc:ojdbc8 | 12.2.0.1 |
| SQL Server | com.microsoft.sqlserver:mssql-jdbc | 9.2.1.jre8 |
| SQLite | org.xerial:sqlite-jdbc | 3.39.3.0 |
| DB2 | com.ibm.db2.jcc:db2jcc | db2jcc4 |
| 达梦 | com.dameng:DmJdbcDriver18 | 8.1.2.141 |
| SAP HANA | com.sap.cloud.db.jdbc:ngdbc | 2.23.10 |
| Teradata | com.teradata.jdbc:terajdbc4 | 17.20.00.12 |
| Redshift | com.amazon.redshift:redshift-jdbc42 | 2.1.0.9 |
| Snowflake | net.snowflake:snowflake-jdbc | 3.13.29 |
| TiDB | org.tikv:tikv-client-java | 3.3.5 |
| Presto | com.facebook.presto:presto-jdbc | 0.279 |
| Trino | io.trino:trino-jdbc | 460 |
| Phoenix | com.aliyun.phoenix:ali-phoenix-shaded-thin-client | 5.2.5-HBase-2.x |
| Tablestore | com.aliyun.openservices:tablestore-jdbc | 5.13.9 |

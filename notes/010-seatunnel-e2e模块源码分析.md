# SeaTunnel E2E 模块源码分析

> 文档编号: 010
> 模块路径: seatunnel-e2e/
> 更新日期: 2025-12-29

---

## 一、模块概述

### 1.1 功能定位

`seatunnel-e2e` 是 SeaTunnel 的**端到端测试模块**，基于 TestContainers 框架实现了完整的集成测试能力：

- 在 Docker 容器中运行真实的 SeaTunnel 作业
- 支持多种执行引擎（Zeta、Flink、Spark）
- 支持 65+ 连接器的 E2E 测试
- 包含线程泄漏检测、ClassLoader 检查等高级功能

### 1.2 模块结构

```
seatunnel-e2e/
├── pom.xml
├── seatunnel-e2e-common/              # 公共测试框架
│   └── src/test/java/org/apache/seatunnel/e2e/
│       ├── common/
│       │   ├── TestSuiteBase.java           # 测试基类
│       │   ├── TestResource.java            # 资源接口
│       │   ├── AbstractFlinkContainer.java
│       │   ├── AbstractSparkContainer.java
│       │   ├── container/                   # 容器实现
│       │   │   ├── TestContainer.java       # 容器接口
│       │   │   ├── AbstractTestContainer.java
│       │   │   ├── seatunnel/SeaTunnelContainer.java
│       │   │   ├── flink/Flink*.java
│       │   │   └── spark/Spark*.java
│       │   ├── junit/                       # JUnit 5 扩展
│       │   │   ├── TestCaseInvocationContextProvider.java
│       │   │   ├── ContainerTestingExtension.java
│       │   │   └── TimingExtension.java
│       │   └── util/                        # 工具类
│       │       ├── ContainerUtil.java
│       │       └── JdbcUtil.java
│       ├── source/inmemory/                # 内存 Source（测试用）
│       └── sink/inmemory/                  # 内存 Sink（测试用）
│
├── seatunnel-connector-v2-e2e/         # 连接器 E2E 测试（65 个子模块）
│   ├── connector-fake-e2e/
│   ├── connector-jdbc-e2e/
│   ├── connector-kafka-e2e/
│   ├── connector-cdc-mysql-e2e/
│   └── ...
│
├── seatunnel-engine-e2e/               # 引擎 E2E 测试
│   ├── connector-seatunnel-e2e-base/
│   ├── connector-console-seatunnel-e2e/
│   └── seatunnel-engine-k8s-e2e/
│
├── seatunnel-transforms-v2-e2e/        # 转换器 E2E 测试
│
└── seatunnel-core-e2e/                 # 核心模块 E2E 测试
```

### 1.3 模块统计

| 子模块 | E2E 测试模块数 | Java 文件数 | 主要内容 |
|--------|----------------|-------------|----------|
| e2e-common | 1 | 53 | 公共测试框架 |
| connector-v2-e2e | 65 | 200+ | 连接器测试 |
| engine-e2e | 4 | 30+ | 引擎测试 |
| transforms-v2-e2e | 7 | 20+ | 转换器测试 |
| core-e2e | 1 | 5+ | 核心模块测试 |
| **总计** | **78** | **310+** | - |

### 1.4 依赖技术栈

```xml
<dependencies>
    <!-- TestContainers - Docker 容器管理 -->
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers</artifactId>
    </dependency>

    <!-- JUnit 4 兼容（TestContainers 需要） -->
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
    </dependency>

    <!-- Awaitility - 异步断言 -->
    <dependency>
        <groupId>org.awaitility</groupId>
        <artifactId>awaitility</artifactId>
    </dependency>

    <!-- REST Assured - HTTP API 测试 -->
    <dependency>
        <groupId>io.rest-assured</groupId>
        <artifactId>rest-assured</artifactId>
    </dependency>
</dependencies>
```

---

## 二、测试框架核心设计

### 2.1 类层次结构

```
TestSuiteBase                          # 测试基类
    ↓ extends
具体测试类 (FakeIT, JdbcMySqlIT, ...)

TestContainer (接口)                   # 容器接口
    ↓ implements
AbstractTestContainer                  # 抽象容器
    ├── SeaTunnelContainer            # Zeta 引擎容器
    ├── AbstractTestFlinkContainer    # Flink 容器
    │   ├── Flink13Container
    │   ├── Flink15Container
    │   └── Flink20Container
    └── AbstractTestSparkContainer    # Spark 容器
        ├── Spark2Container
        └── Spark3Container
```

### 2.2 TestSuiteBase

**路径**: `e2e/common/TestSuiteBase.java`

**职责**: 所有 E2E 测试的基类

```java
@ExtendWith({
    ContainerTestingExtension.class,      // 容器生命周期管理
    TestLoggerExtension.class,            // 日志增强
    TestCaseInvocationContextProvider.class, // 多容器参数化测试
    TimingExtension.class                 // 测试计时
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestSuiteBase {

    protected static final Network NETWORK = TestContainer.NETWORK;

    @TestContainers
    private TestContainersFactory containersFactory = ContainerUtil::discoverTestContainers;

    protected DockerClient dockerClient = DockerClientFactory.lazyClient();
}
```

**关键设计**:
- `@TestInstance(PER_CLASS)`: 测试类级别共享实例，容器可复用
- `@TestContainers`: 自定义注解，注入容器工厂
- 共享 Docker Network，允许容器间通信

### 2.3 TestContainer 接口

**路径**: `e2e/common/container/TestContainer.java`

```java
public interface TestContainer extends TestResource {

    // 共享网络
    Network NETWORK = Network.builder()
            .createNetworkCmdModifier(cmd -> cmd.withName("SEATUNNEL-" + UUID.randomUUID()))
            .enableIpv6(false)
            .build();

    // 容器标识
    TestContainerId identifier();

    // 执行额外命令（扩展点）
    void executeExtraCommands(ContainerExtendedFactory extendedFactory);

    // 执行作业
    Container.ExecResult executeJob(String confFile) throws IOException, InterruptedException;

    // 带变量执行作业
    Container.ExecResult executeJob(String confFile, List<String> variables);

    // 带 JobId 执行作业
    Container.ExecResult executeJob(String confFile, String jobId, String... variables);

    // 保存点
    Container.ExecResult savepointJob(String jobId);

    // 恢复作业
    Container.ExecResult restoreJob(String confFile, String jobId, String... variables);

    // 取消作业
    Container.ExecResult cancelJob(String jobId);

    // 获取作业状态
    String getJobStatus(String jobId);

    // 获取服务日志
    String getServerLogs();

    // 复制文件到容器
    void copyFileToContainer(String path, String targetPath);
}
```

### 2.4 AbstractTestContainer

**路径**: `e2e/common/container/AbstractTestContainer.java`

**职责**: 所有容器实现的公共逻辑

```java
public abstract class AbstractTestContainer implements TestContainer {

    public static final String SEATUNNEL_HOME = "/tmp/seatunnel/";
    protected static final String CONTAINER_VOLUME_MOUNT_PATH = "/tmp/seatunnel_mnt";

    // 模板方法：子类必须实现
    protected abstract String getDockerImage();
    protected abstract String getStartModuleName();
    protected abstract String getStartShellName();
    protected abstract String getConnectorModulePath();
    protected abstract String getConnectorType();
    protected abstract String getConnectorNamePrefix();
    protected abstract List<String> getExtraStartShellCommands();

    // 执行作业
    protected Container.ExecResult executeJob(
            GenericContainer<?> container, String confFile, String jobId, List<String> variables) {

        // 1. 复制配置文件到容器
        final String confInContainerPath = copyConfigFileToContainer(container, confFile);

        // 2. 复制连接器 JAR 到容器
        copyConnectorJarToContainer(container, confFile, ...);

        // 3. 构建命令
        final List<String> command = new ArrayList<>();
        command.add(adaptPathForWin(Paths.get(SEATUNNEL_HOME, "bin", getStartShellName())));
        command.add("--config");
        command.add(adaptPathForWin(confInContainerPath));
        command.add("--name");
        command.add(new File(confInContainerPath).getName());

        if (StringUtils.isNoneEmpty(jobId)) {
            command.add("--set-job-id");
            command.add(jobId);
        }

        // 4. 执行命令
        return executeCommand(container, command);
    }

    // 执行命令
    protected Container.ExecResult executeCommand(
            GenericContainer<?> container, List<String> command) {
        String commandStr = String.join(" ", command);
        LOG.info("Execute command in container: {}", commandStr);

        Container.ExecResult execResult = container.execInContainer("bash", "-c", commandStr);

        // 打印 STDOUT/STDERR
        if (execResult.getStdout() != null && !execResult.getStdout().isEmpty()) {
            LOG.info("STDOUT: {}", execResult.getStdout());
        }
        if (execResult.getStderr() != null && !execResult.getStderr().isEmpty()) {
            LOG.error("STDERR: {}", execResult.getStderr());
        }

        return execResult;
    }
}
```

---

## 三、SeaTunnel 容器实现

### 3.1 SeaTunnelContainer

**路径**: `e2e/common/container/seatunnel/SeaTunnelContainer.java`

**职责**: Zeta 引擎的 E2E 测试容器

```java
@NoArgsConstructor
@Slf4j
@AutoService(TestContainer.class)  // SPI 自动注册
public class SeaTunnelContainer extends AbstractTestContainer {

    protected static final String JDK_DOCKER_IMAGE = "seatunnelhub/openjdk:8u342";
    private static final String CLIENT_SHELL = "seatunnel.sh";
    protected static final String SERVER_SHELL = "seatunnel-cluster.sh";

    protected GenericContainer<?> server;
    private final AtomicInteger runningCount = new AtomicInteger();

    @Override
    public void startUp() throws Exception {
        server = createSeaTunnelServer();
    }

    private GenericContainer<?> createSeaTunnelServer(Network NETWORK) {
        GenericContainer<?> server = new GenericContainer<>(getDockerImage())
                .withNetwork(NETWORK)
                .withEnv("TZ", "UTC")
                .withCommand(buildStartCommand())
                .withNetworkAliases("server")
                .withFileSystemBind("/tmp", "/opt/hive")
                .withFileSystemBind(HOST_VOLUME_MOUNT_PATH, CONTAINER_VOLUME_MOUNT_PATH, BindMode.READ_WRITE)
                // 等待服务启动
                .waitingFor(Wait.forLogMessage(".*received new worker register:.*", 1));

        // 复制 SeaTunnel 发行包到容器
        copySeaTunnelStarterToContainer(server);

        // 端口映射
        server.setPortBindings(Arrays.asList("5801:5801", "8080:8080"));

        // 复制配置文件
        server.withCopyFileToContainer(
                MountableFile.forHostPath(PROJECT_ROOT_PATH + "/seatunnel-e2e/.../resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        // 复制 Hadoop 依赖
        server.withCopyFileToContainer(
                MountableFile.forHostPath(PROJECT_ROOT_PATH + "/.../seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());

        server.start();
        return server;
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables) {
        log.info("test in container: {}", identifier());

        // 记录执行前的线程
        List<String> beforeThreads = ContainerUtil.getJVMThreadNames(server);

        runningCount.incrementAndGet();
        Container.ExecResult result = executeJob(server, confFile, null, variables);
        runningCount.decrementAndGet();

        // 线程泄漏检测
        List<String> afterThreads = ContainerUtil.getJVMThreadNames(server);
        afterThreads = removeSystemThread(beforeThreads, afterThreads);

        if (!afterThreads.isEmpty()) {
            // 等待 120s 让线程释放
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        List<String> threads = ContainerUtil.getJVMThreadNames(server);
                        Assertions.assertTrue(threads.isEmpty(),
                                "There are still threads running: " + threads);
                    });
        }

        return result;
    }

    @Override
    public String getJobStatus(String jobId) {
        HttpGet get = new HttpGet(String.format(
                "http://%s:%d/job-info/%s",
                server.getHost(), server.getMappedPort(8080), jobId));

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            CloseableHttpResponse response = client.execute(get);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String jobStatus = EntityUtils.toString(response.getEntity());
                ObjectNode jsonNodes = JsonUtils.parseObject(jobStatus);
                return jsonNodes.get("jobStatus").asText();
            }
        }
        return null;
    }

    @Override
    public void tearDown() throws Exception {
        if (server != null) {
            server.execInContainer("rm", "-rf", CONTAINER_VOLUME_MOUNT_PATH);
            server.close();
        }
    }
}
```

**关键特性**:
- 使用 `seatunnelhub/openjdk:8u342` 镜像
- 等待日志 `received new worker register` 确认启动
- 线程泄漏检测（执行前后对比线程列表）
- 通过 REST API 获取作业状态

### 3.2 线程泄漏检测

```java
// 移除系统线程，只保留可能泄漏的线程
private static boolean isSystemThread(String s) {
    Pattern aqsThread = Pattern.compile("pool-[0-9]-thread-[0-9]");
    return s.startsWith("hz.main")                          // Hazelcast
            || s.startsWith("seatunnel-coordinator-service") // 协调服务
            || s.startsWith("GC task thread")                // GC
            || s.contains("CompilerThread")                  // JIT
            || s.startsWith("SeaTunnel-CompletableFuture-Thread-")
            || s.contains("ForkJoinPool.commonPool")
            || s.contains("DestroyJavaVM")
            || s.startsWith("heartbeat")
            || s.startsWith("LeaseRenewer")                  // HDFS
            || s.startsWith("commons-pool-evictor");         // Redis Pool
}

// 已知问题的线程（第三方库问题）
protected boolean isIssueWeAlreadyKnow(String threadName) {
    return threadName.startsWith("ClickHouseClientWorker")    // ClickHouse
            || threadName.startsWith("Okio Watchdog")         // InfluxDB
            || threadName.startsWith("OkHttp TaskRunner")
            || threadName.startsWith("SessionExecutor")       // IoTDB
            || threadName.startsWith("iceberg-worker-pool")   // Iceberg
            || threadName.startsWith("cluster-")              // MongoDB
            || threadName.startsWith("grpc");                 // gRPC
}
```

---

## 四、JUnit 5 扩展机制

### 4.1 TestCaseInvocationContextProvider

**路径**: `e2e/common/junit/TestCaseInvocationContextProvider.java`

**职责**: 实现多容器参数化测试

```java
public class TestCaseInvocationContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideInvocationContexts(ExtensionContext context) {
        // 获取所有可用的测试容器
        List<TestContainer> containers = discoverTestContainers();

        // 为每个容器创建一个调用上下文
        return containers.stream().map(container ->
            new TestTemplateInvocationContext() {
                @Override
                public String getDisplayName(int invocationIndex) {
                    return container.identifier().name();
                }

                @Override
                public List<Extension> getAdditionalExtensions() {
                    return Collections.singletonList(
                        new ParameterResolver() {
                            @Override
                            public boolean supportsParameter(ParameterContext parameterContext, ...) {
                                return parameterContext.getParameter().getType() == TestContainer.class;
                            }

                            @Override
                            public Object resolveParameter(ParameterContext parameterContext, ...) {
                                return container;
                            }
                        }
                    );
                }
            }
        );
    }
}
```

### 4.2 ContainerTestingExtension

**路径**: `e2e/common/junit/ContainerTestingExtension.java`

**职责**: 管理容器生命周期

```java
public class ContainerTestingExtension implements BeforeAllCallback, AfterAllCallback {

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // 启动所有测试容器
        TestContainersFactory factory = getContainersFactory(context);
        List<TestContainer> containers = factory.create();
        for (TestContainer container : containers) {
            container.startUp();
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        // 关闭所有测试容器
        List<TestContainer> containers = getContainers(context);
        for (TestContainer container : containers) {
            container.tearDown();
        }
    }
}
```

### 4.3 TimingExtension

**路径**: `e2e/common/junit/TimingExtension.java`

**职责**: 记录测试执行时间

```java
public class TimingExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {

    private static final Logger LOG = LoggerFactory.getLogger(TimingExtension.class);

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        getStore(context).put(START_TIME, System.currentTimeMillis());
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        long startTime = getStore(context).remove(START_TIME, long.class);
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Test {} took {} ms", context.getDisplayName(), duration);
    }
}
```

---

## 五、编写 E2E 测试

### 5.1 基本测试示例

**路径**: `connector-fake-e2e/src/test/java/.../FakeIT.java`

```java
public class FakeIT extends TestSuiteBase {

    @TestTemplate
    public void testFakeConnector(TestContainer container)
            throws IOException, InterruptedException {

        // 执行作业配置文件
        Container.ExecResult result = container.executeJob("/fake_to_assert.conf");

        // 验证退出码为 0
        Assertions.assertEquals(0, result.getExitCode());
    }
}
```

### 5.2 测试配置文件示例

**路径**: `connector-fake-e2e/src/test/resources/fake_to_assert.conf`

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        c_string = string
        c_int = int
        c_double = double
        c_timestamp = timestamp
      }
    }
    plugin_output = "fake"
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "tmp1"
    query = "select * from dual"
  }
}

sink {
  Assert {
    plugin_input = "tmp1"
    rules {
      row_rules = [
        {
          rule_type = MAX_ROW
          rule_value = 5
        }
      ],
      field_rules = [
        {
          field_name = c_string
          field_type = string
          field_value = [
            { rule_type = NOT_NULL }
          ]
        }
      ]
    }
  }
}
```

### 5.3 禁用特定容器的测试

```java
public class SomeConnectorIT extends TestSuiteBase {

    @TestTemplate
    @DisabledOnContainer(
        value = {TestContainerId.SPARK_2},
        disabledReason = "Spark 2.x doesn't support this feature"
    )
    public void testFeature(TestContainer container) {
        // 此测试不会在 Spark 2 上运行
    }
}
```

### 5.4 使用外部容器（如数据库）

```java
public class JdbcMySqlIT extends TestSuiteBase implements TestResource {

    private MySQLContainer<?> mysqlContainer;

    @BeforeAll
    public void startMySql() {
        mysqlContainer = new MySQLContainer<>("mysql:8.0")
                .withNetwork(NETWORK)
                .withNetworkAliases("mysql")
                .withDatabaseName("test")
                .withUsername("root")
                .withPassword("password");
        mysqlContainer.start();

        // 初始化测试数据
        initTestData(mysqlContainer);
    }

    @TestTemplate
    public void testJdbcSource(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob("/jdbc_mysql_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode());
    }

    @AfterAll
    public void stopMySql() {
        if (mysqlContainer != null) {
            mysqlContainer.stop();
        }
    }
}
```

---

## 六、内存测试连接器

### 6.1 InMemorySource

**路径**: `e2e/source/inmemory/InMemorySource.java`

**用途**: 单元测试和引擎测试中的内存数据源

```java
public class InMemorySource implements SeaTunnelSource<SeaTunnelRow, InMemorySourceSplit, InMemoryState> {

    private final List<SeaTunnelRow> testData;

    @Override
    public SourceReader<SeaTunnelRow, InMemorySourceSplit> createReader(SourceReader.Context context) {
        return new InMemorySourceReader(context, testData);
    }

    @Override
    public SourceSplitEnumerator<InMemorySourceSplit, InMemoryState> createEnumerator(
            SourceSplitEnumerator.Context<InMemorySourceSplit> context) {
        return new InMemorySourceSplitEnumerator(context, testData.size());
    }
}
```

### 6.2 InMemorySink

**路径**: `e2e/sink/inmemory/InMemorySink.java`

**用途**: 验证输出数据

```java
public class InMemorySink implements SeaTunnelSink<SeaTunnelRow, InMemoryState, InMemoryCommitInfo, InMemoryAggregatedCommitInfo> {

    // 静态存储，用于验证写入的数据
    private static final List<SeaTunnelRow> COLLECTED_DATA = new CopyOnWriteArrayList<>();

    public static List<SeaTunnelRow> getCollectedData() {
        return Collections.unmodifiableList(COLLECTED_DATA);
    }

    public static void clearCollectedData() {
        COLLECTED_DATA.clear();
    }

    @Override
    public SinkWriter<SeaTunnelRow, InMemoryCommitInfo, InMemoryState> createWriter(
            SinkWriter.Context context) {
        return new InMemorySinkWriter(COLLECTED_DATA);
    }
}
```

---

## 七、运行 E2E 测试

### 7.1 Maven 命令

```bash
# 运行所有 E2E 测试
./mvnw verify -DskipUT=true -DskipIT=false

# 运行特定连接器的 E2E 测试
./mvnw verify -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-jdbc-e2e \
    -DskipUT=true -DskipIT=false

# 运行单个测试类
./mvnw verify -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-fake-e2e \
    -DskipUT=true -DskipIT=false \
    -Dtest=FakeIT
```

### 7.2 环境要求

| 依赖 | 版本要求 | 说明 |
|------|----------|------|
| Docker | 19.03+ | 必需，TestContainers 依赖 |
| JDK | 8 或 11 | 编译和运行 |
| Memory | 8GB+ | 推荐，容器需要足够内存 |

### 7.3 常见问题

**1. Docker 权限问题**
```bash
# 添加当前用户到 docker 组
sudo usermod -aG docker $USER
```

**2. 容器启动超时**
```java
// 增加等待时间
.waitingFor(Wait.forLogMessage(".*started.*", 1)
    .withStartupTimeout(Duration.ofMinutes(5)));
```

**3. 端口冲突**
```java
// 使用随机端口
.withExposedPorts(5801)  // 不用 setPortBindings
```

---

## 八、E2E 测试模块列表

### 8.1 连接器 E2E 测试 (65 个)

| 类别 | 模块 |
|------|------|
| **数据库** | connector-jdbc-e2e, connector-clickhouse-e2e, connector-doris-e2e, connector-starrocks-e2e, connector-mongodb-e2e, connector-elasticsearch-e2e, connector-redis-e2e |
| **消息队列** | connector-kafka-e2e, connector-pulsar-e2e, connector-rabbitmq-e2e, connector-rocketmq-e2e, connector-activemq-e2e |
| **CDC** | connector-cdc-mysql-e2e, connector-cdc-postgres-e2e, connector-cdc-oracle-e2e, connector-cdc-sqlserver-e2e, connector-cdc-mongodb-e2e |
| **文件系统** | connector-file-local-e2e, connector-file-s3-e2e, connector-file-hdfs-e2e, connector-file-oss-e2e, connector-file-ftp-e2e |
| **数据湖** | connector-hive-e2e, connector-iceberg-e2e, connector-hudi-e2e, connector-paimon-e2e |
| **其他** | connector-fake-e2e, connector-assert-e2e, connector-http-e2e, connector-email-e2e |

### 8.2 引擎 E2E 测试 (4 个)

| 模块 | 说明 |
|------|------|
| connector-seatunnel-e2e-base | 基础引擎测试 |
| connector-console-seatunnel-e2e | Console 连接器测试 |
| seatunnel-engine-k8s-e2e | Kubernetes 部署测试 |

### 8.3 转换器 E2E 测试 (7 个)

| 模块 | 说明 |
|------|------|
| seatunnel-transforms-v2-e2e-common | 公共测试 |
| seatunnel-transforms-v2-e2e-part-1 | 转换器测试 Part 1 |
| seatunnel-transforms-v2-e2e-part-2 | 转换器测试 Part 2 |
| seatunnel-transforms-v2-e2e-udf | UDF 测试 |

---

## 九、设计模式总结

### 9.1 使用的设计模式

| 模式 | 应用场景 |
|------|----------|
| **模板方法模式** | AbstractTestContainer 定义测试流程，子类实现具体容器 |
| **工厂模式** | TestContainersFactory 创建容器实例 |
| **策略模式** | 不同容器的执行策略（Zeta/Flink/Spark） |
| **SPI 机制** | @AutoService 自动注册容器实现 |
| **装饰器模式** | JUnit 5 扩展层层装饰测试行为 |

### 9.2 测试设计最佳实践

1. **共享网络**: 所有容器使用同一 Docker 网络
2. **资源复用**: `@TestInstance(PER_CLASS)` 在类级别复用容器
3. **线程检测**: 自动检测线程泄漏
4. **日志增强**: 自动记录测试时间和容器日志
5. **多引擎测试**: `@TestTemplate` 参数化测试多个引擎

---

## 十、总结

`seatunnel-e2e` 模块是 SeaTunnel 质量保障的核心：

1. **框架完善**: 基于 TestContainers + JUnit 5 构建的完整测试框架
2. **覆盖全面**: 65+ 连接器、多引擎、多转换器的 E2E 测试
3. **高级功能**: 线程泄漏检测、ClassLoader 检查、多容器参数化
4. **易于扩展**: 模板方法 + SPI 机制，添加新容器/测试简单

---

**相关文档**:
- [006-seatunnel-connectors-v2模块源码分析.md](./006-seatunnel-connectors-v2模块源码分析.md) - 连接器实现
- [007-seatunnel-transforms-v2模块源码分析.md](./007-seatunnel-transforms-v2模块源码分析.md) - 转换器实现
- [004-seatunnel-engine模块源码分析.md](./004-seatunnel-engine模块源码分析.md) - Zeta 引擎实现

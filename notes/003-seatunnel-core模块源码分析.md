# SeaTunnel Core 模块源码分析

> 文档编号: 003
> 模块路径: seatunnel-core/
> 更新日期: 2025-12-29

---

## 一、模块概述

`seatunnel-core` 是 SeaTunnel 的核心启动器模块，负责：
- 命令行参数解析
- 配置文件加载与解析
- 作业提交与执行
- 多引擎适配（Zeta Engine、Flink、Spark）

### 1.1 子模块结构

```
seatunnel-core/
├── seatunnel-core-starter/     # 核心启动器基础类
├── seatunnel-starter/          # Zeta Engine 启动器
├── seatunnel-flink-starter/    # Flink 引擎启动器
│   ├── seatunnel-flink-starter-common/
│   ├── seatunnel-flink-13-starter/
│   ├── seatunnel-flink-15-starter/
│   └── seatunnel-flink-20-starter/
└── seatunnel-spark-starter/    # Spark 引擎启动器
    ├── seatunnel-spark-starter-common/
    ├── seatunnel-spark-2-starter/
    └── seatunnel-spark-3-starter/
```

---

## 二、seatunnel-core-starter 核心基础模块

### 2.1 包结构

```
org.apache.seatunnel.core.starter/
├── SeaTunnel.java              # 统一入口类
├── Starter.java                # 启动器接口
├── command/                    # 命令模式实现
│   ├── Command.java            # 命令接口
│   ├── CommandArgs.java        # 命令参数基类
│   ├── AbstractCommandArgs.java # 抽象命令参数
│   ├── ConfEncryptCommand.java # 配置加密命令
│   └── ConfDecryptCommand.java # 配置解密命令
├── execution/                  # 执行器接口
│   ├── RuntimeEnvironment.java # 运行时环境
│   ├── PluginExecuteProcessor.java # 插件执行处理器
│   ├── TaskExecution.java      # 任务执行接口
│   └── SourceTableInfo.java    # 源表信息
├── flowcontrol/                # 流控组件
│   ├── FlowControlGate.java    # 流控门
│   └── FlowControlStrategy.java # 流控策略
├── utils/                      # 工具类
│   ├── ConfigBuilder.java      # 配置构建器
│   ├── ConfigShadeUtils.java   # 配置脱敏工具
│   ├── CommandLineUtils.java   # 命令行工具
│   └── FileUtils.java          # 文件工具
└── exception/                  # 异常定义
    ├── CommandException.java
    ├── CommandExecuteException.java
    └── ConfigCheckException.java
```

### 2.2 核心类分析

#### 2.2.1 SeaTunnel - 统一入口

```java
// 文件: SeaTunnel.java
// 职责: 作为所有引擎的统一执行入口

@Slf4j
public class SeaTunnel {
    /**
     * SeaTunnel 的入口方法
     * 采用命令模式，接收不同的 Command 实现
     */
    public static <T extends CommandArgs> void run(Command<T> command) throws CommandException {
        try {
            command.execute();  // 执行具体命令
        } catch (ConfigRuntimeException e) {
            showConfigError(e);  // 配置错误处理
            throw e;
        } catch (Exception e) {
            showFatalError(e);   // 致命错误处理
            throw e;
        }
    }
}
```

**设计模式**: 命令模式（Command Pattern）
- 将请求封装为 `Command` 对象
- 支持不同引擎的命令实现
- 统一的异常处理机制

#### 2.2.2 Command 接口

```java
// 文件: command/Command.java
// 职责: 定义命令执行的标准接口

@FunctionalInterface
public interface Command<T extends CommandArgs> {
    void execute() throws CommandExecuteException, ConfigCheckException;
}
```

**实现类**:
| 命令类 | 功能 |
|--------|------|
| `ClientExecuteCommand` | 客户端执行作业 |
| `ServerExecuteCommand` | 服务端执行（集群模式）|
| `ConfEncryptCommand` | 配置加密 |
| `ConfDecryptCommand` | 配置解密 |
| `SeaTunnelConfValidateCommand` | 配置验证 |

#### 2.2.3 AbstractCommandArgs - 命令参数

```java
// 文件: command/AbstractCommandArgs.java
// 职责: 定义通用命令行参数

@Data
public abstract class AbstractCommandArgs extends CommandArgs {
    // 配置文件路径
    @Parameter(names = {"-c", "--config"}, description = "Config file")
    protected String configFile;

    // 变量替换
    @Parameter(names = {"-i", "--variable"},
               description = "Variable substitution, e.g., -i city=beijing")
    protected List<String> variables;

    // 配置检查
    @Parameter(names = {"--check"}, description = "Whether check config")
    protected boolean checkConfig = false;

    // 作业名称
    @Parameter(names = {"-n", "--name"}, description = "SeaTunnel job name")
    protected String jobName;

    // 配置加密/解密
    @Parameter(names = {"--encrypt"}, description = "Encrypt config file")
    protected boolean encrypt = false;

    @Parameter(names = {"--decrypt"}, description = "Decrypt config file")
    protected boolean decrypt = false;

    public abstract DeployMode getDeployMode();
}
```

**使用的框架**: JCommander（命令行参数解析）

#### 2.2.4 ConfigBuilder - 配置构建器

```java
// 文件: utils/ConfigBuilder.java
// 职责: 加载、解析、处理配置文件

@Slf4j
public class ConfigBuilder {

    /**
     * 从文件路径加载配置
     * 支持 HOCON 格式和自定义适配器
     */
    public static Config of(@NonNull Path filePath, List<String> variables) {
        log.info("Loading config file from path: {}", filePath);

        // 1. 尝试使用 SPI 适配器
        Optional<ConfigAdapter> adapterSupplier = ConfigAdapterUtils.selectAdapter(filePath);

        // 2. 解析配置
        Config config = adapterSupplier
                .map(adapter -> of(adapter, filePath, variables))
                .orElseGet(() -> ofInner(filePath, variables));

        // 3. 日志输出（脱敏处理）
        log.info("Parsed config file: \n{}",
                mapToString(configDesensitization(config.root().unwrapped(),
                            ConfigShadeUtils.getSensitiveOptions(config))));
        return config;
    }

    /**
     * 变量替换处理
     * 支持 ${variable} 和 ${variable:default} 格式
     */
    private static Config backfillUserVariables(Config config, List<String> variables) {
        // 将变量设置为系统属性
        variables.stream()
                .filter(Objects::nonNull)
                .map(variable -> variable.split("=", 2))
                .filter(pair -> pair.length == 2)
                .forEach(pair -> System.setProperty(pair[0], pair[1]));

        // 解析配置中的占位符
        return config.resolveWith(systemConfig,
                ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }

    /**
     * 敏感信息脱敏
     * 对密码等敏感字段进行掩码处理
     */
    public static Map<String, Object> configDesensitization(
            Map<String, Object> configMap, Set<String> sensitiveKeywords) {
        // 递归处理，将敏感字段值替换为 "******"
    }
}
```

**核心功能**:
1. HOCON 配置文件解析
2. 变量替换（`-i key=value`）
3. 配置加密/解密
4. 敏感信息脱敏日志

#### 2.2.5 FlowControlGate - 流量控制

```java
// 文件: flowcontrol/FlowControlGate.java
// 职责: 实现读取限流

public class FlowControlGate {
    // 字节速率限制器
    private final Optional<RateLimiter> bytesRateLimiter;
    // 行数速率限制器
    private final Optional<RateLimiter> countRateLimiter;

    /**
     * 对每一行数据进行流控审计
     */
    public void audit(SeaTunnelRow row) {
        // 字节限流
        bytesRateLimiter.ifPresent(rateLimiter ->
            rateLimiter.acquire(row.getBytesSize()));
        // 行数限流
        countRateLimiter.ifPresent(RateLimiter::acquire);
    }
}
```

**使用的库**: Google Guava RateLimiter

**配置方式**:
```hocon
env {
  read_limit.rows_per_second = 10000
  read_limit.bytes_per_second = 10485760
}
```

#### 2.2.6 PluginExecuteProcessor - 插件执行处理器

```java
// 文件: execution/PluginExecuteProcessor.java
// 职责: 定义 Source/Transform/Sink 执行接口

public interface PluginExecuteProcessor<T, ENV extends RuntimeEnvironment> {
    /**
     * 执行处理逻辑
     * @param upstreamDataStreams 上游数据流
     * @return 处理后的数据流
     */
    List<T> execute(List<T> upstreamDataStreams) throws TaskExecuteException;

    /**
     * 设置运行时环境
     */
    void setRuntimeEnvironment(ENV runtimeEnvironment);
}
```

**泛型说明**:
- `T`: 数据流类型（如 Flink 的 DataStream、Spark 的 Dataset）
- `ENV`: 运行时环境类型

---

## 三、seatunnel-starter - Zeta Engine 启动器

### 3.1 类结构

```
org.apache.seatunnel.core.starter.seatunnel/
├── SeaTunnelClient.java        # 客户端入口（提交作业）
├── SeaTunnelServer.java        # 服务端入口（启动集群）
├── SeaTunnelConnector.java     # 连接器管理
├── args/
│   ├── ClientCommandArgs.java  # 客户端参数
│   ├── ServerCommandArgs.java  # 服务端参数
│   └── ConnectorCheckCommandArgs.java
└── command/
    ├── ClientExecuteCommand.java    # 作业执行命令
    ├── ServerExecuteCommand.java    # 服务启动命令
    ├── SeaTunnelConfValidateCommand.java
    └── ConnectorCheckCommand.java
```

### 3.2 SeaTunnelClient - 客户端入口

```java
// 文件: SeaTunnelClient.java
// 职责: 作业提交的 main 入口

@Slf4j
public class SeaTunnelClient {
    public static void main(String[] args) throws CommandException {
        // 1. 解析命令行参数
        ClientCommandArgs clientCommandArgs = CommandLineUtils.parse(
                args,
                new ClientCommandArgs(),
                EngineType.SEATUNNEL.getStarterShellName(),
                true);

        // 2. 执行命令
        try {
            SeaTunnel.run(clientCommandArgs.buildCommand());
        } catch (Error e) {
            log.error("Exception StackTrace: {}", ExceptionUtils.getStackTrace(e));
            System.exit(1);
        }
    }
}
```

### 3.3 ClientCommandArgs - 客户端参数

```java
// 文件: args/ClientCommandArgs.java
// 职责: 定义客户端命令行参数

@Data
public class ClientCommandArgs extends AbstractCommandArgs {

    // 运行模式: local 或 cluster
    @Parameter(names = {"-m", "--master", "-e", "--deploy-mode"},
               description = "SeaTunnel job submit master, support [local, cluster]")
    private MasterType masterType = MasterType.CLUSTER;

    // 从 savepoint 恢复
    @Parameter(names = {"-r", "--restore"}, description = "restore with savepoint by jobId")
    private String restoreJobId;

    // 创建 savepoint
    @Parameter(names = {"-s", "--savepoint"}, description = "savepoint job by jobId")
    private String savePointJobId;

    // 集群名称
    @Parameter(names = {"-cn", "--cluster"}, description = "The name of cluster")
    private String clusterName;

    // 查询作业状态
    @Parameter(names = {"-j", "--job-id"}, description = "Get job status by JobId")
    private String jobId;

    // 取消作业
    @Parameter(names = {"-can", "--cancel-job"}, description = "Cancel job by JobId")
    private List<String> cancelJobId;

    // 异步模式
    @Parameter(names = {"--async"}, description = "Run the job asynchronously")
    private boolean async = false;

    // 列出所有作业
    @Parameter(names = {"-l", "--list"}, description = "list job status")
    private boolean listJob = false;

    /**
     * 根据参数构建对应的命令
     */
    @Override
    public Command<?> buildCommand() {
        if (checkConfig) {
            return new SeaTunnelConfValidateCommand(this);
        }
        if (encrypt) {
            return new ConfEncryptCommand(this);
        }
        if (decrypt) {
            return new ConfDecryptCommand(this);
        }
        return new ClientExecuteCommand(this);
    }
}
```

**命令行示例**:
```bash
# 本地模式运行
./bin/seatunnel.sh --config job.conf -e local

# 集群模式运行
./bin/seatunnel.sh --config job.conf

# 异步提交
./bin/seatunnel.sh --config job.conf --async

# 查看作业状态
./bin/seatunnel.sh -j 12345678

# 取消作业
./bin/seatunnel.sh -can 12345678
```

### 3.4 ClientExecuteCommand - 作业执行命令

```java
// 文件: command/ClientExecuteCommand.java
// 职责: 核心作业执行逻辑

@Slf4j
public class ClientExecuteCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;
    private SeaTunnelClient engineClient;
    private HazelcastInstance instance;

    @Override
    public void execute() throws CommandExecuteException {
        LocalDateTime startTime = LocalDateTime.now();
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();

        try {
            // 1. 获取运行模式
            boolean isLocalMode = clientCommandArgs.getMasterType().equals(MasterType.LOCAL);

            // 2. 本地模式：创建本地 Hazelcast 实例
            if (isLocalMode) {
                clusterName = creatRandomClusterName(clusterName);
                instance = createServerInLocal(clusterName, seaTunnelConfig);
            }

            // 3. 创建引擎客户端
            engineClient = new SeaTunnelClient(clientConfig);

            // 4. 根据参数执行不同操作
            if (clientCommandArgs.isListJob()) {
                // 列出作业
                System.out.println(engineClient.getJobClient().listJobStatus(true));
            } else if (clientCommandArgs.getJobId() != null) {
                // 查询作业状态
                System.out.println(engineClient.getJobClient().getJobDetailStatus(...));
            } else if (clientCommandArgs.getCancelJobId() != null) {
                // 取消作业
                for (String cancelJobId : clientCommandArgs.getCancelJobId()) {
                    engineClient.getJobClient().cancelJob(Long.parseLong(cancelJobId));
                }
            } else {
                // 5. 提交新作业
                Path configFile = FileUtils.getConfigPath(clientCommandArgs);
                checkConfigExist(configFile);

                // 创建执行环境
                ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                        configFile.toString(),
                        clientCommandArgs.getVariables(),
                        jobConfig,
                        seaTunnelConfig,
                        customJobId);

                // 执行作业
                ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

                // 6. 注册关闭钩子
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    shutdownHook(clientJobProxy);
                }));

                // 7. 等待作业完成
                JobResult jobResult = clientJobProxy.waitForJobCompleteV2();
            }
        } finally {
            // 8. 清理资源
            closeClient();
        }
    }

    /**
     * 本地模式创建服务
     */
    private HazelcastInstance createServerInLocal(String clusterName, SeaTunnelConfig config) {
        config.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        config.getEngineConfig().setMode(ExecutionMode.LOCAL);

        return HazelcastInstanceFactory.newHazelcastInstance(
                config.getHazelcastConfig(),
                Thread.currentThread().getName(),
                new SeaTunnelNodeContext(config));
    }
}
```

**执行流程图**:
```
┌─────────────────────────────────────────────────────────────┐
│                    ClientExecuteCommand                      │
├─────────────────────────────────────────────────────────────┤
│  1. 加载 SeaTunnelConfig                                    │
│     ↓                                                        │
│  2. 判断运行模式 (LOCAL/CLUSTER)                            │
│     ├── LOCAL: 创建本地 Hazelcast 实例                      │
│     └── CLUSTER: 连接远程集群                               │
│     ↓                                                        │
│  3. 创建 SeaTunnelClient                                    │
│     ↓                                                        │
│  4. 根据参数执行操作                                        │
│     ├── listJob: 列出作业                                   │
│     ├── jobId: 查询状态                                     │
│     ├── cancelJobId: 取消作业                               │
│     └── configFile: 提交新作业                              │
│         ├── 加载配置文件                                    │
│         ├── 创建 JobExecutionEnvironment                    │
│         ├── 执行作业 (execute)                              │
│         ├── 注册 ShutdownHook                               │
│         └── 等待作业完成                                    │
│     ↓                                                        │
│  5. 输出作业统计信息                                        │
│     ↓                                                        │
│  6. 关闭客户端                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 四、seatunnel-flink-starter - Flink 引擎启动器

### 4.1 模块结构

```
seatunnel-flink-starter/
├── seatunnel-flink-starter-common/  # 公共代码
│   ├── SeaTunnelFlink.java          # Flink 入口基类
│   ├── FlinkStarter.java            # Flink 启动器
│   ├── AbstractFlinkStarter.java    # 抽象启动器
│   └── execution/
│       ├── FlinkRuntimeEnvironment.java
│       ├── SourceExecuteProcessor.java
│       ├── TransformExecuteProcessor.java
│       └── SinkExecuteProcessor.java
├── seatunnel-flink-13-starter/      # Flink 1.13 适配
├── seatunnel-flink-15-starter/      # Flink 1.15 适配
└── seatunnel-flink-20-starter/      # Flink 2.0 适配
```

### 4.2 SeaTunnelFlink - Flink 入口

```java
// 文件: SeaTunnelFlink.java (flink-starter-common)
// 职责: Flink 作业入口

public class SeaTunnelFlink extends AbstractSeaTunnelFlink {
    public static void main(String[] args) throws CommandException {
        runSeaTunnel(args, EngineType.FLINK15);
    }
}
```

**版本适配**:
- `seatunnel-flink-13-starter`: Flink 1.13.x
- `seatunnel-flink-15-starter`: Flink 1.15.x
- `seatunnel-flink-20-starter`: Flink 2.0.x

### 4.3 FlinkRuntimeEnvironment

```java
// 文件: execution/FlinkRuntimeEnvironment.java
// 职责: 管理 Flink 执行环境

public class FlinkRuntimeEnvironment implements RuntimeEnvironment {
    private StreamExecutionEnvironment environment;
    private StreamTableEnvironment tableEnvironment;

    // 初始化 Flink 环境
    public void prepare() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnvironment = StreamTableEnvironment.create(environment);

        // 设置并行度
        environment.setParallelism(config.getParallelism());

        // 设置检查点
        if (config.isCheckpointEnabled()) {
            environment.enableCheckpointing(config.getCheckpointInterval());
        }
    }
}
```

---

## 五、seatunnel-spark-starter - Spark 引擎启动器

### 5.1 模块结构

```
seatunnel-spark-starter/
├── seatunnel-spark-starter-common/  # 公共代码
│   ├── SeaTunnelSpark.java          # Spark 入口
│   ├── SparkStarter.java            # Spark 启动器
│   ├── args/SparkCommandArgs.java   # Spark 参数
│   ├── command/
│   │   ├── SparkTaskExecuteCommand.java
│   │   └── SparkConfValidateCommand.java
│   └── execution/
│       ├── SparkRuntimeEnvironment.java
│       ├── SourceExecuteProcessor.java
│       ├── TransformExecuteProcessor.java
│       └── SinkExecuteProcessor.java
├── seatunnel-spark-2-starter/       # Spark 2.x 适配
└── seatunnel-spark-3-starter/       # Spark 3.x 适配
```

### 5.2 SeaTunnelSpark - Spark 入口

```java
// 文件: SeaTunnelSpark.java
// 职责: Spark 作业入口

public class SeaTunnelSpark {
    public static void main(String[] args) throws CommandException {
        // 解析命令行参数
        SparkCommandArgs sparkCommandArgs = CommandLineUtils.parse(
                args,
                new SparkCommandArgs(),
                EngineType.SPARK3.getStarterShellName(),
                true);

        // 执行作业
        SeaTunnel.run(sparkCommandArgs.buildCommand());
    }
}
```

### 5.3 SparkRuntimeEnvironment

```java
// 文件: execution/SparkRuntimeEnvironment.java
// 职责: 管理 Spark 执行环境

public class SparkRuntimeEnvironment implements RuntimeEnvironment {
    private SparkSession sparkSession;

    public void prepare() {
        SparkConf sparkConf = new SparkConf();
        // 从配置加载 Spark 参数
        config.getSparkConfig().forEach(sparkConf::set);

        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }
}
```

---

## 六、关键设计模式

### 6.1 命令模式 (Command Pattern)

```
          ┌──────────────────┐
          │   SeaTunnel      │ (Invoker)
          │   run(command)   │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │   Command<T>     │ (Interface)
          │   execute()      │
          └────────┬─────────┘
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
┌───────────────┐    ┌───────────────┐
│ ClientExecute │    │ ServerExecute │
│   Command     │    │    Command    │
└───────────────┘    └───────────────┘
```

### 6.2 模板方法模式 (Template Method)

```java
// AbstractCommandArgs 定义模板
public abstract class AbstractCommandArgs {
    // 通用参数定义
    protected String configFile;
    protected List<String> variables;

    // 模板方法 - 子类实现
    public abstract DeployMode getDeployMode();
    public abstract Command<?> buildCommand();
}

// 具体实现
public class ClientCommandArgs extends AbstractCommandArgs {
    @Override
    public DeployMode getDeployMode() {
        return DeployMode.CLIENT;
    }

    @Override
    public Command<?> buildCommand() {
        return new ClientExecuteCommand(this);
    }
}
```

### 6.3 策略模式 (Strategy Pattern)

```
RuntimeEnvironment (Strategy Interface)
        ├── FlinkRuntimeEnvironment
        ├── SparkRuntimeEnvironment
        └── (Zeta uses Hazelcast directly)

PluginExecuteProcessor (Strategy Interface)
        ├── Flink: FlinkAbstractPluginExecuteProcessor
        ├── Spark: SparkAbstractPluginExecuteProcessor
        └── Zeta: (handled by engine)
```

---

## 七、类图总览

```
┌─────────────────────────────────────────────────────────────────────┐
│                        seatunnel-core-starter                        │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐       ┌─────────────────────┐                      │
│  │  SeaTunnel  │──────>│  Command<Args>      │                      │
│  │  run()      │       │  execute()          │                      │
│  └─────────────┘       └─────────────────────┘                      │
│                                  △                                   │
│                    ┌─────────────┴─────────────┐                    │
│                    │                           │                    │
│  ┌─────────────────────────┐   ┌─────────────────────────┐          │
│  │ ConfEncryptCommand      │   │ ConfDecryptCommand      │          │
│  └─────────────────────────┘   └─────────────────────────┘          │
│                                                                      │
│  ┌──────────────────────┐                                           │
│  │ AbstractCommandArgs  │<──────────┐                               │
│  │ - configFile         │           │                               │
│  │ - variables          │           │                               │
│  │ - jobName            │           │                               │
│  └──────────────────────┘           │                               │
│            △                        │                               │
└────────────┼────────────────────────┼───────────────────────────────┘
             │                        │
┌────────────┼────────────────────────┼───────────────────────────────┐
│            │    seatunnel-starter   │                               │
├────────────┼────────────────────────┼───────────────────────────────┤
│  ┌─────────────────────┐   ┌─────────────────────────────┐          │
│  │ ClientCommandArgs   │   │ ClientExecuteCommand        │          │
│  │ - masterType        │──>│ - createServerInLocal()     │          │
│  │ - restoreJobId      │   │ - engineClient              │          │
│  │ - async             │   │ - execute()                 │          │
│  └─────────────────────┘   └─────────────────────────────┘          │
│                                                                      │
│  ┌─────────────────────┐   ┌─────────────────────────────┐          │
│  │ ServerCommandArgs   │──>│ ServerExecuteCommand        │          │
│  └─────────────────────┘   └─────────────────────────────┘          │
│                                                                      │
│  ┌──────────────────┐      ┌──────────────────┐                     │
│  │ SeaTunnelClient  │      │ SeaTunnelServer  │                     │
│  │ main()           │      │ main()           │                     │
│  └──────────────────┘      └──────────────────┘                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 八、配置加载流程

```
配置文件 (.conf)
      │
      ▼
┌─────────────────┐
│  ConfigBuilder  │
│  of(filePath)   │
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐  ┌────────────┐
│ HOCON  │  │ ConfigAdapter│ (SPI)
│ Parser │  │ (SQL, YAML) │
└────┬───┘  └──────┬─────┘
     │             │
     └──────┬──────┘
            ▼
    ┌───────────────┐
    │ 变量替换       │
    │ ${var:default}│
    └───────┬───────┘
            ▼
    ┌───────────────┐
    │ 配置解密       │
    │ ConfigShade   │
    └───────┬───────┘
            ▼
    ┌───────────────┐
    │ 敏感信息脱敏   │
    │ (日志输出)    │
    └───────┬───────┘
            ▼
      Config 对象
```

---

## 九、总结

### 9.1 模块职责

| 模块 | 职责 |
|------|------|
| `seatunnel-core-starter` | 公共基础类、命令模式、配置解析 |
| `seatunnel-starter` | Zeta Engine 入口和作业管理 |
| `seatunnel-flink-starter` | Flink 引擎适配 |
| `seatunnel-spark-starter` | Spark 引擎适配 |

### 9.2 核心类

| 类 | 功能 |
|----|------|
| `SeaTunnel` | 统一执行入口 |
| `Command` | 命令接口 |
| `AbstractCommandArgs` | 命令参数基类 |
| `ConfigBuilder` | 配置加载与解析 |
| `ClientExecuteCommand` | 作业执行核心逻辑 |
| `FlowControlGate` | 流量控制 |

### 9.3 扩展点

1. **新增命令**: 实现 `Command` 接口
2. **新增配置格式**: 实现 `ConfigAdapter` SPI
3. **新增引擎适配**: 扩展 starter 模块

---

*文档结束*

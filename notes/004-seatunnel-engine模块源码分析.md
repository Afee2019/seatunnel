# SeaTunnel Engine 模块源码分析

> 文档编号: 004
> 模块路径: seatunnel-engine/
> 更新日期: 2025-12-29

---

## 一、模块概述

`seatunnel-engine` 是 SeaTunnel 的原生计算引擎（Zeta Engine），基于 Hazelcast 分布式框架构建，专为数据同步场景优化。

### 1.1 子模块结构

```
seatunnel-engine/
├── seatunnel-engine-common/     # 公共模块：配置、异常、常量
├── seatunnel-engine-core/       # 核心模块：DAG、解析、协议
├── seatunnel-engine-client/     # 客户端：作业提交、状态查询
├── seatunnel-engine-server/     # 服务端：调度、执行、检查点
├── seatunnel-engine-storage/    # 存储：检查点持久化
├── seatunnel-engine-serializer/ # 序列化：Protobuf 实现
└── seatunnel-engine-ui/         # Web UI
```

### 1.2 技术栈

| 组件 | 技术 |
|------|------|
| 分布式通信 | Hazelcast 5.x |
| 高性能队列 | LMAX Disruptor |
| 序列化 | Protobuf |
| REST API | Jetty |
| 系统监控 | OSHI |

---

## 二、engine-common 公共模块

### 2.1 包结构

```
org.apache.seatunnel.engine.common/
├── config/                    # 配置类
│   ├── SeaTunnelConfig.java   # 引擎主配置
│   ├── EngineConfig.java      # 引擎参数配置
│   ├── JobConfig.java         # 作业配置
│   ├── ConfigProvider.java    # 配置加载器
│   └── server/                # 服务端配置项
│       ├── CheckpointConfig.java
│       ├── SlotServiceConfig.java
│       ├── HttpConfig.java
│       └── TelemetryConfig.java
├── job/                       # 作业定义
│   ├── JobStatus.java         # 作业状态枚举
│   └── JobResult.java         # 作业结果
├── runtime/                   # 运行时
│   ├── ExecutionMode.java     # 执行模式
│   └── DeployType.java        # 部署类型
├── exception/                 # 异常定义
└── utils/                     # 工具类
```

### 2.2 SeaTunnelConfig - 引擎主配置

```java
// 文件: config/SeaTunnelConfig.java
// 职责: 封装引擎配置和 Hazelcast 配置

public class SeaTunnelConfig {
    // 引擎配置
    private final EngineConfig engineConfig = new EngineConfig();
    // Hazelcast 配置
    private Config hazelcastConfig;

    public SeaTunnelConfig() {
        hazelcastConfig = new Config();
        // 设置默认多播端口
        hazelcastConfig.getNetworkConfig()
                .getJoin()
                .getMulticastConfig()
                .setMulticastPort(Constant.DEFAULT_SEATUNNEL_MULTICAST_PORT);
        // 设置热重启持久化目录
        hazelcastConfig.getHotRestartPersistenceConfig()
                .setBaseDir(new File(seatunnelHome(), "recovery"));
    }
}
```

### 2.3 EngineConfig - 引擎参数

```java
// 文件: config/EngineConfig.java
// 核心配置项（对应 seatunnel.yaml）

public class EngineConfig {
    // 集群角色
    public enum ClusterRole { MASTER, WORKER, MASTER_AND_WORKER }

    private ClusterRole clusterRole = ClusterRole.MASTER_AND_WORKER;

    // 类加载器缓存模式
    private boolean classloaderCacheMode = true;

    // 历史作业保留时间
    private int historyJobExpireMinutes = 1440;

    // 备份数量
    private int backupCount = 1;

    // 检查点配置
    private CheckpointConfig checkpointConfig;

    // Slot 服务配置
    private SlotServiceConfig slotServiceConfig;

    // HTTP 配置
    private HttpConfig httpConfig;

    // 遥测配置
    private TelemetryConfig telemetryConfig;
}
```

### 2.4 JobStatus - 作业状态

```java
// 文件: job/JobStatus.java
// 职责: 定义作业生命周期状态

public enum JobStatus {
    INITIALIZING(EndState.NOT_END),   // 初始化中
    CREATED(EndState.NOT_END),        // 已创建
    PENDING(EndState.NOT_END),        // 等待资源
    SCHEDULED(EndState.NOT_END),      // 已调度
    RUNNING(EndState.NOT_END),        // 运行中
    FAILING(EndState.NOT_END),        // 失败中
    FAILED(EndState.GLOBALLY),        // 已失败
    DOING_SAVEPOINT(EndState.NOT_END),// Savepoint 中
    SAVEPOINT_DONE(EndState.GLOBALLY),// Savepoint 完成
    CANCELING(EndState.NOT_END),      // 取消中
    CANCELED(EndState.GLOBALLY),      // 已取消
    FINISHED(EndState.GLOBALLY),      // 已完成
    UNKNOWABLE(EndState.GLOBALLY);    // 未知

    public boolean isEndState() {
        return endState != EndState.NOT_END;
    }
}
```

**状态流转图**:
```
INITIALIZING → CREATED → PENDING → SCHEDULED → RUNNING
                                                  ↓
                                       ┌─────────┴─────────┐
                                       ↓                   ↓
                                   FAILING            CANCELING
                                       ↓                   ↓
                                   FAILED              CANCELED

                                   RUNNING → FINISHED
                                       ↓
                               DOING_SAVEPOINT → SAVEPOINT_DONE
```

---

## 三、engine-core 核心模块

### 3.1 包结构

```
org.apache.seatunnel.engine.core/
├── dag/                       # DAG 图结构
│   ├── actions/               # Action 定义
│   │   ├── Action.java        # Action 接口
│   │   ├── SourceAction.java  # 数据源 Action
│   │   ├── SinkAction.java    # 数据目标 Action
│   │   └── TransformAction.java
│   ├── logical/               # 逻辑图
│   │   ├── LogicalDag.java    # 逻辑 DAG
│   │   ├── LogicalVertex.java # 逻辑顶点
│   │   └── LogicalEdge.java   # 逻辑边
│   └── internal/              # 内部结构
├── checkpoint/                # 检查点
│   ├── Checkpoint.java
│   └── CheckpointType.java
├── parse/                     # 配置解析
│   ├── MultipleTableJobConfigParser.java
│   └── JobConfigParser.java
├── job/                       # 作业定义
│   ├── JobImmutableInformation.java
│   └── AbstractJobEnvironment.java
└── protocol/                  # 通信协议
    └── codec/                 # 编解码器
```

### 3.2 Action 接口

```java
// 文件: dag/actions/Action.java
// 职责: 定义执行单元接口

public interface Action extends Serializable {
    @NonNull String getName();
    void setName(@NonNull String name);

    // 上游 Action 列表
    @NonNull List<Action> getUpstream();
    void addUpstream(@NonNull Action action);

    // 并行度
    int getParallelism();
    void setParallelism(int parallelism);

    // 唯一标识
    long getId();

    // JAR 包 URL
    Set<URL> getJarUrls();

    // 连接器 JAR 标识
    Set<ConnectorJarIdentifier> getConnectorJarIdentifiers();

    // 配置信息
    Config getConfig();
}
```

**Action 实现类**:

| 类 | 功能 |
|----|------|
| `SourceAction` | 数据源操作，封装 `SeaTunnelSource` |
| `SinkAction` | 数据写入操作，封装 `SeaTunnelSink` |
| `TransformAction` | 单个转换操作 |
| `TransformChainAction` | 转换链（多个转换合并） |

### 3.3 LogicalDag - 逻辑 DAG

```java
// 文件: dag/logical/LogicalDag.java
// 职责: 描述作业的逻辑执行计划

/**
 * LogicalDag 描述 SeaTunnel Engine 运行的逻辑计划
 * - LogicalVertex 定义操作符
 * - LogicalEdge 定义操作符之间的关系
 *
 * 三种基本顶点类型:
 * 1. SeaTunnelSource: 只有出边
 * 2. SeaTunnelTransform: 有入边和出边
 * 3. SeaTunnelSink: 只有入边
 */
@Slf4j
public class LogicalDag implements IdentifiedDataSerializable {
    @Getter private JobConfig jobConfig;

    // 边集合
    private final Set<LogicalEdge> edges = new LinkedHashSet<>();

    // 顶点映射 (vertexId -> LogicalVertex)
    private final LinkedHashMap<Long, LogicalVertex> logicalVertexMap = new LinkedHashMap<>();

    // ID 生成器
    private IdGenerator idGenerator;

    // 是否从 SavePoint 启动
    private boolean isStartWithSavePoint = false;

    public void addLogicalVertex(LogicalVertex logicalVertex) {
        logicalVertexMap.put(logicalVertex.getVertexId(), logicalVertex);
    }

    public void addEdge(LogicalEdge logicalEdge) {
        edges.add(logicalEdge);
    }

    // 转换为 JSON（用于展示）
    @NonNull public JsonObject getLogicalDagAsJson() { ... }
}
```

### 3.4 MultipleTableJobConfigParser - 配置解析器

```java
// 文件: parse/MultipleTableJobConfigParser.java
// 职责: 解析 HOCON 配置文件，生成 Action 列表

public class MultipleTableJobConfigParser {
    /**
     * 解析配置文件
     * @return Pair<Action列表, JAR URL集合>
     */
    public ImmutablePair<List<Action>, Set<URL>> parse(ClassLoader classLoader) {
        // 1. 解析 source 配置
        List<SourceAction> sourceActions = parseSource();

        // 2. 解析 transform 配置（可选）
        List<TransformAction> transformActions = parseTransform();

        // 3. 解析 sink 配置
        List<SinkAction> sinkActions = parseSink();

        // 4. 构建 Action 依赖关系
        buildActionDAG(sourceActions, transformActions, sinkActions);

        return new ImmutablePair<>(actions, jarUrls);
    }
}
```

### 3.5 Protocol Codec - 通信协议编解码

```java
// 示例: SeaTunnelSubmitJobCodec.java
// 职责: 作业提交请求的编解码

public final class SeaTunnelSubmitJobCodec {
    public static ClientMessage encodeRequest(
            long jobId,
            Data jobImmutableInformation,
            boolean isStartWithSavePoint) {
        // 编码请求消息
    }

    public static void decodeResponse(ClientMessage clientMessage) {
        // 解码响应消息
    }
}
```

**主要 Codec 类**:
- `SeaTunnelSubmitJobCodec`: 提交作业
- `SeaTunnelCancelJobCodec`: 取消作业
- `SeaTunnelGetJobStatusCodec`: 获取作业状态
- `SeaTunnelWaitForJobCompleteCodec`: 等待作业完成
- `SeaTunnelGetJobMetricsCodec`: 获取作业指标
- `SeaTunnelSavePointJobCodec`: 创建 SavePoint

---

## 四、engine-client 客户端模块

### 4.1 包结构

```
org.apache.seatunnel.engine.client/
├── SeaTunnelClient.java           # 客户端入口
├── SeaTunnelHazelcastClient.java  # Hazelcast 客户端封装
├── SeaTunnelClientInstance.java   # 客户端接口
└── job/
    ├── ClientJobExecutionEnvironment.java  # 作业执行环境
    ├── ClientJobProxy.java                  # 作业代理
    ├── JobClient.java                       # 作业客户端
    ├── JobMetricsRunner.java               # 指标收集
    └── JobStatusRunner.java                # 状态轮询
```

### 4.2 SeaTunnelClient - 客户端入口

```java
// 文件: SeaTunnelClient.java
// 职责: 提供作业提交、查询、管理的 API

public class SeaTunnelClient implements SeaTunnelClientInstance, AutoCloseable {
    private final SeaTunnelHazelcastClient hazelcastClient;
    @Getter private final JobClient jobClient;

    public SeaTunnelClient(@NonNull ClientConfig clientConfig) {
        this.hazelcastClient = new SeaTunnelHazelcastClient(clientConfig);
        this.jobClient = new JobClient(this.hazelcastClient);
    }

    /**
     * 创建作业执行环境
     */
    @Override
    public ClientJobExecutionEnvironment createExecutionContext(
            @NonNull String filePath,
            List<String> variables,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig,
            Long jobId) {
        return new ClientJobExecutionEnvironment(
                jobConfig, filePath, variables, hazelcastClient, seaTunnelConfig, jobId);
    }

    /**
     * 从 SavePoint 恢复作业
     */
    @Override
    public ClientJobExecutionEnvironment restoreExecutionContext(
            @NonNull String filePath,
            List<String> variables,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig,
            @NonNull Long jobId) {
        return new ClientJobExecutionEnvironment(
                jobConfig, filePath, variables, hazelcastClient, seaTunnelConfig, true, jobId);
    }

    /**
     * 获取作业指标摘要
     */
    public JobMetricsSummary getJobMetricsSummary(Long jobId) {
        return jobClient.getJobMetricsSummary(jobId);
    }

    /**
     * 获取集群健康指标
     */
    public Map<String, String> getClusterHealthMetrics() { ... }

    @Override
    public void close() {
        hazelcastClient.getHazelcastInstance().shutdown();
    }
}
```

### 4.3 ClientJobExecutionEnvironment - 作业执行环境

```java
// 文件: job/ClientJobExecutionEnvironment.java
// 职责: 封装作业执行的上下文，解析配置，生成 LogicalDag

public class ClientJobExecutionEnvironment extends AbstractJobEnvironment {
    private final String jobFilePath;
    private final List<String> variables;
    private final SeaTunnelHazelcastClient seaTunnelHazelcastClient;
    private final JobClient jobClient;
    private final SeaTunnelConfig seaTunnelConfig;
    private final ConnectorPackageClient connectorPackageClient;

    /**
     * 获取逻辑 DAG
     */
    @Override
    public LogicalDag getLogicalDag() {
        // 1. 解析配置文件，获取 Action 列表
        ImmutablePair<List<Action>, Set<URL>> immutablePair = getJobConfigParser().parse(null);
        actions.addAll(immutablePair.getLeft());

        // 2. 如果启用了 JAR 包上传，上传连接器 JAR 到服务端
        if (enableUploadConnectorJarPackage) {
            Set<ConnectorJarIdentifier> commonJarIdentifiers =
                    connectorPackageClient.uploadCommonPluginJars(jobId, commonPluginJars);
            // 上传 Action 相关的 JAR 包
            uploadActionPluginJar(actions, pluginJarIdentifiers);
        }

        // 3. 生成逻辑 DAG
        return getLogicalDagGenerator().generate();
    }

    /**
     * 执行作业
     */
    public ClientJobProxy execute() throws ExecutionException, InterruptedException {
        // 1. 生成逻辑 DAG
        LogicalDag logicalDag = getLogicalDag();

        // 2. 创建作业不可变信息
        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        jobConfig.getName(),
                        isStartWithSavePoint,
                        serializationService,
                        logicalDag,
                        jarUrls,
                        connectorJarIdentifiers);

        // 3. 通过 JobClient 创建作业代理
        return jobClient.createJobProxy(jobImmutableInformation);
    }
}
```

### 4.4 ClientJobProxy - 作业代理

```java
// 文件: job/ClientJobProxy.java
// 职责: 作业的客户端代理，提供作业控制和状态查询

public class ClientJobProxy implements Job {
    private final SeaTunnelHazelcastClient seaTunnelHazelcastClient;
    private final Long jobId;
    private JobResult jobResult;

    /**
     * 提交作业到 Master 节点
     */
    private void submitJob(JobImmutableInformation jobImmutableInformation) {
        LOGGER.info("Start submit job, job id: {}, with plugin jar {}",
                jobImmutableInformation.getJobId(),
                jobImmutableInformation.getPluginJarsUrls());

        // 编码请求
        ClientMessage request = SeaTunnelSubmitJobCodec.encodeRequest(
                jobImmutableInformation.getJobId(),
                serializationService.toData(jobImmutableInformation),
                jobImmutableInformation.isStartWithSavePoint());

        // 发送到 Master 并等待完成
        PassiveCompletableFuture<Void> submitJobFuture =
                seaTunnelHazelcastClient.requestOnMasterAndGetCompletableFuture(request);
        submitJobFuture.join();

        LOGGER.info("Submit job finished, job id: {}, job name: {}",
                jobImmutableInformation.getJobId(),
                jobImmutableInformation.getJobName());
    }

    /**
     * 等待作业完成（阻塞）
     */
    @Override
    public JobResult waitForJobCompleteV2() {
        jobResult = RetryUtils.retryWithException(
                () -> {
                    PassiveCompletableFuture<JobResult> jobFuture = doWaitForJobComplete();
                    return jobFuture.get();
                },
                new RetryUtils.RetryMaterial(100000, true, ...));
        return jobResult;
    }

    /**
     * 取消作业
     */
    @Override
    public void cancelJob() {
        PassiveCompletableFuture<Void> cancelFuture =
                seaTunnelHazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelCancelJobCodec.encodeRequest(jobId));
        cancelFuture.join();
    }

    /**
     * 获取作业状态
     */
    @Override
    public JobStatus getJobStatus() {
        int jobStatusOrdinal = seaTunnelHazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetJobStatusCodec.encodeRequest(jobId),
                SeaTunnelGetJobStatusCodec::decodeResponse);
        return JobStatus.values()[jobStatusOrdinal];
    }
}
```

---

## 五、engine-server 服务端模块

### 5.1 包结构

```
org.apache.seatunnel.engine.server/
├── SeaTunnelServer.java           # 服务入口
├── SeaTunnelServerStarter.java    # 服务启动器
├── CoordinatorService.java        # 协调器服务
├── TaskExecutionService.java      # 任务执行服务
├── CheckpointService.java         # 检查点服务
├── EventService.java              # 事件服务
├── JettyService.java              # REST API 服务
├── dag/
│   ├── physical/                  # 物理执行图
│   │   ├── PhysicalPlan.java
│   │   ├── SubPlan.java
│   │   └── PhysicalVertex.java
│   └── execution/                 # 执行图
├── master/
│   ├── JobMaster.java             # 作业 Master
│   └── JobHistoryService.java     # 历史服务
├── resourcemanager/               # 资源管理
│   ├── ResourceManager.java
│   ├── allocation/
│   └── thirdparty/
│       ├── kubernetes/
│       └── yarn/
├── task/                          # 任务执行
│   ├── SeaTunnelTask.java
│   ├── SourceSplitEnumeratorTask.java
│   ├── SourceReaderTask.java
│   ├── SinkWriterTask.java
│   └── flow/                      # 数据流
├── service/
│   ├── slot/                      # Slot 管理
│   └── jar/                       # JAR 包管理
├── rest/                          # REST API
│   ├── servlet/
│   └── service/
└── telemetry/                     # 遥测
    ├── metrics/
    └── log/
```

### 5.2 SeaTunnelServer - 服务入口

```java
// 文件: SeaTunnelServer.java
// 职责: SeaTunnel 引擎的核心服务类

@Slf4j
public class SeaTunnelServer
        implements ManagedService, MembershipAwareService, LiveOperationsTracker {

    public static final String SERVICE_NAME = "st:impl:seaTunnelServer";

    private NodeEngineImpl nodeEngine;
    private volatile SlotService slotService;
    private TaskExecutionService taskExecutionService;
    private ClassLoaderService classLoaderService;
    private CoordinatorService coordinatorService;
    private CheckpointService checkpointService;
    private JettyService jettyService;
    private EventService eventService;
    private SeaTunnelHealthMonitor seaTunnelHealthMonitor;
    private final SeaTunnelConfig seaTunnelConfig;

    /**
     * 初始化服务
     */
    @Override
    public void init(NodeEngine engine, Properties hzProperties) {
        this.nodeEngine = (NodeEngineImpl) engine;

        // 初始化类加载器服务
        classLoaderService = new DefaultClassLoaderService(
                seaTunnelConfig.getEngineConfig().isClassloaderCacheMode(), nodeEngine);

        // 初始化事件服务
        eventService = new EventService(nodeEngine);

        // 根据角色启动不同服务
        ClusterRole role = seaTunnelConfig.getEngineConfig().getClusterRole();
        if (role == ClusterRole.MASTER_AND_WORKER) {
            startWorker();
            startMaster();
        } else if (role == ClusterRole.WORKER) {
            startWorker();
        } else {
            startMaster();
        }

        // 初始化健康监控
        seaTunnelHealthMonitor = new SeaTunnelHealthMonitor(node);

        // 启动 Jetty REST API
        if (httpConfig.isEnabled()) {
            jettyService = new JettyService(nodeEngine, seaTunnelConfig);
            jettyService.createJettyServer();
        }
    }

    /**
     * 启动 Master 服务
     */
    private void startMaster() {
        // 协调器服务（作业调度、资源管理）
        coordinatorService = new CoordinatorService(nodeEngine, this, engineConfig);
        // 检查点服务
        checkpointService = new CheckpointService(engineConfig.getCheckpointConfig());
        // 定时打印执行信息
        monitorService.scheduleAtFixedRate(this::printExecutionInfo, ...);
    }

    /**
     * 启动 Worker 服务
     */
    private void startWorker() {
        // 任务执行服务
        taskExecutionService = new TaskExecutionService(classLoaderService, nodeEngine, eventService);
        taskExecutionService.start();
        // Slot 服务
        getSlotService();
    }

    /**
     * 判断当前节点是否为 Master
     */
    public boolean isMasterNode() {
        return nodeEngine.getThisAddress().equals(nodeEngine.getMasterAddress());
    }

    /**
     * 处理成员移除事件（故障转移）
     */
    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        if (isMasterNode()) {
            this.getCoordinatorService().memberRemoved(event);
        }
    }
}
```

### 5.3 CoordinatorService - 协调器服务

```java
// 文件: CoordinatorService.java
// 职责: 作业调度、资源分配、状态管理的核心服务

public class CoordinatorService {
    private final NodeEngineImpl nodeEngine;

    // 资源管理器
    private volatile ResourceManager resourceManager;

    // 历史作业服务
    private JobHistoryService jobHistoryService;

    // 运行中的作业信息 (IMap: jobId -> JobInfo)
    private IMap<Long, JobInfo> runningJobInfoIMap;

    // 运行中的作业状态 (IMap: jobId/pipelineId/taskId -> status)
    private IMap<Object, Object> runningJobStateIMap;

    // 运行中的 JobMaster (内存)
    private final Map<Long, JobMaster> runningJobMasterMap = new ConcurrentHashMap<>();

    // 待执行作业队列
    private final PeekBlockingQueue<PendingJobInfo> pendingJobQueue;

    /**
     * 提交作业
     */
    public PassiveCompletableFuture<Void> submitJob(long jobId, Data jobImmutableData, boolean isRestore) {
        // 1. 反序列化作业信息
        JobImmutableInformation jobImmutableInfo = deserialize(jobImmutableData);

        // 2. 创建 JobMaster
        JobMaster jobMaster = new JobMaster(
                nodeEngine, this, jobImmutableInfo, checkpointService);

        // 3. 初始化 JobMaster
        jobMaster.init();

        // 4. 加入待执行队列或直接执行
        if (scheduleStrategy == ScheduleStrategy.QUEUE) {
            pendingJobQueue.add(new PendingJobInfo(jobMaster));
        } else {
            jobMaster.run();
        }

        return new PassiveCompletableFuture<>(completableFuture);
    }

    /**
     * 取消作业
     */
    public PassiveCompletableFuture<Void> cancelJob(long jobId) {
        JobMaster jobMaster = runningJobMasterMap.get(jobId);
        if (jobMaster != null) {
            return jobMaster.cancelJob();
        }
        throw new JobNotFoundException(jobId);
    }

    /**
     * 获取作业状态
     */
    public JobStatus getJobStatus(long jobId) {
        Object status = runningJobStateIMap.get(jobId);
        if (status != null) {
            return (JobStatus) status;
        }
        // 从历史记录查询
        return jobHistoryService.getJobStatus(jobId);
    }

    /**
     * 等待作业完成
     */
    public PassiveCompletableFuture<JobResult> waitForJobComplete(long jobId) {
        JobMaster jobMaster = runningJobMasterMap.get(jobId);
        return jobMaster.getJobMasterCompleteFuture();
    }

    /**
     * 成员移除处理（故障转移）
     */
    public void memberRemoved(MembershipServiceEvent event) {
        // 重新调度在故障节点上运行的任务
        runningJobMasterMap.values().forEach(jobMaster -> {
            jobMaster.handleMemberRemoved(event.getMember());
        });
    }
}
```

### 5.4 TaskExecutionService - 任务执行服务

```java
// 文件: TaskExecutionService.java
// 职责: Worker 节点上的任务执行管理

public class TaskExecutionService implements DynamicMetricsProvider {
    // 正在执行的任务组
    private final ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts;

    // 任务执行线程池
    private final ExecutorService taskExecutor;

    /**
     * 部署任务组
     */
    public PassiveCompletableFuture<Void> deployTask(
            TaskGroupLocation taskGroupLocation,
            TaskGroupDeploymentConfig config) {
        // 1. 创建任务组上下文
        TaskGroupContext context = new TaskGroupContext(taskGroupLocation, config);

        // 2. 初始化任务
        for (TaskConfig taskConfig : config.getTasks()) {
            SeaTunnelTask task = TaskFactory.createTask(taskConfig);
            context.addTask(task);
        }

        // 3. 提交到执行线程池
        taskExecutor.submit(() -> {
            context.runAllTasks();
        });

        return context.getDeployFuture();
    }

    /**
     * 取消任务组
     */
    public void cancelTask(TaskGroupLocation taskGroupLocation) {
        TaskGroupContext context = executionContexts.get(taskGroupLocation);
        if (context != null) {
            context.cancel();
        }
    }
}
```

### 5.5 物理执行图

```
LogicalDag (逻辑图)
     │
     ▼ JobMaster 转换
PhysicalPlan (物理计划)
     │
     ├── SubPlan (Pipeline 1)
     │      ├── PhysicalVertex (Source, parallelism=2)
     │      │      ├── Task 1
     │      │      └── Task 2
     │      ├── PhysicalVertex (Transform)
     │      └── PhysicalVertex (Sink)
     │
     └── SubPlan (Pipeline 2)
            └── ...
```

**核心类**:

| 类 | 职责 |
|----|------|
| `PhysicalPlan` | 物理执行计划，包含多个 Pipeline |
| `SubPlan` | 单个 Pipeline，对应一条数据流 |
| `PhysicalVertex` | 物理顶点，包含并行任务 |
| `TaskGroup` | 任务组，调度的基本单位 |

---

## 六、作业执行流程

### 6.1 作业提交流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          作业提交流程                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Client                                    Master                        │
│    │                                         │                           │
│    │  1. 解析配置文件                         │                           │
│    │     - ConfigBuilder.of()               │                           │
│    │     - MultipleTableJobConfigParser     │                           │
│    │                                         │                           │
│    │  2. 生成 LogicalDag                     │                           │
│    │     - Action 列表                       │                           │
│    │     - LogicalVertex/Edge               │                           │
│    │                                         │                           │
│    │  3. 创建 JobImmutableInformation        │                           │
│    │                                         │                           │
│    │  4. SeaTunnelSubmitJobCodec.encode()   │                           │
│    │──────────────────────────────────────>│                           │
│    │                                         │                           │
│    │                                         │  5. CoordinatorService    │
│    │                                         │     .submitJob()          │
│    │                                         │                           │
│    │                                         │  6. 创建 JobMaster        │
│    │                                         │                           │
│    │                                         │  7. 生成 PhysicalPlan     │
│    │                                         │     - LogicalDag →        │
│    │                                         │       PhysicalPlan        │
│    │                                         │                           │
│    │                                         │  8. 资源分配              │
│    │                                         │     - ResourceManager     │
│    │                                         │     - SlotService         │
│    │                                         │                           │
│    │                                         │  9. 部署任务到 Worker      │
│    │<──────────────────────────────────────│                           │
│    │                                         │                           │
│    │  10. 等待作业完成                        │                           │
│    │      ClientJobProxy.waitForJobComplete()│                           │
│    │                                         │                           │
└─────────────────────────────────────────────────────────────────────────┘
```

### 6.2 任务执行流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Worker 任务执行                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐ │
│  │ SourceSplit      │     │ SourceReader     │     │ SinkWriter       │ │
│  │ EnumeratorTask   │     │ Task             │     │ Task             │ │
│  └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘ │
│           │                        │                        │           │
│           │  1. 分配 Split          │                        │           │
│           │ ─────────────────────> │                        │           │
│           │                        │                        │           │
│           │                        │  2. 读取数据            │           │
│           │                        │     SeaTunnelRow       │           │
│           │                        │                        │           │
│           │                        │  3. 发送到下游          │           │
│           │                        │ ─────────────────────> │           │
│           │                        │                        │           │
│           │                        │                        │  4. 写入  │
│           │                        │                        │           │
│           │                        │  5. Checkpoint         │           │
│           │ <───────────────────── │ <───────────────────── │           │
│           │                        │                        │           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 七、检查点机制

### 7.1 检查点流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Checkpoint 流程                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  CheckpointCoordinator (Master)                                          │
│        │                                                                 │
│        │  1. 触发 Checkpoint (定时)                                       │
│        │                                                                 │
│        ▼                                                                 │
│  ┌─────────────────┐                                                    │
│  │ Barrier 注入     │                                                    │
│  └────────┬────────┘                                                    │
│           │                                                              │
│           ▼                                                              │
│  SourceReader ────> Transform ────> SinkWriter                          │
│       │                                    │                             │
│       │  2. 保存状态                         │  3. 预提交                  │
│       │                                    │                             │
│       ▼                                    ▼                             │
│  CheckpointCoordinator                                                   │
│        │                                                                 │
│        │  4. 所有 Task 完成                                               │
│        │                                                                 │
│        ▼                                                                 │
│  ┌─────────────────┐                                                    │
│  │ 持久化到 Storage │                                                    │
│  │ (HDFS/S3/Local) │                                                    │
│  └─────────────────┘                                                    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 7.2 检查点存储

```
seatunnel-engine-storage/
├── checkpoint-storage-api/        # 存储 API
└── checkpoint-storage-plugins/
    ├── checkpoint-storage-hdfs/   # HDFS 存储
    └── checkpoint-storage-local-file/  # 本地文件存储
```

---

## 八、资源管理

### 8.1 Slot 模型

```
Worker Node
    │
    ├── Slot 1 ─── TaskGroup (Source + Transform)
    │
    ├── Slot 2 ─── TaskGroup (Sink)
    │
    └── Slot 3 ─── (空闲)

配置:
slot-service:
  dynamic-slot: true  # 动态 Slot（根据需求分配）
```

### 8.2 资源分配策略

```java
// AllocateStrategy.java
public enum AllocateStrategy {
    RANDOM,      // 随机分配
    ROUND_ROBIN, // 轮询分配
    LOCALITY     // 本地优先
}
```

### 8.3 第三方资源集成

```
resourcemanager/thirdparty/
├── kubernetes/   # K8s 集成
│   └── KubernetesResourceManager.java
└── yarn/         # YARN 集成
    └── YarnResourceManager.java
```

---

## 九、REST API

### 9.1 API 端点

| 端点 | 方法 | 功能 |
|------|------|------|
| `/running-jobs` | GET | 获取运行中的作业 |
| `/job-info/{jobId}` | GET | 获取作业详情 |
| `/cancel-job/{jobId}` | POST | 取消作业 |
| `/overview` | GET | 集群概览 |
| `/metrics` | GET | Prometheus 指标 |
| `/submit-job` | POST | 提交作业 |

### 9.2 JettyService

```java
// JettyService.java
public class JettyService {
    public void createJettyServer() {
        Server server = new Server(httpConfig.getPort());

        // 注册 Servlet
        ServletContextHandler context = new ServletContextHandler();
        context.addServlet(new ServletHolder(new RunningJobsServlet()), "/running-jobs");
        context.addServlet(new ServletHolder(new JobInfoServlet()), "/job-info/*");
        context.addServlet(new ServletHolder(new OverviewServlet()), "/overview");
        // ...

        server.setHandler(context);
        server.start();
    }
}
```

---

## 十、类图总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Client                                      │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌───────────────────────────┐                  │
│  │ SeaTunnelClient  │───>│ ClientJobExecutionEnvironment │             │
│  └────────┬─────────┘    └─────────────┬─────────────┘                  │
│           │                            │                                 │
│           │                            ▼                                 │
│           │              ┌───────────────────────────┐                  │
│           │              │     ClientJobProxy        │                  │
│           │              └─────────────┬─────────────┘                  │
│           │                            │                                 │
│           ▼                            ▼                                 │
│  ┌──────────────────┐    ┌───────────────────────────┐                  │
│  │ SeaTunnelHazelcast│    │     JobClient            │                  │
│  │     Client        │    └───────────────────────────┘                  │
│  └────────┬─────────┘                                                   │
│           │ (Hazelcast RPC)                                              │
└───────────┼─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Server                                      │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                       SeaTunnelServer                            │   │
│  │  (ManagedService, MembershipAwareService)                        │   │
│  └──────────────────────────────┬───────────────────────────────────┘   │
│                                 │                                        │
│        ┌────────────────────────┼────────────────────────┐              │
│        │                        │                        │              │
│        ▼                        ▼                        ▼              │
│  ┌────────────────┐   ┌─────────────────┐   ┌──────────────────────┐   │
│  │CoordinatorService│   │TaskExecutionService│  │   SlotService    │   │
│  │  (Master)        │   │   (Worker)        │  │   (Worker)        │   │
│  └───────┬────────┘   └─────────────────┘   └──────────────────────┘   │
│          │                                                              │
│          ▼                                                              │
│  ┌────────────────┐   ┌─────────────────┐                              │
│  │   JobMaster    │   │ResourceManager  │                              │
│  └───────┬────────┘   └─────────────────┘                              │
│          │                                                              │
│          ▼                                                              │
│  ┌────────────────────────────────────────┐                            │
│  │            PhysicalPlan                │                            │
│  │  ┌──────────┐  ┌──────────┐            │                            │
│  │  │ SubPlan  │  │ SubPlan  │ ...        │                            │
│  │  └──────────┘  └──────────┘            │                            │
│  └────────────────────────────────────────┘                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 十一、总结

### 11.1 模块职责

| 模块 | 职责 |
|------|------|
| `engine-common` | 配置、异常、常量、作业状态定义 |
| `engine-core` | DAG 结构、配置解析、通信协议 |
| `engine-client` | 作业提交、状态查询、客户端 API |
| `engine-server` | 调度、执行、检查点、资源管理 |
| `engine-storage` | 检查点持久化 |
| `engine-serializer` | Protobuf 序列化 |

### 11.2 核心设计

1. **基于 Hazelcast 的分布式架构**: 利用 Hazelcast 实现集群通信、状态共享
2. **Master-Worker 模式**: Master 负责调度，Worker 负责执行
3. **两阶段 DAG 转换**: LogicalDag → PhysicalPlan
4. **Slot 资源模型**: 类似 Flink 的资源抽象
5. **Barrier 检查点**: 实现 Exactly-Once 语义

### 11.3 扩展点

1. **资源管理器**: 支持 K8s、YARN 等
2. **检查点存储**: 支持 HDFS、S3、本地文件
3. **REST API**: 可扩展新的管理接口

---

*文档结束*

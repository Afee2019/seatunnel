# SeaTunnel Translation 模块源码分析

> 文档编号: 009
> 模块路径: seatunnel-translation/
> 更新日期: 2025-12-29

---

## 一、模块概述

### 1.1 功能定位

`seatunnel-translation` 是 SeaTunnel 的**引擎适配层**，负责将 SeaTunnel Connector V2 API 翻译为各执行引擎的原生 API：

- **Flink 适配**: 将 SeaTunnel Source/Sink 翻译为 Flink Source/Sink API
- **Spark 适配**: 将 SeaTunnel Source/Sink 翻译为 Spark DataSource V2 API

这是 SeaTunnel **"Write Once, Run Anywhere"** 设计理念的核心实现。

### 1.2 模块结构

```
seatunnel-translation/
├── pom.xml
├── seatunnel-translation-base/              # 公共基类
│   └── src/main/java/org/apache/seatunnel/translation/
│       ├── source/                          # Source 翻译基类
│       │   ├── BaseSourceFunction.java
│       │   ├── ParallelSource.java          # 并行模式
│       │   ├── CoordinatedSource.java       # 协调模式
│       │   ├── ParallelEnumeratorContext.java
│       │   ├── ParallelReaderContext.java
│       │   ├── CoordinatedEnumeratorContext.java
│       │   └── CoordinatedReaderContext.java
│       ├── sink/                            # Sink 翻译基类
│       │   ├── SinkConverter.java
│       │   ├── SinkWriterConverter.java
│       │   └── SinkCommitterConverter.java
│       └── serialization/                   # 序列化工具
│           └── RowConverter.java
│
├── seatunnel-translation-flink/             # Flink 适配层
│   ├── seatunnel-translation-flink-common/  # Flink 公共实现
│   ├── seatunnel-translation-flink-13/      # Flink 1.13 适配
│   ├── seatunnel-translation-flink-15/      # Flink 1.15 适配
│   └── seatunnel-translation-flink-20/      # Flink 2.0 适配
│
└── seatunnel-translation-spark/             # Spark 适配层
    ├── seatunnel-translation-spark-common/  # Spark 公共实现
    ├── seatunnel-translation-spark-2.4/     # Spark 2.4 适配
    └── seatunnel-translation-spark-3.3/     # Spark 3.3 适配
```

### 1.3 模块统计

| 子模块 | Java 文件数 | 主要功能 |
|--------|-------------|----------|
| translation-base | 14 | 公共基类、Source/Sink 翻译抽象 |
| translation-flink-common | 24 | Flink Source/Sink/Metric 实现 |
| translation-flink-13/15/20 | 各 3-5 | 版本特定适配 |
| translation-spark-common | 11 | Spark 公共类型转换 |
| translation-spark-2.4/3.3 | 各 20+ | Spark DataSource V2 实现 |
| **总计** | **110** | - |

---

## 二、核心设计理念

### 2.1 适配器模式

Translation 层采用**适配器模式**，将 SeaTunnel API 转换为各引擎的原生 API：

```
┌─────────────────────────────────────────────────────────────┐
│                  SeaTunnel Connector V2 API                  │
│  ┌─────────────────┐    ┌─────────────────┐                 │
│  │ SeaTunnelSource │    │ SeaTunnelSink   │                 │
│  └────────┬────────┘    └────────┬────────┘                 │
└───────────┼──────────────────────┼──────────────────────────┘
            │                      │
            ▼                      ▼
┌───────────────────────────────────────────────────────────────┐
│                   Translation Layer                            │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ ┌─────────────────┐              ┌─────────────────┐     │ │
│  │ │   FlinkSource   │              │   FlinkSink     │     │ │
│  │ │ (Flink API)     │              │ (Flink API)     │     │ │
│  │ └─────────────────┘              └─────────────────┘     │ │
│  │ ┌─────────────────┐              ┌─────────────────┐     │ │
│  │ │ SparkSource     │              │ SparkSink       │     │ │
│  │ │ (DataSource V2) │              │ (DataSource V2) │     │ │
│  │ └─────────────────┘              └─────────────────┘     │ │
│  └──────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
            │                      │
            ▼                      ▼
┌───────────────────────────────────────────────────────────────┐
│              执行引擎 (Flink / Spark)                          │
└───────────────────────────────────────────────────────────────┘
```

### 2.2 两种 Source 模式

SeaTunnel 支持两种 Source 运行模式：

| 模式 | 类 | 适用场景 | 特点 |
|------|-----|----------|------|
| **Parallel** | `ParallelSource` | 批处理、简单流 | 每个 Task 独立运行 Enumerator + Reader |
| **Coordinated** | `CoordinatedSource` | CDC、复杂流 | 单个 Enumerator 协调多个 Reader |

---

## 三、Translation Base 模块

### 3.1 BaseSourceFunction 接口

**路径**: `translation/source/BaseSourceFunction.java`

```java
public interface BaseSourceFunction<T> {
    // 打开资源
    void open() throws Exception;

    // 运行主循环
    void run(Collector<T> collector) throws Exception;

    // 关闭资源
    void close() throws IOException;

    // 快照状态（用于 Checkpoint）
    Map<Integer, List<byte[]>> snapshotState(long checkpointId) throws Exception;

    // Checkpoint 完成通知
    void notifyCheckpointComplete(long checkpointId) throws Exception;

    // Checkpoint 中止通知
    void notifyCheckpointAborted(long checkpointId) throws Exception;
}
```

### 3.2 ParallelSource

**路径**: `translation/source/ParallelSource.java`

**特点**: 每个并行度独立运行一个 Enumerator 和 Reader。

```java
public class ParallelSource<T, SplitT extends SourceSplit, StateT extends Serializable>
        implements BaseSourceFunction<T> {

    protected final SeaTunnelSource<T, SplitT, StateT> source;
    protected final ParallelEnumeratorContext<SplitT> parallelEnumeratorContext;
    protected final ParallelReaderContext readerContext;

    protected final SourceSplitEnumerator<SplitT, StateT> splitEnumerator;
    protected final SourceReader<T, SplitT> reader;

    @Override
    public void open() throws Exception {
        executorService = ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(1, ...);
        splitEnumerator.open();
        reader.open();
        splitEnumerator.registerReader(subtaskId);
    }

    @Override
    public void run(Collector<T> collector) throws Exception {
        // 异步运行 Enumerator
        Future<?> future = executorService.submit(() -> splitEnumerator.run());

        // 主循环：持续 poll 数据
        while (running) {
            reader.pollNext(collector);
            if (collector.isEmptyThisPollNext()) {
                Thread.sleep(100);  // 无数据时休眠
            } else {
                Thread.sleep(0L);   // 让出 CPU 给 Checkpoint 线程
            }
        }
    }

    @Override
    public Map<Integer, List<byte[]>> snapshotState(long checkpointId) throws Exception {
        Map<Integer, List<byte[]>> allStates = new HashMap<>(2);

        // 保存 Enumerator 状态
        StateT enumeratorState = splitEnumerator.snapshotState(checkpointId);
        allStates.put(-1, Collections.singletonList(enumeratorStateSerializer.serialize(enumeratorState)));

        // 保存 Reader 状态
        List<SplitT> splitStates = reader.snapshotState(checkpointId);
        List<byte[]> readerStateBytes = new ArrayList<>();
        for (SplitT splitState : splitStates) {
            readerStateBytes.add(splitSerializer.serialize(splitState));
        }
        allStates.put(subtaskId, readerStateBytes);

        return allStates;
    }
}
```

**状态管理**:
- `-1` 存储 Enumerator 状态
- `subtaskId` 存储对应 Reader 状态

### 3.3 CoordinatedSource

**路径**: `translation/source/CoordinatedSource.java`

**特点**: 单个 Enumerator 协调多个 Reader，适用于需要全局协调的场景（如 CDC）。

```java
public class CoordinatedSource<T, SplitT extends SourceSplit, StateT extends Serializable>
        implements BaseSourceFunction<T> {

    protected final Integer parallelism;
    protected final Map<Integer, SourceReader<T, SplitT>> readerMap = new ConcurrentHashMap<>();
    protected final Map<Integer, AtomicBoolean> readerRunningMap;
    protected final AtomicInteger completedReader = new AtomicInteger(0);

    @Override
    public void run(Collector<T> collector) throws Exception {
        // 并行启动所有 Reader
        readerMap.entrySet().parallelStream().forEach(entry -> {
            final AtomicBoolean flag = readerRunningMap.get(entry.getKey());
            final SourceReader<T, SplitT> reader = entry.getValue();
            executorService.execute(() -> {
                while (flag.get()) {
                    reader.pollNext(collector);
                    // ...
                }
            });
        });

        // 运行 Enumerator
        splitEnumerator.run();

        // 等待所有 Reader 完成
        while (running) {
            Thread.sleep(SLEEP_TIME_INTERVAL);
        }
    }

    // 某个 Reader 完成时的回调
    protected void handleNoMoreElement(int subtaskId) {
        readerRunningMap.get(subtaskId).set(false);
        if (completedReader.incrementAndGet() == this.parallelism) {
            this.running = false;  // 所有 Reader 完成
        }
    }
}
```

### 3.4 RowConverter 抽象类

**路径**: `translation/serialization/RowConverter.java`

**职责**: SeaTunnelRow 与引擎原生行格式的双向转换

```java
public abstract class RowConverter<T> implements Serializable {
    protected final SeaTunnelDataType<?> dataType;

    // SeaTunnelRow → 引擎行
    public abstract T convert(SeaTunnelRow seaTunnelRow) throws IOException;

    // 引擎行 → SeaTunnelRow
    public abstract SeaTunnelRow reconvert(T engineRow) throws IOException;

    // 数据类型校验
    public void validate(SeaTunnelRow seaTunnelRow) throws IOException {
        // 校验每个字段的类型是否匹配
    }
}
```

---

## 四、Flink 适配层

### 4.1 模块结构

```
seatunnel-translation-flink-common/
├── source/
│   ├── FlinkSource.java              # Flink Source 适配器
│   ├── FlinkSourceReader.java        # SourceReader 适配器
│   ├── FlinkSourceEnumerator.java    # SplitEnumerator 适配器
│   ├── FlinkSourceReaderContext.java
│   ├── FlinkSourceSplitEnumeratorContext.java
│   ├── FlinkRowCollector.java        # 数据收集器
│   ├── SplitWrapper.java             # Split 包装器
│   └── SplitWrapperSerializer.java
├── sink/
│   ├── FlinkSink.java                # Flink Sink 适配器
│   ├── FlinkSinkWriter.java          # SinkWriter 适配器
│   ├── FlinkCommitter.java           # Committer 适配器
│   ├── FlinkGlobalCommitter.java     # GlobalCommitter 适配器
│   ├── FlinkSinkWriterContext.java
│   └── CommitWrapper.java
├── serialization/
│   ├── FlinkSimpleVersionedSerializer.java
│   ├── FlinkWriterStateSerializer.java
│   └── CommitWrapperSerializer.java
└── metric/
    ├── FlinkCounter.java
    ├── FlinkMeter.java
    └── FlinkMetricContext.java
```

### 4.2 FlinkSource

**路径**: `translation/flink/source/FlinkSource.java`

**职责**: 将 `SeaTunnelSource` 适配为 Flink `Source` 接口

```java
public class FlinkSource<SplitT extends SourceSplit, EnumStateT extends Serializable>
        implements Source<SeaTunnelRow, SplitWrapper<SplitT>, EnumStateT>,
                ResultTypeQueryable<SeaTunnelRow> {

    private final SeaTunnelSource<SeaTunnelRow, SplitT, EnumStateT> source;

    // Boundedness 转换
    @Override
    public Boundedness getBoundedness() {
        return source.getBoundedness() == org.apache.seatunnel.api.source.Boundedness.BOUNDED
                ? Boundedness.BOUNDED
                : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    // 创建 SourceReader
    @Override
    public SourceReader<SeaTunnelRow, SplitWrapper<SplitT>> createReader(
            SourceReaderContext readerContext) throws Exception {
        org.apache.seatunnel.api.source.SourceReader.Context context =
                new FlinkSourceReaderContext(readerContext, source);
        org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, SplitT> reader =
                source.createReader(context);
        return new FlinkSourceReader<>(reader, context, envConfig);
    }

    // 创建 SplitEnumerator
    @Override
    public SplitEnumerator<SplitWrapper<SplitT>, EnumStateT> createEnumerator(
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext) throws Exception {
        SourceSplitEnumerator.Context<SplitT> context =
                new FlinkSourceSplitEnumeratorContext<>(enumContext);
        SourceSplitEnumerator<SplitT, EnumStateT> enumerator = source.createEnumerator(context);
        return new FlinkSourceEnumerator<>(enumerator, enumContext);
    }

    // Split 序列化器
    @Override
    public SimpleVersionedSerializer<SplitWrapper<SplitT>> getSplitSerializer() {
        return new SplitWrapperSerializer<>(source.getSplitSerializer());
    }
}
```

### 4.3 FlinkSourceReader

**路径**: `translation/flink/source/FlinkSourceReader.java`

**职责**: 将 SeaTunnel SourceReader 适配为 Flink SourceReader

```java
public class FlinkSourceReader<SplitT extends SourceSplit>
        implements SourceReader<SeaTunnelRow, SplitWrapper<SplitT>> {

    private final org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, SplitT> sourceReader;
    private final FlinkRowCollector flinkRowCollector;
    private InputStatus inputStatus = InputStatus.MORE_AVAILABLE;

    @Override
    public void start() {
        sourceReader.open();
        context.getEventListener().onEvent(new ReaderOpenEvent());
    }

    @Override
    public InputStatus pollNext(ReaderOutput<SeaTunnelRow> output) throws Exception {
        sourceReader.pollNext(flinkRowCollector.withReaderOutput(output));
        if (flinkRowCollector.isEmptyThisPollNext()) {
            // 无数据时返回 NOTHING_AVAILABLE，触发等待
            return InputStatus.NOTHING_AVAILABLE;
        }
        return inputStatus;
    }

    @Override
    public List<SplitWrapper<SplitT>> snapshotState(long checkpointId) {
        List<SplitT> splitTS = sourceReader.snapshotState(checkpointId);
        return splitTS.stream().map(SplitWrapper::new).collect(Collectors.toList());
    }

    @Override
    public void addSplits(List<SplitWrapper<SplitT>> splits) {
        sourceReader.addSplits(
                splits.stream().map(SplitWrapper::getSourceSplit).collect(Collectors.toList()));
    }
}
```

### 4.4 FlinkSink

**路径**: `translation/flink/sink/FlinkSink.java`

**职责**: 将 `SeaTunnelSink` 适配为 Flink `Sink` 接口

```java
public class FlinkSink<InputT, CommT, WriterStateT, GlobalCommT>
        implements Sink<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>, GlobalCommT> {

    private final SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink;

    @Override
    public SinkWriter<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>> createWriter(
            Sink.InitContext context, List<FlinkWriterState<WriterStateT>> states) {
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new FlinkSinkWriterContext(context, parallelism);
        if (states == null || states.isEmpty()) {
            return new FlinkSinkWriter<>(sink.createWriter(stContext), 1, stContext);
        } else {
            List<WriterStateT> restoredState =
                    states.stream().map(FlinkWriterState::getState).collect(Collectors.toList());
            return new FlinkSinkWriter<>(
                    sink.restoreWriter(stContext, restoredState),
                    states.get(0).getCheckpointId() + 1,
                    stContext);
        }
    }

    @Override
    public Optional<Committer<CommitWrapper<CommT>>> createCommitter() {
        return sink.createCommitter().map(FlinkCommitter::new);
    }

    @Override
    public Optional<GlobalCommitter<CommitWrapper<CommT>, GlobalCommT>> createGlobalCommitter() {
        return sink.createAggregatedCommitter().map(FlinkGlobalCommitter::new);
    }
}
```

### 4.5 API 映射关系

| SeaTunnel API | Flink API | 说明 |
|---------------|-----------|------|
| `SeaTunnelSource` | `Source<T, Split, State>` | 数据源 |
| `SourceReader` | `SourceReader<T, Split>` | 数据读取器 |
| `SourceSplitEnumerator` | `SplitEnumerator<Split, State>` | 分片分配器 |
| `SeaTunnelSink` | `Sink<T, Commit, State, Global>` | 数据接收器 |
| `SinkWriter` | `SinkWriter<T, Commit, State>` | 数据写入器 |
| `SinkCommitter` | `Committer<Commit>` | 提交器 |
| `SinkAggregatedCommitter` | `GlobalCommitter<Commit, Global>` | 全局提交器 |
| `Boundedness.BOUNDED` | `Boundedness.BOUNDED` | 有界流 |
| `Boundedness.UNBOUNDED` | `Boundedness.CONTINUOUS_UNBOUNDED` | 无界流 |

---

## 五、Spark 适配层

### 5.1 模块结构

```
seatunnel-translation-spark-3.3/
├── source/
│   ├── SeaTunnelSparkSource.java          # Spark DataSource 入口
│   ├── SeaTunnelSourceTable.java          # Table 实现
│   ├── scan/
│   │   ├── SeaTunnelScan.java             # Scan 实现
│   │   └── SeaTunnelScanBuilder.java
│   └── partition/
│       ├── batch/                         # 批处理分区
│       │   ├── SeaTunnelBatch.java
│       │   ├── SeaTunnelBatchInputPartition.java
│       │   ├── SeaTunnelBatchPartitionReader.java
│       │   ├── ParallelBatchPartitionReader.java
│       │   └── CoordinatedBatchPartitionReader.java
│       └── micro/                         # 微批分区
│           ├── SeaTunnelMicroBatch.java
│           ├── SeaTunnelMicroBatchInputPartition.java
│           ├── SeaTunnelMicroBatchPartitionReader.java
│           ├── ParallelMicroBatchPartitionReader.java
│           └── CoordinatedMicroBatchPartitionReader.java
├── sink/
│   └── write/
│       ├── SeaTunnelWrite.java            # Write 实现
│       ├── SeaTunnelSparkDataWriterFactory.java
│       └── SeaTunnelSparkDataWriter.java
└── common/
    └── serialization/
        ├── InternalRowConverter.java      # Row 转换器
        └── SeaTunnelRowConverter.java

seatunnel-translation-spark-common/
├── serialization/
│   ├── InternalRowConverter.java          # SeaTunnelRow ↔ InternalRow
│   └── SeaTunnelRowConverter.java
├── utils/
│   ├── TypeConverterUtils.java            # 类型转换工具
│   └── InstantConverterUtils.java         # 时间转换工具
└── execution/
    └── MultiTableManager.java             # 多表管理
```

### 5.2 InternalRowConverter

**路径**: `translation/spark/serialization/InternalRowConverter.java`

**职责**: SeaTunnelRow 与 Spark InternalRow 的双向转换

```java
public final class InternalRowConverter extends RowConverter<InternalRow> {

    // SeaTunnelRow → InternalRow
    @Override
    public InternalRow convert(SeaTunnelRow seaTunnelRow) throws IOException {
        return parcel(seaTunnelRow, (SeaTunnelRowType) dataType);
    }

    private InternalRow parcel(SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType) {
        int arity = rowType.getTotalFields();
        // 额外添加 2 个字段：rowKind + tableId
        MutableValue[] values = new MutableValue[arity + 2];

        // 填充数据字段
        for (int i = 0; i < indexes.length; i++) {
            values[indexes[i] + 2] = createMutableValue(rowType.getFieldType(indexes[i]));
            Object fieldValue = convert(seaTunnelRow.getField(i), rowType.getFieldType(indexes[i]));
            if (fieldValue != null) {
                values[indexes[i] + 2].update(fieldValue);
            }
        }

        // 填充 rowKind (index 0)
        values[0] = new MutableByte();
        values[0].update(seaTunnelRow.getRowKind().toByteValue());

        // 填充 tableId (index 1)
        values[1] = new MutableAny();
        values[1].update(UTF8String.fromString(seaTunnelRow.getTableId()));

        return new SpecificInternalRow(values);
    }

    // InternalRow → SeaTunnelRow
    public SeaTunnelRow unpack(InternalRow engineRow, SeaTunnelRowType rowType) {
        RowKind rowKind = RowKind.fromByteValue(engineRow.getByte(0));
        String tableId = engineRow.getString(1);

        Object[] fields = new Object[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            fields[i] = reconvert(
                    engineRow.get(indexes[i] + 2, TypeConverterUtils.convert(rowType.getFieldType(indexes[i]))),
                    rowType.getFieldType(indexes[i]));
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setRowKind(rowKind);
        seaTunnelRow.setTableId(tableId);
        return seaTunnelRow;
    }
}
```

### 5.3 类型映射

**SeaTunnel → Spark 类型转换**:

| SeaTunnel Type | Spark Type | 转换逻辑 |
|----------------|------------|----------|
| BOOLEAN | Boolean | 直接映射 |
| TINYINT | Byte | 直接映射 |
| SMALLINT | Short | 直接映射 |
| INT | Integer | 直接映射 |
| BIGINT | Long | 直接映射 |
| FLOAT | Float | 直接映射 |
| DOUBLE | Double | 直接映射 |
| STRING | UTF8String | `UTF8String.fromString()` |
| DECIMAL | Decimal | `Decimal.apply()` |
| DATE | Int (epoch day) | `LocalDate.toEpochDay()` |
| TIME | Long (nano of day) | `LocalTime.toNanoOfDay()` |
| TIMESTAMP | Long (epoch micro) | `InstantConverterUtils.toEpochMicro()` |
| ARRAY | ArrayData | `ArrayData.toArrayData()` |
| MAP | ArrayBasedMapData | 键值分离存储 |
| ROW | SpecificInternalRow | 递归转换 |

### 5.4 Spark DataSource V2 集成

**Batch 模式**:
```
SeaTunnelSparkSource (TableProvider)
    ↓
SeaTunnelSourceTable (Table)
    ↓
SeaTunnelScanBuilder (ScanBuilder)
    ↓
SeaTunnelScan (Scan)
    ↓
SeaTunnelBatch (Batch)
    ↓
SeaTunnelBatchInputPartition (InputPartition)
    ↓
SeaTunnelBatchPartitionReader (PartitionReader)
    ├── ParallelBatchPartitionReader   (并行模式)
    └── CoordinatedBatchPartitionReader (协调模式)
```

**MicroBatch 模式**:
```
SeaTunnelSparkSource (TableProvider)
    ↓
SeaTunnelSourceTable (Table, SupportsRead)
    ↓
SeaTunnelMicroBatch (MicroBatchStream)
    ↓
SeaTunnelMicroBatchInputPartition (InputPartition)
    ↓
SeaTunnelMicroBatchPartitionReader (PartitionReader)
    ├── ParallelMicroBatchPartitionReader
    └── CoordinatedMicroBatchPartitionReader
```

---

## 六、版本适配策略

### 6.1 Flink 版本适配

| 模块 | 支持版本 | 主要差异 |
|------|----------|----------|
| flink-13 | Flink 1.13.x | 基础 Source/Sink API |
| flink-15 | Flink 1.15.x | 改进的 Checkpoint 机制 |
| flink-20 | Flink 2.0.x | 新版 API 适配 |

### 6.2 Spark 版本适配

| 模块 | 支持版本 | 主要差异 |
|------|----------|----------|
| spark-2.4 | Spark 2.4.x | DataSource V2 初版 |
| spark-3.3 | Spark 3.3.x | 改进的 DataSource V2 API |

---

## 七、Checkpoint 与状态管理

### 7.1 状态序列化流程

```
┌─────────────────────────────────────────────────────────────┐
│                    snapshotState(checkpointId)               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Enumerator State (key = -1)                                 │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ enumeratorStateSerializer.serialize(enumeratorState)    ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Reader State (key = subtaskId)                              │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ for each split: splitSerializer.serialize(split)        ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Map<Integer, List<byte[]>> allStates                        │
│  {                                                           │
│    -1: [enumeratorStateBytes],                               │
│    0: [split0Bytes, split1Bytes, ...],                       │
│    1: [split2Bytes, split3Bytes, ...],                       │
│    ...                                                       │
│  }                                                           │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 状态恢复流程

```java
// ParallelSource 构造函数
if (restoredState != null && restoredState.size() > 0) {
    // 恢复 Enumerator 状态
    StateT restoredEnumeratorState = null;
    if (restoredState.containsKey(-1)) {
        restoredEnumeratorState = enumeratorStateSerializer.deserialize(restoredState.get(-1).get(0));
    }

    // 恢复 Reader 状态
    restoredSplitState = new ArrayList<>(restoredState.get(subtaskId).size());
    for (byte[] splitBytes : restoredState.get(subtaskId)) {
        restoredSplitState.add(splitSerializer.deserialize(splitBytes));
    }

    // 使用恢复的状态创建 Enumerator
    splitEnumerator = source.restoreEnumerator(parallelEnumeratorContext, restoredEnumeratorState);
} else {
    // 首次启动，创建新的 Enumerator
    splitEnumerator = source.createEnumerator(parallelEnumeratorContext);
}
```

---

## 八、设计模式总结

### 8.1 使用的设计模式

| 模式 | 应用场景 |
|------|----------|
| **适配器模式** | FlinkSource/FlinkSink 适配 SeaTunnel API 到 Flink API |
| **包装器模式** | SplitWrapper、CommitWrapper 包装 SeaTunnel 对象 |
| **策略模式** | ParallelSource vs CoordinatedSource 两种运行策略 |
| **模板方法模式** | RowConverter 定义转换骨架，子类实现具体转换 |
| **工厂模式** | PartitionReaderFactory 创建 PartitionReader |

### 8.2 关键设计决策

1. **状态独立**: 每个引擎的状态序列化独立实现，不依赖引擎特定格式
2. **类型桥接**: RowConverter 作为类型系统的桥梁
3. **版本隔离**: 不同引擎版本的适配代码隔离在独立模块
4. **事件传递**: 通过 EventListener 传递生命周期事件

---

## 九、总结

`seatunnel-translation` 模块是 SeaTunnel 多引擎支持的核心：

1. **统一抽象**: 将 SeaTunnel Connector API 翻译为各引擎原生 API
2. **双模式支持**: Parallel 和 Coordinated 两种 Source 运行模式
3. **完整状态管理**: 支持 Checkpoint 和故障恢复
4. **多版本适配**: 支持 Flink 1.13/1.15/2.0 和 Spark 2.4/3.3
5. **类型桥接**: 完整的 SeaTunnelRow 与引擎行格式转换

---

**相关文档**:
- [003-seatunnel-core模块源码分析.md](./003-seatunnel-core模块源码分析.md) - 启动器实现
- [004-seatunnel-engine模块源码分析.md](./004-seatunnel-engine模块源码分析.md) - Zeta 引擎实现
- [005-seatunnel-api模块源码分析.md](./005-seatunnel-api模块源码分析.md) - Connector API 定义

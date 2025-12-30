# SeaTunnel API 模块源码分析

> 文档编号: 005
> 模块路径: seatunnel-api/
> 更新日期: 2025-12-29

---

## 一、模块概述

`seatunnel-api` 是 SeaTunnel 的核心 API 模块，定义了 Connector V2 的标准接口，包括：
- **Source API**: 数据源读取接口
- **Sink API**: 数据目标写入接口
- **Transform API**: 数据转换接口
- **Table API**: 表结构、Catalog、数据类型

所有连接器都需要实现这些接口。

### 1.1 包结构

```
org.apache.seatunnel.api/
├── source/                 # 数据源 API
├── sink/                   # 数据写入 API
├── transform/              # 数据转换 API
├── table/                  # 表相关
│   ├── type/               # 数据类型系统
│   ├── catalog/            # Catalog 接口
│   ├── factory/            # 工厂模式 (SPI)
│   ├── connector/          # 表连接器
│   ├── converter/          # 类型转换器
│   └── schema/             # Schema 变更
├── serialization/          # 序列化接口
├── state/                  # 状态管理
├── event/                  # 事件系统
├── common/                 # 公共类
│   └── metrics/            # 指标
├── configuration/          # 配置管理
├── options/                # 配置选项
├── annotation/             # 注解
├── env/                    # 执行环境
└── tracing/                # 链路追踪
```

---

## 二、Source API - 数据源接口

### 2.1 核心接口关系

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Source API 架构                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────────┐                                           │
│  │   SeaTunnelSource    │  <-- 数据源入口，工厂类                    │
│  │  (工厂接口)           │                                           │
│  └──────────┬───────────┘                                           │
│             │ 创建                                                   │
│     ┌───────┴───────┐                                               │
│     ▼               ▼                                               │
│  ┌───────────────┐  ┌────────────────────┐                          │
│  │SourceSplit   │  │SourceReader        │  <-- Worker 上运行        │
│  │Enumerator    │  │ (数据读取)          │                          │
│  │(分片生成)     │  └────────────────────┘                          │
│  └───────┬──────┘            ▲                                      │
│          │                   │                                       │
│          │  分配 Split       │  读取数据                             │
│          └───────────────────┘                                      │
│                                                                      │
│  运行位置:                                                           │
│  - SeaTunnelSource: 客户端初始化                                    │
│  - SourceSplitEnumerator: Master 节点                               │
│  - SourceReader: Worker 节点                                        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 SeaTunnelSource - 数据源接口

```java
// 文件: source/SeaTunnelSource.java
// 职责: 数据源的入口接口，负责创建 Reader 和 Enumerator

/**
 * @param <T>      产生的记录类型
 * @param <SplitT> 分片类型
 * @param <StateT> 检查点状态类型
 */
public interface SeaTunnelSource<T, SplitT extends SourceSplit, StateT extends Serializable>
        extends Serializable,
                PluginIdentifierInterface,
                SeaTunnelPluginLifeCycle,
                SeaTunnelJobAware {

    /**
     * 获取数据源的边界性
     * BOUNDED: 有界（批处理）
     * UNBOUNDED: 无界（流处理）
     */
    Boundedness getBoundedness();

    /**
     * 获取输出的 CatalogTable 列表
     * 替代已废弃的 getProducedType()
     */
    default List<CatalogTable> getProducedCatalogTables();

    /**
     * 创建 SourceReader（在 Worker 上运行）
     */
    SourceReader<T, SplitT> createReader(SourceReader.Context readerContext) throws Exception;

    /**
     * 创建 SourceSplitEnumerator（在 Master 上运行）
     */
    SourceSplitEnumerator<SplitT, StateT> createEnumerator(
            SourceSplitEnumerator.Context<SplitT> enumeratorContext) throws Exception;

    /**
     * 从检查点恢复 Enumerator
     */
    SourceSplitEnumerator<SplitT, StateT> restoreEnumerator(
            SourceSplitEnumerator.Context<SplitT> enumeratorContext,
            StateT checkpointState) throws Exception;

    /**
     * 获取分片序列化器
     */
    default Serializer<SplitT> getSplitSerializer();

    /**
     * 获取状态序列化器
     */
    default Serializer<StateT> getEnumeratorStateSerializer();
}
```

**泛型参数说明**:
- `T`: 产生的数据类型（通常是 `SeaTunnelRow`）
- `SplitT`: 分片类型（如 `JdbcSourceSplit`、`KafkaSourceSplit`）
- `StateT`: Enumerator 状态类型（用于检查点）

### 2.3 SourceSplitEnumerator - 分片枚举器

```java
// 文件: source/SourceSplitEnumerator.java
// 职责: 生成和分配数据分片（在 Master 节点运行）

public interface SourceSplitEnumerator<SplitT extends SourceSplit, StateT>
        extends AutoCloseable, CheckpointListener {

    void open();

    /**
     * 运行枚举器，生成分片
     * 调用顺序: open() -> addSplitsBack() -> registerReader() -> run()
     */
    void run() throws Exception;

    void close() throws IOException;

    /**
     * 当 Reader 失败时，将分片返还给 Enumerator
     */
    void addSplitsBack(List<SplitT> splits, int subtaskId);

    /**
     * 获取未分配的分片数量
     */
    int currentUnassignedSplitSize();

    /**
     * 处理 Reader 的分片请求
     */
    void handleSplitRequest(int subtaskId);

    /**
     * 注册 Reader
     */
    void registerReader(int subtaskId);

    /**
     * 保存状态快照（用于检查点）
     */
    StateT snapshotState(long checkpointId) throws Exception;

    /**
     * 处理来自 Reader 的事件
     */
    default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent);

    /**
     * Enumerator 上下文
     */
    interface Context<SplitT extends SourceSplit> {
        int currentParallelism();
        Set<Integer> registeredReaders();
        void assignSplit(int subtaskId, List<SplitT> splits);
        void signalNoMoreSplits(int subtask);
        void sendEventToSourceReader(int subtaskId, SourceEvent event);
        MetricsContext getMetricsContext();
        EventListener getEventListener();
    }
}
```

### 2.4 SourceReader - 数据读取器

```java
// 文件: source/SourceReader.java
// 职责: 读取数据（在 Worker 节点运行）

public interface SourceReader<T, SplitT extends SourceSplit>
        extends AutoCloseable, CheckpointListener {

    void open() throws Exception;

    void close() throws IOException;

    /**
     * 拉取下一批数据
     * @param output 数据收集器
     */
    void pollNext(Collector<T> output) throws Exception;

    /**
     * 保存分片状态快照
     */
    List<SplitT> snapshotState(long checkpointId) throws Exception;

    /**
     * 接收 Enumerator 分配的分片
     */
    void addSplits(List<SplitT> splits);

    /**
     * 收到无更多分片的信号
     */
    void handleNoMoreSplits();

    /**
     * 处理来自 Enumerator 的事件
     */
    default void handleSourceEvent(SourceEvent sourceEvent);

    /**
     * Reader 上下文
     */
    interface Context {
        int getIndexOfSubtask();
        Boundedness getBoundedness();
        void signalNoMoreElement();
        void sendSplitRequest();
        void sendSourceEventToEnumerator(SourceEvent sourceEvent);
        MetricsContext getMetricsContext();
        EventListener getEventListener();
    }
}
```

### 2.5 SourceSplit - 分片接口

```java
// 文件: source/SourceSplit.java
// 职责: 定义数据分片

public interface SourceSplit extends Serializable {
    /**
     * 获取分片唯一标识
     */
    String splitId();
}
```

### 2.6 Boundedness - 边界性

```java
// 文件: source/Boundedness.java

public enum Boundedness {
    BOUNDED,    // 有界数据源（批处理）
    UNBOUNDED   // 无界数据源（流处理）
}
```

### 2.7 辅助接口

| 接口 | 功能 |
|------|------|
| `SupportParallelism` | 支持自定义并行度 |
| `SupportColumnProjection` | 支持列裁剪 |
| `SupportCoordinate` | 支持协调器模式 |
| `SupportSchemaEvolution` | 支持 Schema 演进 |

---

## 三、Sink API - 数据写入接口

### 3.1 核心接口关系

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Sink API 架构                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────────┐                                           │
│  │   SeaTunnelSink      │  <-- 数据写入入口，工厂类                  │
│  │  (工厂接口)           │                                           │
│  └──────────┬───────────┘                                           │
│             │ 创建                                                   │
│     ┌───────┴───────────────────┬────────────────────┐              │
│     ▼                           ▼                    ▼              │
│  ┌───────────────┐    ┌─────────────────┐   ┌────────────────────┐ │
│  │ SinkWriter    │    │ SinkCommitter   │   │SinkAggregated     │ │
│  │ (数据写入)    │    │ (提交确认)       │   │Committer          │ │
│  └───────┬───────┘    └────────┬────────┘   │(聚合提交)         │ │
│          │                     │             └────────────────────┘ │
│          │  prepareCommit      │                                    │
│          └─────────────────────┘                                    │
│                                                                      │
│  两阶段提交 (2PC):                                                   │
│  1. SinkWriter.prepareCommit() -> CommitInfo                        │
│  2. SinkCommitter.commit(List<CommitInfo>)                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.2 SeaTunnelSink - 数据写入接口

```java
// 文件: sink/SeaTunnelSink.java
// 职责: 数据写入的入口接口

/**
 * @param <IN>                   输入数据类型（SeaTunnelRow）
 * @param <StateT>               Writer 状态类型
 * @param <CommitInfoT>          提交信息类型
 * @param <AggregatedCommitInfoT> 聚合提交信息类型
 */
public interface SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
        extends Serializable,
                PluginIdentifierInterface,
                SeaTunnelPluginLifeCycle,
                SeaTunnelJobAware {

    /**
     * 创建 SinkWriter
     */
    SinkWriter<IN, CommitInfoT, StateT> createWriter(SinkWriter.Context context)
            throws IOException;

    /**
     * 从状态恢复 SinkWriter
     */
    default SinkWriter<IN, CommitInfoT, StateT> restoreWriter(
            SinkWriter.Context context, List<StateT> states) throws IOException;

    /**
     * 获取 Writer 状态序列化器
     */
    default Optional<Serializer<StateT>> getWriterStateSerializer();

    /**
     * 创建 SinkCommitter（用于两阶段提交）
     */
    default Optional<SinkCommitter<CommitInfoT>> createCommitter() throws IOException;

    /**
     * 获取提交信息序列化器
     */
    default Optional<Serializer<CommitInfoT>> getCommitInfoSerializer();

    /**
     * 创建聚合提交器
     */
    default Optional<SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT>>
            createAggregatedCommitter() throws IOException;

    /**
     * 获取写入的 CatalogTable
     */
    default Optional<CatalogTable> getWriteCatalogTable();
}
```

### 3.3 SinkWriter - 数据写入器

```java
// 文件: sink/SinkWriter.java
// 职责: 实际写入数据（在 Worker 节点运行）

public interface SinkWriter<T, CommitInfoT, StateT> {

    /**
     * 写入一条数据
     */
    void write(T element) throws IOException;

    /**
     * 预提交（第一阶段）
     * 返回提交信息，由 SinkCommitter 处理
     */
    Optional<CommitInfoT> prepareCommit(long checkpointId) throws IOException;

    /**
     * 保存状态快照
     */
    default List<StateT> snapshotState(long checkpointId) throws IOException;

    /**
     * 中止预提交（回滚）
     */
    void abortPrepare();

    /**
     * 关闭 Writer
     */
    void close() throws IOException;

    /**
     * Writer 上下文
     */
    interface Context extends Serializable {
        int getIndexOfSubtask();
        default int getNumberOfParallelSubtasks();
        MetricsContext getMetricsContext();
        EventListener getEventListener();
    }
}
```

### 3.4 SinkCommitter - 提交器

```java
// 文件: sink/SinkCommitter.java
// 职责: 提交 Writer 的预提交结果（第二阶段）

public interface SinkCommitter<CommitInfoT> extends AutoCloseable {

    /**
     * 初始化
     */
    default void init() throws IOException {}

    /**
     * 提交
     */
    List<CommitInfoT> commit(List<CommitInfoT> commitInfos) throws IOException;

    /**
     * 中止提交
     */
    void abort(List<CommitInfoT> commitInfos) throws IOException;

    @Override
    default void close() throws IOException {}
}
```

### 3.5 SaveMode - 保存模式

```java
// 文件: sink/SchemaSaveMode.java / DataSaveMode.java
// 职责: 定义写入时的行为

public enum SchemaSaveMode {
    RECREATE_SCHEMA,    // 重建表结构
    CREATE_SCHEMA_WHEN_NOT_EXIST,  // 不存在时创建
    ERROR_WHEN_SCHEMA_NOT_EXIST,   // 不存在时报错
    IGNORE              // 忽略
}

public enum DataSaveMode {
    DROP_DATA,          // 删除已有数据
    APPEND_DATA,        // 追加数据
    CUSTOM_PROCESSING,  // 自定义处理
    ERROR_WHEN_DATA_EXISTS  // 存在数据时报错
}
```

### 3.6 辅助接口

| 接口 | 功能 |
|------|------|
| `SupportSaveMode` | 支持保存模式 |
| `SupportMultiTableSink` | 支持多表写入 |
| `SupportSchemaEvolutionSink` | 支持 Schema 演进 |
| `SupportResourceShare` | 支持资源共享 |

---

## 四、Transform API - 数据转换接口

### 4.1 SeaTunnelTransform

```java
// 文件: transform/SeaTunnelTransform.java
// 职责: 数据转换接口

public interface SeaTunnelTransform<T>
        extends Serializable, PluginIdentifierInterface, SeaTunnelJobAware {

    /**
     * 初始化
     */
    default void open() {}

    /**
     * 获取输出的 CatalogTable
     */
    CatalogTable getProducedCatalogTable();

    /**
     * 获取输出的 CatalogTable 列表（多表）
     */
    List<CatalogTable> getProducedCatalogTables();

    /**
     * 映射 Schema 变更事件
     */
    default SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent);

    /**
     * 关闭
     */
    default void close() {}
}
```

**常用实现**:
- `FilterTransform`: 过滤转换
- `SqlTransform`: SQL 转换
- `CopyTransform`: 字段复制
- `ReplaceTransform`: 字段替换
- `SplitTransform`: 字段分割

---

## 五、Table API - 表结构定义

### 5.1 Factory - 工厂接口 (SPI)

```java
// 文件: table/factory/Factory.java
// 职责: 连接器工厂的基接口

public interface Factory {
    /**
     * 获取工厂标识符（连接器名称）
     */
    String factoryIdentifier();

    /**
     * 获取支持的配置选项
     */
    Set<Option<?>> optionRule();
}

// 文件: table/factory/TableSourceFactory.java
// 职责: Source 工厂（SPI 入口）

public interface TableSourceFactory extends Factory {
    /**
     * 创建 TableSource
     */
    default <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context);

    /**
     * 获取 Source 类
     */
    Class<? extends SeaTunnelSource> getSourceClass();
}

// 文件: table/factory/TableSinkFactory.java
// 职责: Sink 工厂（SPI 入口）

public interface TableSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
        extends Factory {
    /**
     * 创建 TableSink
     */
    default TableSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createSink(
            TableSinkFactoryContext context);
}
```

**SPI 注册**:
```
META-INF/services/org.apache.seatunnel.api.table.factory.Factory
```

### 5.2 Catalog - 元数据管理

```java
// 文件: table/catalog/Catalog.java
// 职责: 数据库/表元数据管理

public interface Catalog extends AutoCloseable {

    void open() throws CatalogException;
    void close() throws CatalogException;

    String name();
    String getDefaultDatabase() throws CatalogException;

    // 数据库操作
    boolean databaseExists(String databaseName) throws CatalogException;
    List<String> listDatabases() throws CatalogException;
    void createDatabase(TablePath tablePath, boolean ignoreIfExists);
    void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists);

    // 表操作
    boolean tableExists(TablePath tablePath) throws CatalogException;
    List<String> listTables(String databaseName) throws CatalogException;
    CatalogTable getTable(TablePath tablePath) throws CatalogException;
    void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists);
    void dropTable(TablePath tablePath, boolean ignoreIfNotExists);
    default void truncateTable(TablePath tablePath, boolean ignoreIfNotExists);
}
```

### 5.3 CatalogTable - 表定义

```java
// 文件: table/catalog/CatalogTable.java
// 职责: 表的完整定义

public interface CatalogTable extends Serializable {
    TableIdentifier getTableId();
    TableSchema getTableSchema();
    Map<String, String> getOptions();
    List<String> getPartitionKeys();
    String getComment();
    String getCatalogName();

    // 便捷方法
    default SeaTunnelRowType getSeaTunnelRowType() {
        return getTableSchema().toPhysicalRowDataType();
    }
}
```

### 5.4 TableSchema - 表结构

```java
// 文件: table/catalog/TableSchema.java
// 职责: 表的列定义

public class TableSchema implements Serializable {
    private final List<Column> columns;
    private final PrimaryKey primaryKey;
    private final List<ConstraintKey> constraintKeys;

    public SeaTunnelRowType toPhysicalRowDataType() { ... }

    public static Builder builder() { ... }

    public interface Builder {
        Builder column(Column column);
        Builder primaryKey(PrimaryKey primaryKey);
        Builder constraintKey(ConstraintKey constraintKey);
        TableSchema build();
    }
}
```

### 5.5 Column - 列定义

```java
// 文件: table/catalog/Column.java

public interface Column extends Serializable {
    String getName();
    SeaTunnelDataType<?> getDataType();
    Long getColumnLength();
    Integer getScale();
    boolean isNullable();
    Object getDefaultValue();
    String getComment();
}

// 实现类
public class PhysicalColumn implements Column { ... }  // 物理列
public class MetadataColumn implements Column { ... }  // 元数据列
```

---

## 六、数据类型系统

### 6.1 类型层次

```
SeaTunnelDataType<T> (接口)
├── BasicType<T>           # 基本类型
├── DecimalType            # 精确小数
├── LocalTimeType<T>       # 时间类型
├── PrimitiveByteArrayType # 字节数组
├── ArrayType<T>           # 数组
├── MapType<K,V>           # Map
├── SeaTunnelRowType       # Row（复合类型）
└── VectorType             # 向量类型
```

### 6.2 BasicType - 基本类型

```java
// 文件: table/type/BasicType.java

public class BasicType<T> implements SeaTunnelDataType<T> {
    public static final BasicType<String> STRING_TYPE =
            new BasicType<>(String.class, SqlType.STRING);
    public static final BasicType<Boolean> BOOLEAN_TYPE =
            new BasicType<>(Boolean.class, SqlType.BOOLEAN);
    public static final BasicType<Byte> BYTE_TYPE =
            new BasicType<>(Byte.class, SqlType.TINYINT);
    public static final BasicType<Short> SHORT_TYPE =
            new BasicType<>(Short.class, SqlType.SMALLINT);
    public static final BasicType<Integer> INT_TYPE =
            new BasicType<>(Integer.class, SqlType.INT);
    public static final BasicType<Long> LONG_TYPE =
            new BasicType<>(Long.class, SqlType.BIGINT);
    public static final BasicType<Float> FLOAT_TYPE =
            new BasicType<>(Float.class, SqlType.FLOAT);
    public static final BasicType<Double> DOUBLE_TYPE =
            new BasicType<>(Double.class, SqlType.DOUBLE);
    public static final BasicType<Void> VOID_TYPE =
            new BasicType<>(Void.class, SqlType.NULL);
}
```

### 6.3 SqlType - SQL 类型枚举

```java
// 文件: table/type/SqlType.java

public enum SqlType {
    NULL,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    STRING,
    BYTES,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMP_TZ,
    ARRAY,
    MAP,
    ROW,
    // 向量类型
    FLOAT_VECTOR,
    FLOAT16_VECTOR,
    BFLOAT16_VECTOR,
    BINARY_VECTOR,
    SPARSE_FLOAT_VECTOR
}
```

### 6.4 SeaTunnelRow - 行数据

```java
// 文件: table/type/SeaTunnelRow.java
// 职责: 核心数据传输对象

public final class SeaTunnelRow implements Serializable {
    private String tableId = "";          // 表标识
    private RowKind rowKind = RowKind.INSERT;  // 行类型（INSERT/UPDATE/DELETE）
    private final Object[] fields;        // 字段值数组
    private Map<String, Object> options;  // 扩展选项

    public SeaTunnelRow(int arity) {
        this.fields = new Object[arity];
    }

    public SeaTunnelRow(Object[] fields) {
        this.fields = fields;
    }

    public void setField(int pos, Object value);
    public Object getField(int pos);
    public int getArity();

    public void setTableId(String tableId);
    public String getTableId();

    public void setRowKind(RowKind rowKind);
    public RowKind getRowKind();

    public SeaTunnelRow copy();
    public SeaTunnelRow copy(int[] indexMapping);
    public int getBytesSize();
}
```

### 6.5 RowKind - 行类型

```java
// 文件: table/type/RowKind.java
// 职责: 描述 changelog 中的行变更类型

public enum RowKind {
    INSERT("+I", (byte) 0),       // 插入
    UPDATE_BEFORE("-U", (byte) 1), // 更新前（旧值）
    UPDATE_AFTER("+U", (byte) 2),  // 更新后（新值）
    DELETE("-D", (byte) 3);        // 删除

    public String shortString();
    public byte toByteValue();
    public static RowKind fromByteValue(byte value);
}
```

### 6.6 SeaTunnelRowType - 行类型定义

```java
// 文件: table/type/SeaTunnelRowType.java

public class SeaTunnelRowType implements CompositeType<SeaTunnelRow> {
    private final String[] fieldNames;
    private final SeaTunnelDataType<?>[] fieldTypes;

    public SeaTunnelRowType(String[] fieldNames, SeaTunnelDataType<?>[] fieldTypes);

    public String[] getFieldNames();
    public SeaTunnelDataType<?>[] getFieldTypes();
    public SeaTunnelDataType<?> getFieldType(int index);
    public int getTotalFields();
    public int indexOf(String fieldName);
}
```

### 6.7 类型映射表

| SeaTunnel 类型 | Java 类型 | SQL Type |
|----------------|-----------|----------|
| `STRING_TYPE` | `String` | STRING |
| `BOOLEAN_TYPE` | `Boolean` | BOOLEAN |
| `BYTE_TYPE` | `Byte` | TINYINT |
| `SHORT_TYPE` | `Short` | SMALLINT |
| `INT_TYPE` | `Integer` | INT |
| `LONG_TYPE` | `Long` | BIGINT |
| `FLOAT_TYPE` | `Float` | FLOAT |
| `DOUBLE_TYPE` | `Double` | DOUBLE |
| `DecimalType` | `BigDecimal` | DECIMAL |
| `LocalTimeType.LOCAL_DATE_TYPE` | `LocalDate` | DATE |
| `LocalTimeType.LOCAL_TIME_TYPE` | `LocalTime` | TIME |
| `LocalTimeType.LOCAL_DATE_TIME_TYPE` | `LocalDateTime` | TIMESTAMP |
| `PrimitiveByteArrayType` | `byte[]` | BYTES |
| `ArrayType<T>` | `T[]` | ARRAY |
| `MapType<K,V>` | `Map<K,V>` | MAP |
| `SeaTunnelRowType` | `SeaTunnelRow` | ROW |

---

## 七、配置系统

### 7.1 Option - 配置选项

```java
// 文件: configuration/Option.java

public class Option<T> {
    private final String key;
    private final Class<T> typeClass;
    private final T defaultValue;
    private final String description;

    public static <T> OptionBuilder<T> key(String key);
}

// 使用示例
public static final Option<String> HOST = Options.key("host")
        .stringType()
        .noDefaultValue()
        .withDescription("Database host");

public static final Option<Integer> PORT = Options.key("port")
        .intType()
        .defaultValue(3306)
        .withDescription("Database port");
```

### 7.2 ReadonlyConfig - 只读配置

```java
// 文件: configuration/ReadonlyConfig.java

public interface ReadonlyConfig extends Serializable {
    <T> T get(Option<T> option);
    <T> Optional<T> getOptional(Option<T> option);
    Map<String, String> toMap();
}
```

---

## 八、事件系统

### 8.1 事件类型

```java
// Source 事件
source/event/
├── ReaderOpenEvent.java      # Reader 打开
├── ReaderCloseEvent.java     # Reader 关闭
├── EnumeratorOpenEvent.java  # Enumerator 打开
├── EnumeratorCloseEvent.java # Enumerator 关闭
└── MessageDelayedEvent.java  # 消息延迟

// Sink 事件
sink/event/
└── WriterCloseEvent.java     # Writer 关闭

// Schema 变更事件
table/schema/event/
├── SchemaChangeEvent.java    # Schema 变更基类
├── AlterTableAddColumnEvent.java
├── AlterTableDropColumnEvent.java
├── AlterTableModifyColumnEvent.java
└── AlterTableChangeColumnEvent.java
```

### 8.2 EventListener

```java
// 文件: event/EventListener.java

public interface EventListener extends Serializable {
    void onEvent(Event event);
}
```

---

## 九、连接器开发指南

### 9.1 实现 Source 连接器

```java
// 1. 实现 TableSourceFactory（SPI 入口）
public class MySourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "MySource";
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> new MySource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MySource.class;
    }
}

// 2. 实现 SeaTunnelSource
public class MySource implements SeaTunnelSource<SeaTunnelRow, MySplit, MyState> {
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() { ... }

    @Override
    public SourceReader<SeaTunnelRow, MySplit> createReader(SourceReader.Context context) {
        return new MySourceReader(context);
    }

    @Override
    public SourceSplitEnumerator<MySplit, MyState> createEnumerator(
            SourceSplitEnumerator.Context<MySplit> context) {
        return new MySourceEnumerator(context);
    }
}

// 3. 实现 SourceSplitEnumerator
public class MySourceEnumerator
        implements SourceSplitEnumerator<MySplit, MyState> {
    @Override
    public void run() {
        // 生成分片并分配给 Reader
        context.assignSplit(subtaskId, splits);
    }
}

// 4. 实现 SourceReader
public class MySourceReader implements SourceReader<SeaTunnelRow, MySplit> {
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        // 读取数据并输出
        output.collect(row);
    }
}
```

### 9.2 实现 Sink 连接器

```java
// 1. 实现 TableSinkFactory（SPI 入口）
public class MySinkFactory implements TableSinkFactory<SeaTunnelRow, Void, Void, Void> {
    @Override
    public String factoryIdentifier() {
        return "MySink";
    }

    @Override
    public TableSink<SeaTunnelRow, Void, Void, Void> createSink(TableSinkFactoryContext context) {
        return () -> new MySink(context.getCatalogTable());
    }
}

// 2. 实现 SeaTunnelSink
public class MySink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {
    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
        return new MySinkWriter(context);
    }
}

// 3. 实现 SinkWriter
public class MySinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {
    @Override
    public void write(SeaTunnelRow element) throws IOException {
        // 写入数据
    }

    @Override
    public Optional<Void> prepareCommit(long checkpointId) {
        return Optional.empty();
    }
}
```

---

## 十、类图总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           seatunnel-api                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                          Source API                                 │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │  SeaTunnelSource<T, SplitT, StateT>                                │ │
│  │       ├── createReader() -> SourceReader                           │ │
│  │       └── createEnumerator() -> SourceSplitEnumerator              │ │
│  │                                                                     │ │
│  │  SourceSplitEnumerator         SourceReader                        │ │
│  │       ├── run()                     ├── pollNext()                 │ │
│  │       ├── snapshotState()           ├── addSplits()                │ │
│  │       └── handleSplitRequest()      └── snapshotState()            │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                           Sink API                                  │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │  SeaTunnelSink<IN, StateT, CommitInfoT, AggCommitInfoT>            │ │
│  │       ├── createWriter() -> SinkWriter                              │ │
│  │       ├── createCommitter() -> SinkCommitter                        │ │
│  │       └── createAggregatedCommitter() -> SinkAggregatedCommitter    │ │
│  │                                                                     │ │
│  │  SinkWriter                     SinkCommitter                       │ │
│  │       ├── write()                    └── commit()                   │ │
│  │       └── prepareCommit()                                           │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                         Table API                                   │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │  Factory (SPI)                                                      │ │
│  │       ├── TableSourceFactory                                        │ │
│  │       └── TableSinkFactory                                          │ │
│  │                                                                     │ │
│  │  Catalog                        CatalogTable                        │ │
│  │       ├── getTable()                 ├── TableSchema                │ │
│  │       ├── createTable()              │     └── Column               │ │
│  │       └── dropTable()                └── TableIdentifier            │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                      Data Type System                               │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │  SeaTunnelDataType<T>                                               │ │
│  │       ├── BasicType (STRING, INT, LONG, ...)                        │ │
│  │       ├── DecimalType                                               │ │
│  │       ├── LocalTimeType (DATE, TIME, TIMESTAMP)                     │ │
│  │       ├── ArrayType<T>                                              │ │
│  │       ├── MapType<K, V>                                             │ │
│  │       └── SeaTunnelRowType                                          │ │
│  │                                                                     │ │
│  │  SeaTunnelRow                   RowKind                             │ │
│  │       ├── fields[]                   ├── INSERT                     │ │
│  │       ├── tableId                    ├── UPDATE_BEFORE              │ │
│  │       └── rowKind                    ├── UPDATE_AFTER               │ │
│  │                                      └── DELETE                     │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 十一、总结

### 11.1 核心接口

| 接口 | 功能 | 运行位置 |
|------|------|----------|
| `SeaTunnelSource` | 数据源工厂 | 客户端 |
| `SourceSplitEnumerator` | 分片生成与分配 | Master |
| `SourceReader` | 数据读取 | Worker |
| `SeaTunnelSink` | 数据写入工厂 | 客户端 |
| `SinkWriter` | 数据写入 | Worker |
| `SinkCommitter` | 两阶段提交 | Worker/Master |
| `Catalog` | 元数据管理 | 客户端 |

### 11.2 设计特点

1. **SPI 机制**: 通过 `Factory` 接口实现插件发现
2. **分片模型**: Enumerator-Reader 分离，支持并行读取
3. **两阶段提交**: Writer-Committer 模式，支持 Exactly-Once
4. **类型系统**: 完整的数据类型抽象，支持复杂类型
5. **Changelog**: 通过 `RowKind` 支持 CDC 场景

### 11.3 扩展点

1. **新增连接器**: 实现 `TableSourceFactory`/`TableSinkFactory`
2. **新增转换**: 实现 `SeaTunnelTransform`
3. **自定义类型**: 扩展 `SeaTunnelDataType`

---

*文档结束*

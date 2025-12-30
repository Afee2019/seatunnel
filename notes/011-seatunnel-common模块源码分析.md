# SeaTunnel Common 模块源码分析

> 文档编号: 011
> 模块路径: seatunnel-common/
> 更新日期: 2025-12-29

---

## 一、模块概述

### 1.1 功能定位

`seatunnel-common` 是 SeaTunnel 的**公共基础模块**，提供了整个项目通用的：

- **常量定义**: 配置节名称、Logo、版权信息
- **异常体系**: 统一的错误码和异常类
- **工具类**: JSON、日期时间、文件、重试、反射等
- **配置工具**: HOCON 配置解析和校验
- **并发工具**: Handover 生产者-消费者模式

### 1.2 模块结构

```
seatunnel-common/
├── pom.xml
└── src/main/java/org/apache/seatunnel/common/
    ├── Constants.java                    # 全局常量
    ├── Handover.java                     # 生产者-消费者工具
    ├── config/                           # 配置相关
    │   ├── CheckConfigUtil.java          # 配置校验
    │   ├── CheckResult.java              # 校验结果
    │   ├── Common.java                   # 配置公共方法
    │   ├── ConfigRuntimeException.java   # 配置异常
    │   ├── DeployMode.java               # 部署模式枚举
    │   └── TypesafeConfigUtils.java      # HOCON 工具
    ├── constants/                        # 常量枚举
    │   ├── CollectionConstants.java      # 集合常量
    │   ├── EngineType.java               # 引擎类型
    │   ├── JobMode.java                  # 作业模式
    │   └── PluginType.java               # 插件类型
    ├── exception/                        # 异常体系
    │   ├── CommonError.java              # 错误工厂类
    │   ├── CommonErrorCode.java          # 错误码枚举
    │   ├── ExceptionParamsUtil.java      # 异常参数工具
    │   ├── SeaTunnelErrorCode.java       # 错误码接口
    │   └── SeaTunnelRuntimeException.java # 运行时异常
    └── utils/                            # 工具类
        ├── DateTimeUtils.java            # 日期时间工具
        ├── DateUtils.java                # 日期工具
        ├── EncodingUtils.java            # 编码工具
        ├── ExceptionUtils.java           # 异常工具
        ├── FileUtils.java                # 文件工具
        ├── JsonUtils.java                # JSON 工具
        ├── JdbcUrlUtil.java              # JDBC URL 解析
        ├── PlaceholderUtils.java         # 占位符替换
        ├── ReflectionUtils.java          # 反射工具
        ├── RetryUtils.java               # 重试工具
        ├── SerializationUtils.java       # 序列化工具
        ├── StringFormatUtils.java        # 字符串格式化
        ├── TemporaryClassLoaderContext.java # ClassLoader 上下文
        ├── TimeUtils.java                # 时间工具
        ├── VariablesSubstitute.java      # 变量替换
        ├── VectorUtils.java              # 向量工具
        └── function/                     # 函数式接口
            ├── ConsumerWithException.java
            ├── FunctionWithException.java
            ├── RunnableWithException.java
            └── SupplierWithException.java
```

### 1.3 模块统计

| 包 | Java 文件数 | 主要功能 |
|-----|-------------|----------|
| root | 2 | Constants, Handover |
| config | 6 | 配置校验和解析 |
| constants | 4 | 枚举常量 |
| exception | 6 | 异常体系 |
| utils | 17 | 工具类 |
| utils/function | 4 | 函数式接口 |
| **总计** | **39** | - |

### 1.4 依赖关系

```xml
<dependencies>
    <!-- HOCON 配置解析（Shaded） -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-config-shade</artifactId>
    </dependency>

    <!-- Apache Commons（Shaded） -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-commons-lang3</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-collections4</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-csv</artifactId>
    </dependency>

    <!-- Guava（Shaded） -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-guava</artifactId>
    </dependency>

    <!-- Jackson（Shaded） -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-jackson</artifactId>
    </dependency>

    <!-- Arrow（向量计算） -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-arrow</artifactId>
    </dependency>
</dependencies>
```

---

## 二、全局常量

### 2.1 Constants

**路径**: `common/Constants.java`

```java
public final class Constants {
    // 配置节名称
    public static final String ENV = "env";
    public static final String SOURCE = "source";
    public static final String TRANSFORM = "transform";
    public static final String SINK = "sink";

    // 序列化配置
    public static final String SOURCE_SERIALIZATION = "source.serialization";
    public static final String SINK_SERIALIZATION = "sink.serialization";

    // HDFS 配置
    public static final String HDFS_ROOT = "hdfs.root";
    public static final String HDFS_USER = "hdfs.user";

    // Checkpoint
    public static final String CHECKPOINT_ID = "checkpoint.id";

    // 变量占位符
    public static final String UUID = "uuid";
    public static final String NOW = "now";

    // Logo
    public static final String ST_LOGO =
            " _____               _____                             _ \n"
          + "/  ___|             |_   _|                           | |\n"
          + "\\ `--.   ___   __ _   | |   _   _  _ __   _ __    ___ | |\n"
          + " `--. \\ / _ \\ / _` |  | |  | | | || '_ \\ | '_ \\  / _ \\| |\n"
          + "/\\__/ /|  __/| (_| |  | |  | |_| || | | || | | ||  __/| |\n"
          + "\\____/  \\___| \\__,_|  \\_/   \\__,_||_| |_||_| |_| \\___||_|\n";

    public static final String COPYRIGHT_LINE =
            "Copyright © 2021-2024 The Apache Software Foundation...";
}
```

### 2.2 常量枚举

#### EngineType - 引擎类型

```java
public enum EngineType {
    SEATUNNEL,  // Zeta 引擎
    SPARK,      // Spark 引擎
    FLINK       // Flink 引擎
}
```

#### JobMode - 作业模式

```java
public enum JobMode {
    BATCH,      // 批处理
    STREAMING   // 流处理
}
```

#### PluginType - 插件类型

```java
public enum PluginType {
    SOURCE("source"),
    TRANSFORM("transform"),
    SINK("sink");

    private final String type;
}
```

#### DeployMode - 部署模式

```java
public enum DeployMode {
    CLIENT,     // 客户端模式
    CLUSTER,    // 集群模式
    LOCAL,      // 本地模式
    RUN,        // 运行模式
    RUN_APPLICATION  // Application 模式
}
```

---

## 三、异常体系

### 3.1 设计理念

SeaTunnel 采用**结构化异常**设计：

- **错误码**: 唯一标识错误类型
- **错误模板**: 带占位符的错误消息
- **参数化**: 运行时填充具体信息

### 3.2 SeaTunnelErrorCode 接口

**路径**: `exception/SeaTunnelErrorCode.java`

```java
public interface SeaTunnelErrorCode {
    // 错误码（如 "COMMON-01"）
    String getCode();

    // 错误消息模板
    String getErrorMessage();

    // 错误描述（可选，用于详细说明）
    String getDescription();
}
```

### 3.3 CommonErrorCode 枚举

**路径**: `exception/CommonErrorCode.java`

```java
public enum CommonErrorCode implements SeaTunnelErrorCode {

    FILE_OPERATION_FAILED(
            "COMMON-01",
            "<identifier> <operation> file '<fileName>' failed.",
            "<identifier> <operation> file '<fileName>' failed."),

    UNSUPPORTED_DATA_TYPE(
            "COMMON-02",
            "'<identifier>' unsupported data type '<dataType>' for field '<field>'",
            ...),

    JSON_OPERATION_FAILED(
            "COMMON-03",
            "'<identifier>' json operation failed with '<payload>'",
            ...),

    CONVERT_TO_SEATUNNEL_TYPE_ERROR(
            "COMMON-04",
            "'<connector>' <type> unsupported convert '<dataType>' to SeaTunnel type for field '<field>'",
            ...),

    WRITE_SEATUNNEL_ROW_ERROR(
            "COMMON-06",
            "'<connector>' write SeaTunnelRow error '<seaTunnelRow>'",
            ...),

    // ... 更多错误码
    ;

    private final String code;
    private final String errorMessage;
    private final String description;
}
```

### 3.4 SeaTunnelRuntimeException

**路径**: `exception/SeaTunnelRuntimeException.java`

**特点**:
- 继承 `RuntimeException`（非检查异常）
- 包含错误码和参数化信息
- 支持参数提取

```java
public class SeaTunnelRuntimeException extends RuntimeException {

    private final SeaTunnelErrorCode seaTunnelErrorCode;
    private final Map<String, String> params;

    // 带参数的构造函数
    public SeaTunnelRuntimeException(
            SeaTunnelErrorCode seaTunnelErrorCode, Map<String, String> params) {
        super(ExceptionParamsUtil.getDescription(seaTunnelErrorCode.getErrorMessage(), params));
        this.seaTunnelErrorCode = seaTunnelErrorCode;
        this.params = params;
    }

    // 获取参数值并解析为 Map
    public Map<String, String> getParamsValueAsMap(String key) {
        return OBJECT_MAPPER.readValue(params.get(key), new TypeReference<Map<String, String>>() {});
    }
}
```

### 3.5 CommonError 工厂类

**路径**: `exception/CommonError.java`

**职责**: 提供统一的异常创建方法

```java
public class CommonError {

    // 文件操作失败
    public static SeaTunnelRuntimeException fileOperationFailed(
            String identifier, String operation, String fileName, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("operation", operation);
        params.put("fileName", fileName);
        return new SeaTunnelRuntimeException(FILE_OPERATION_FAILED, params, cause);
    }

    // 不支持的数据类型
    public static SeaTunnelRuntimeException unsupportedDataType(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(UNSUPPORTED_DATA_TYPE, params);
    }

    // 类型转换错误
    public static SeaTunnelRuntimeException convertToSeaTunnelTypeError(
            String connector, PluginType pluginType, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("type", pluginType.getType());
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_SEATUNNEL_TYPE_ERROR, params);
    }

    // JSON 操作失败
    public static SeaTunnelRuntimeException jsonOperationError(
            String identifier, String payload, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("payload", payload);
        return new SeaTunnelRuntimeException(JSON_OPERATION_FAILED, params, cause);
    }

    // ... 更多工厂方法
}
```

**使用示例**:
```java
// 抛出文件操作异常
throw CommonError.fileOperationFailed("JDBC", "read", "/path/to/file", e);
// 输出: JDBC read file '/path/to/file' failed.

// 抛出类型转换异常
throw CommonError.convertToSeaTunnelTypeError("MySQL", PluginType.SOURCE, "GEOMETRY", "location");
// 输出: 'MySQL' source unsupported convert 'GEOMETRY' to SeaTunnel type for field 'location'
```

---

## 四、工具类详解

### 4.1 JsonUtils

**路径**: `utils/JsonUtils.java`

**功能**: 基于 Jackson 的 JSON 序列化/反序列化工具

```java
public class JsonUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)      // 忽略未知属性
            .configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true) // 空数组转 null
            .configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)  // 未知枚举转 null
            .configure(REQUIRE_SETTERS_FOR_GETTERS, true)
            .setTimeZone(TimeZone.getDefault())
            .registerModule(new JavaTimeModule());              // 支持 Java 8 时间 API

    // JSON 字符串 → 对象
    public static <T> T parseObject(String json, Class<T> clazz);

    // JSON 字符串 → 泛型对象
    public static <T> T parseObject(String json, TypeReference<T> type);

    // JSON 字符串 → List
    public static <T> List<T> toList(String json, Class<T> clazz);

    // JSON 字符串 → Map
    public static Map<String, String> toMap(String json);
    public static <K, V> Map<K, V> toMap(String json, Class<K> classK, Class<V> classV);

    // 对象 → JSON 字符串
    public static String toJsonString(Object object);

    // 创建节点
    public static ArrayNode createArrayNode();
    public static ObjectNode createObjectNode();

    // 判断是否为 JSON 数组
    public static boolean isJsonArray(String jsonString);
}
```

### 4.2 RetryUtils

**路径**: `utils/RetryUtils.java`

**功能**: 可配置的重试执行器

```java
public class RetryUtils {

    public static <T> T retryWithException(
            Execution<T, Exception> execution,
            RetryMaterial retryMaterial) throws Exception {

        int i = 0;
        Exception lastException;

        do {
            i++;
            try {
                return execution.execute();
            } catch (Exception e) {
                lastException = e;
                // 检查是否可重试
                if (retryCondition != null && !retryCondition.canRetry(e)) {
                    if (retryMaterial.shouldThrowException()) {
                        throw e;
                    }
                } else {
                    // 计算退避时间并等待
                    long backoff = retryMaterial.computeRetryWaitTimeMillis(i);
                    Thread.sleep(backoff);
                }
            }
        } while (i < retryMaterial.getRetryTimes());

        if (retryMaterial.shouldThrowException()) {
            throw new RuntimeException("Execute failed after retry " + retryTimes + " times", lastException);
        }
        return null;
    }

    // 重试材料（配置）
    public static class RetryMaterial {
        private final int retryTimes;              // 重试次数
        private final boolean shouldThrowException; // 是否抛出异常
        private final RetryCondition<Exception> retryCondition; // 重试条件
        private final long sleepTimeMillis;        // 基础等待时间
        private final boolean sleepTimeIncrease;   // 是否指数退避

        // 计算退避时间（指数退避）
        public long computeRetryWaitTimeMillis(int retryAttempts) {
            if (!sleepTimeIncrease) {
                return sleepTimeMillis;
            }
            // 指数退避: sleepTime * 2^retryAttempts
            long result = sleepTimeMillis << retryAttempts;
            return Math.min(MAX_RETRY_TIME_MS, result);
        }
    }

    // 执行接口
    @FunctionalInterface
    public interface Execution<T, E extends Exception> {
        T execute() throws E;
    }

    // 重试条件
    public interface RetryCondition<T> {
        boolean canRetry(T input);
    }
}
```

**使用示例**:
```java
// 重试 3 次，每次间隔 1 秒，指数退避
RetryMaterial material = new RetryMaterial(
    3,                           // 重试次数
    true,                        // 失败后抛出异常
    e -> e instanceof IOException, // 仅 IO 异常可重试
    1000,                        // 基础等待 1 秒
    true                         // 启用指数退避
);

String result = RetryUtils.retryWithException(() -> {
    return httpClient.get(url);
}, material);
```

### 4.3 DateTimeUtils

**路径**: `utils/DateTimeUtils.java`

**功能**: 日期时间解析和格式化

```java
public class DateTimeUtils {

    // 预定义格式
    public enum Formatter {
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SSSSSS("yyyy-MM-dd HH:mm:ss.SSSSSS"),
        YYYY_MM_DD_HH_MM_SS_ISO8601("yyyy-MM-dd'T'HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_NO_SPLIT("yyyyMMddHHmmss"),
        // ... 更多格式
    }

    // 自动匹配格式解析
    public static LocalDateTime parse(String dateTime) {
        DateTimeFormatter formatter = matchDateTimeFormatter(dateTime);
        return LocalDateTime.parse(dateTime, formatter);
    }

    // 指定格式解析
    public static LocalDateTime parse(String dateTime, Formatter formatter);

    // 时间戳解析
    public static LocalDateTime parse(long timestamp);
    public static LocalDateTime parse(long timestamp, ZoneId zoneId);

    // 格式化输出
    public static String toString(LocalDateTime dateTime, Formatter formatter);
    public static String toString(long timestamp, Formatter formatter);

    // 自动匹配 DateTimeFormatter
    public static DateTimeFormatter matchDateTimeFormatter(String dateTime) {
        if (dateTime.length() == 19) {
            // 匹配 "yyyy-MM-dd HH:mm:ss" 等 19 位格式
            for (Map.Entry<Pattern, DateTimeFormatter> entry : FORMATTER_MAP_19.entrySet()) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
        } else if (dateTime.length() > 19) {
            // 匹配带毫秒的格式
            // ...
        }
        return null;
    }
}
```

**支持的格式**:

| 长度 | 格式示例 | Pattern |
|------|----------|---------|
| 14 | 20201231235959 | yyyyMMddHHmmss |
| 19 | 2020-12-31 23:59:59 | yyyy-MM-dd HH:mm:ss |
| 19 | 2020/12/31 23:59:59 | yyyy/MM/dd HH:mm:ss |
| 19 | 2020-12-31T23:59:59 | yyyy-MM-dd'T'HH:mm:ss |
| >19 | 2020-12-31 23:59:59.123456 | 自动检测 |

### 4.4 FileUtils

**路径**: `utils/FileUtils.java`

**功能**: 文件操作工具

```java
public class FileUtils {

    // 搜索 JAR 文件
    public static List<URL> searchJarFiles(@NonNull Path directory) throws IOException;

    // 读取文件内容
    public static String readFileToStr(Path path);

    // 写入字符串到文件
    public static void writeStringToFile(String filePath, String str);

    // 创建新文件（删除旧文件）
    public static void createNewFile(String filePath) throws IOException;

    // 创建新目录（清空已存在目录）
    public static void createNewDir(@NonNull String dirPath);

    // 删除文件/目录
    public static void deleteFile(@NonNull String filePath);

    // 获取文件行数
    public static Long getFileLineNumber(@NonNull String filePath);

    // 获取目录下所有文件行数
    public static Long getFileLineNumberFromDir(@NonNull String dirPath);

    // 列出目录下的文件
    public static List<File> listFile(String dirPath);

    // 检查文件是否存在
    public static boolean isFileExist(String filePath);

    // 创建父目录
    public static void createParentFile(File file);
}
```

### 4.5 Handover

**路径**: `common/Handover.java`

**功能**: 线程安全的生产者-消费者队列

```java
public final class Handover<T> implements Closeable {

    private static final int DEFAULT_QUEUE_SIZE = 10000;
    private final Object lock = new Object();
    private final LinkedBlockingQueue<T> blockingQueue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);
    private Throwable error;

    // 检查队列是否为空
    public boolean isEmpty() throws Exception {
        if (error != null) {
            rethrowException(error, error.getMessage());
        }
        return blockingQueue.isEmpty();
    }

    // 消费者：获取下一个元素
    public Optional<T> pollNext() throws Exception {
        if (error != null) {
            rethrowException(error, error.getMessage());
        } else if (!isEmpty()) {
            return Optional.ofNullable(blockingQueue.poll());
        }
        return Optional.empty();
    }

    // 生产者：放入元素
    public void produce(final T element) throws InterruptedException, ClosedException {
        if (error != null) {
            throw new ClosedException();
        }
        blockingQueue.put(element);  // 阻塞等待队列有空间
    }

    // 报告错误
    public void reportError(Throwable t) {
        synchronized (lock) {
            if (error == null) {
                error = t;
            }
            lock.notifyAll();
        }
    }

    // 关闭
    @Override
    public void close() {
        synchronized (lock) {
            if (error == null) {
                error = new ClosedException();
            }
            lock.notifyAll();
        }
    }
}
```

**使用场景**:
- SourceReader 与 Enumerator 之间的数据传递
- 异步任务结果收集

---

## 五、配置工具

### 5.1 CheckConfigUtil

**路径**: `config/CheckConfigUtil.java`

**功能**: 配置项校验

```java
public class CheckConfigUtil {

    // 检查必需的配置项
    public static CheckResult checkAllExists(Config config, String... configNames) {
        for (String configName : configNames) {
            if (!config.hasPath(configName)) {
                return CheckResult.error("Missing config: " + configName);
            }
        }
        return CheckResult.success();
    }

    // 检查至少存在一个配置项
    public static CheckResult checkAtLeastOneExists(Config config, String... configNames) {
        for (String configName : configNames) {
            if (config.hasPath(configName)) {
                return CheckResult.success();
            }
        }
        return CheckResult.error("At least one of " + Arrays.toString(configNames) + " required");
    }
}
```

### 5.2 TypesafeConfigUtils

**路径**: `config/TypesafeConfigUtils.java`

**功能**: HOCON 配置解析工具

```java
public class TypesafeConfigUtils {

    // 安全获取配置值
    public static String getConfig(Config config, String key, String defaultValue) {
        return config.hasPath(key) ? config.getString(key) : defaultValue;
    }

    // 获取嵌套配置
    public static Config getSubConfig(Config config, String key) {
        return config.hasPath(key) ? config.getConfig(key) : ConfigFactory.empty();
    }

    // 配置转 Map
    public static Map<String, String> configToMap(Config config) {
        Map<String, String> map = new HashMap<>();
        config.entrySet().forEach(entry ->
            map.put(entry.getKey(), entry.getValue().unwrapped().toString())
        );
        return map;
    }
}
```

---

## 六、函数式接口

### 6.1 带异常的函数式接口

**路径**: `utils/function/`

```java
// 带异常的 Supplier
@FunctionalInterface
public interface SupplierWithException<T, E extends Exception> {
    T get() throws E;
}

// 带异常的 Consumer
@FunctionalInterface
public interface ConsumerWithException<T, E extends Exception> {
    void accept(T t) throws E;
}

// 带异常的 Function
@FunctionalInterface
public interface FunctionWithException<T, R, E extends Exception> {
    R apply(T t) throws E;
}

// 带异常的 Runnable
@FunctionalInterface
public interface RunnableWithException<E extends Exception> {
    void run() throws E;
}
```

**使用示例**:
```java
// 在 Lambda 中抛出检查异常
SupplierWithException<Connection, SQLException> supplier = () -> {
    return DriverManager.getConnection(url, user, password);
};

try {
    Connection conn = supplier.get();
} catch (SQLException e) {
    // 处理异常
}
```

---

## 七、其他工具类

### 7.1 TemporaryClassLoaderContext

**路径**: `utils/TemporaryClassLoaderContext.java`

**功能**: 临时切换线程上下文 ClassLoader

```java
public class TemporaryClassLoaderContext implements AutoCloseable {

    private final ClassLoader originalClassLoader;

    public TemporaryClassLoaderContext(ClassLoader newClassLoader) {
        this.originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newClassLoader);
    }

    @Override
    public void close() {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
}
```

**使用示例**:
```java
try (TemporaryClassLoaderContext ignored = new TemporaryClassLoaderContext(pluginClassLoader)) {
    // 在这里使用插件 ClassLoader
    Class<?> clazz = Class.forName("com.example.Plugin");
}
// 自动恢复原 ClassLoader
```

### 7.2 ExceptionUtils

**路径**: `utils/ExceptionUtils.java`

**功能**: 异常处理工具

```java
public class ExceptionUtils {

    // 获取异常消息
    public static String getMessage(Throwable e) {
        return e.getMessage() != null ? e.getMessage() : e.getClass().getName();
    }

    // 获取根因异常
    public static Throwable getRootCause(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    // 获取完整堆栈
    public static String getStackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
```

### 7.3 SerializationUtils

**路径**: `utils/SerializationUtils.java`

**功能**: Java 序列化工具

```java
public class SerializationUtils {

    // 对象序列化为字节数组
    public static byte[] serialize(Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            return baos.toByteArray();
        }
    }

    // 字节数组反序列化为对象
    public static <T> T deserialize(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        }
    }
}
```

---

## 八、设计模式总结

### 8.1 使用的设计模式

| 模式 | 应用场景 |
|------|----------|
| **工厂方法模式** | CommonError 工厂类创建异常 |
| **单例模式** | JsonUtils 中的 ObjectMapper |
| **生产者-消费者模式** | Handover 类 |
| **模板方法模式** | RetryUtils.Execution 接口 |
| **策略模式** | RetryCondition 重试条件 |

### 8.2 代码规范

1. **Shaded 依赖**: 使用 `org.apache.seatunnel.shade.*` 避免依赖冲突
2. **工具类私有构造**: 防止实例化
3. **参数校验**: 使用 `@NonNull` 注解
4. **异常链**: 保留原始异常信息

---

## 九、总结

`seatunnel-common` 模块是 SeaTunnel 的基础设施层：

1. **统一异常体系**: 错误码 + 参数化消息，便于问题定位
2. **丰富工具类**: JSON、日期、文件、重试等常用功能
3. **配置工具**: HOCON 配置解析和校验
4. **并发工具**: Handover 生产者-消费者模式
5. **依赖隔离**: 使用 Shaded 依赖避免冲突

---

**相关文档**:
- [005-seatunnel-api模块源码分析.md](./005-seatunnel-api模块源码分析.md) - API 层对 Common 的使用
- [006-seatunnel-connectors-v2模块源码分析.md](./006-seatunnel-connectors-v2模块源码分析.md) - 连接器中的错误处理

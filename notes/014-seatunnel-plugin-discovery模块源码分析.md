# SeaTunnel Plugin Discovery 模块源码分析
> 文档编号: 014
> 模块路径: seatunnel-plugin-discovery/
> 更新日期: 2025-12-29
---

## 一、模块概述

### 1.1 模块定位

`seatunnel-plugin-discovery` 模块是 SeaTunnel 的**插件发现与加载模块**，负责：

1. **插件发现** - 根据配置文件中的插件名称找到对应的 JAR 包
2. **插件加载** - 通过 SPI 机制动态加载插件实例
3. **依赖管理** - 管理插件的依赖 JAR 包
4. **配置校验** - 获取插件的配置选项规则

### 1.2 模块结构

```
seatunnel-plugin-discovery/
├── pom.xml
└── src/
    ├── main/java/org/apache/seatunnel/plugin/discovery/
    │   ├── PluginDiscovery.java                     # 插件发现接口
    │   ├── AbstractPluginDiscovery.java             # 抽象基类（核心实现）
    │   └── seatunnel/
    │       ├── SeaTunnelSourcePluginDiscovery.java  # Source 插件发现
    │       ├── SeaTunnelSinkPluginDiscovery.java    # Sink 插件发现
    │       ├── SeaTunnelTransformPluginDiscovery.java # Transform 插件发现
    │       └── SeaTunnelFactoryDiscovery.java       # Factory 发现
    └── test/java/...                                # 单元测试
```

### 1.3 依赖关系

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-api</artifactId>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-common</artifactId>
    </dependency>
</dependencies>
```

### 1.4 文件统计

| 类型 | 文件数 | 说明 |
|------|--------|------|
| 接口 | 1 | PluginDiscovery |
| 抽象类 | 1 | AbstractPluginDiscovery |
| 实现类 | 4 | Source/Sink/Transform/Factory Discovery |
| 测试类 | 2 | 单元测试 |
| **合计** | **8** | |

---

## 二、核心接口：PluginDiscovery

### 2.1 接口定义

```java
public interface PluginDiscovery<T> {

    /**
     * 获取插件 JAR 包路径
     */
    List<URL> getPluginJarPaths(List<PluginIdentifier> pluginIdentifiers);

    /**
     * 获取插件 JAR 及其依赖的路径
     */
    List<URL> getPluginJarAndDependencyPaths(List<PluginIdentifier> pluginIdentifiers);

    /**
     * 根据插件标识符创建插件实例
     * @throws IllegalArgumentException 如果未找到插件
     */
    T createPluginInstance(PluginIdentifier pluginIdentifier);

    /**
     * 使用额外的 JAR 包创建插件实例
     */
    T createPluginInstance(PluginIdentifier pluginIdentifier, Collection<URL> pluginJars);

    /**
     * 创建可选的插件实例（未找到返回 Optional.empty()）
     */
    Optional<T> createOptionalPluginInstance(PluginIdentifier pluginIdentifier);

    /**
     * 获取所有插件实例
     */
    List<T> getAllPlugins(List<PluginIdentifier> pluginIdentifiers);

    /**
     * 获取所有插件及其配置规则
     */
    default LinkedHashMap<PluginIdentifier, OptionRule> getPlugins() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * 获取指定插件的配置选项
     * @return (插件标识符, 必需选项, 可选选项)
     */
    default ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> getOptionRules(
            String pluginIdentifier) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
```

### 2.2 PluginIdentifier

插件标识符由三部分组成：

```java
public class PluginIdentifier {
    private final String engineType;   // 引擎类型：seatunnel
    private final String pluginType;   // 插件类型：source/sink/transform
    private final String pluginName;   // 插件名称：Jdbc/Kafka/Console 等

    public static PluginIdentifier of(String engineType, String pluginType, String pluginName) {
        return new PluginIdentifier(engineType, pluginType, pluginName);
    }
}
```

**示例**：
```java
// JDBC Source 插件
PluginIdentifier.of("seatunnel", "source", "Jdbc")

// Kafka Sink 插件
PluginIdentifier.of("seatunnel", "sink", "Kafka")

// SQL Transform 插件
PluginIdentifier.of("seatunnel", "transform", "Sql")
```

---

## 三、核心实现：AbstractPluginDiscovery

### 3.1 类结构

```java
@Slf4j
public abstract class AbstractPluginDiscovery<T> implements PluginDiscovery<T> {

    // 插件映射配置文件名
    private static final String PLUGIN_MAPPING_FILE = "plugin-mapping.properties";

    // 插件目录
    private final Path pluginDir;

    // 插件映射配置（plugin-mapping.properties 内容）
    private final Config pluginMappingConfig;

    // 添加 URL 到 ClassLoader 的策略
    private final BiConsumer<ClassLoader, List<URL>> addURLToClassLoaderConsumer;

    // 插件 JAR 路径缓存
    protected final ConcurrentHashMap<PluginIdentifier, Optional<List<URL>>> pluginJarPath;

    // 各类型插件实例映射
    protected final Map<PluginIdentifier, String> sourcePluginInstance;
    protected final Map<PluginIdentifier, String> sinkPluginInstance;
    protected final Map<PluginIdentifier, String> transformPluginInstance;

    // 子类必须实现：返回插件基类
    protected abstract Class<T> getPluginBaseClass();
}
```

### 3.2 构造函数

```java
// 默认构造函数：使用默认连接器目录
public AbstractPluginDiscovery() {
    this(Common.connectorDir(), loadConnectorPluginConfig());
}

// 指定插件目录
public AbstractPluginDiscovery(Path pluginDir) {
    this(pluginDir, loadConnectorPluginConfig());
}

// 完整构造函数
public AbstractPluginDiscovery(
        Path pluginDir,
        Config pluginMappingConfig,
        BiConsumer<ClassLoader, List<URL>> addURLToClassLoaderConsumer) {
    this.pluginDir = pluginDir;
    this.pluginMappingConfig = pluginMappingConfig;
    this.addURLToClassLoaderConsumer = addURLToClassLoaderConsumer;

    // 初始化各类型插件映射
    this.sourcePluginInstance = getAllSupportedPlugins(PluginType.SOURCE);
    this.sinkPluginInstance = getAllSupportedPlugins(PluginType.SINK);
    this.transformPluginInstance = getAllSupportedPlugins(PluginType.TRANSFORM);

    log.info("Load {} Plugin from {}", getPluginBaseClass().getSimpleName(), pluginDir);
}
```

### 3.3 插件映射配置加载

```java
// 加载 plugin-mapping.properties 配置
protected static Config loadConnectorPluginConfig() {
    return ConfigFactory.parseFile(
            Common.connectorDir().resolve(PLUGIN_MAPPING_FILE).toFile())
        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
}

// 获取所有支持的插件
public static Map<PluginIdentifier, String> getAllSupportedPlugins(PluginType pluginType) {
    Config config = loadConnectorPluginConfig();
    Map<PluginIdentifier, String> pluginIdentifiers = new HashMap<>();

    if (config.isEmpty() || !config.hasPath(CollectionConstants.SEATUNNEL_PLUGIN)) {
        return pluginIdentifiers;
    }

    Config engineConfig = config.getConfig(CollectionConstants.SEATUNNEL_PLUGIN);
    if (engineConfig.hasPath(pluginType.getType())) {
        engineConfig.getConfig(pluginType.getType())
            .entrySet()
            .forEach(entry -> {
                pluginIdentifiers.put(
                    PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        pluginType.getType(),
                        entry.getKey()),
                    entry.getValue().unwrapped().toString());
            });
    }
    return pluginIdentifiers;
}
```

### 3.4 插件实例创建

```java
@Override
public Optional<T> createOptionalPluginInstance(
        PluginIdentifier pluginIdentifier, Collection<URL> pluginJars) {

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    // 1. 首先尝试从 classpath 加载
    T pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
    if (pluginInstance != null) {
        log.info("Load plugin: {} from classpath", pluginIdentifier);
        return Optional.of(pluginInstance);
    }

    // 2. 如果 classpath 没有，从插件目录加载
    Optional<List<URL>> pluginJarPaths = getPluginJarPath(pluginIdentifier);
    if (pluginJarPaths.isPresent()) {
        try {
            // 尝试将 JAR 添加到当前 ClassLoader
            addURLToClassLoaderConsumer.accept(classLoader, pluginJarPaths.get());
            addURLToClassLoaderConsumer.accept(classLoader, (List<URL>) pluginJars);
        } catch (Exception e) {
            // 如果失败，创建新的 URLClassLoader
            log.warn("Can't load jar use current thread classloader, "
                   + "use URLClassLoader instead now.");
            URL[] urls = Stream.concat(
                    pluginJars.stream(),
                    pluginJarPaths.get().stream())
                .distinct()
                .toArray(URL[]::new);
            classLoader = new URLClassLoader(urls,
                Thread.currentThread().getContextClassLoader());
        }

        pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
        if (pluginInstance != null) {
            log.info("Load plugin: {} from path: {} use classloader: {}",
                    pluginIdentifier, pluginJarPaths.get(),
                    classLoader.getClass().getName());
            return Optional.of(pluginInstance);
        }
    }

    return Optional.empty();
}
```

### 3.5 SPI 插件加载

```java
protected T loadPluginInstance(PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
    // 使用 Java SPI 机制加载插件
    ServiceLoader<T> serviceLoader = ServiceLoader.load(getPluginBaseClass(), classLoader);

    for (T t : serviceLoader) {
        if (t instanceof PluginIdentifierInterface) {
            // 新版 API：通过 PluginIdentifierInterface 匹配
            PluginIdentifierInterface pluginIdentifierInstance = (PluginIdentifierInterface) t;
            if (StringUtils.equalsIgnoreCase(
                    pluginIdentifierInstance.getPluginName(),
                    pluginIdentifier.getPluginName())) {
                return (T) pluginIdentifierInstance;
            }
        } else {
            throw new UnsupportedOperationException(
                "Plugin instance: " + t + " is not supported.");
        }
    }
    return null;
}
```

### 3.6 插件 JAR 路径查找

```java
private Optional<List<URL>> findPluginJarPath(PluginIdentifier pluginIdentifier) {
    // 1. 获取插件映射前缀（如 connector-jdbc）
    Optional<String> pluginPrefix = getPluginMappingPrefix(pluginIdentifier);
    if (!pluginPrefix.isPresent()) {
        return Optional.empty();
    }

    final String pluginName = pluginIdentifier.getPluginName().toLowerCase();

    // 2. 在插件目录中查找匹配的 JAR 文件
    File[] targetPluginFiles = pluginDir.toFile().listFiles(
        pathname -> filterPluginJar(pathname, pluginPrefix.get(), pluginName));

    if (ArrayUtils.isEmpty(targetPluginFiles)) {
        return Optional.empty();
    }

    // 3. 返回 JAR 路径
    PluginType type = PluginType.valueOf(
        pluginIdentifier.getPluginType().toUpperCase());
    List<URL> pluginJarPaths;

    if (targetPluginFiles.length == 1) {
        pluginJarPaths = Collections.singletonList(
            targetPluginFiles[0].toURI().toURL());
    } else {
        pluginJarPaths = selectPluginJar(
            targetPluginFiles, pluginPrefix.get(), pluginName, type).get();
    }

    log.info("Discovery plugin jar for: {} at: {}", pluginIdentifier, pluginJarPaths);
    return Optional.of(pluginJarPaths);
}

// 过滤插件 JAR 文件
private boolean filterPluginJar(File pathname, String pluginJarPrefix, String pluginName) {
    // CDC 插件需要额外加载 connector-cdc-base
    if (pluginName.contains("cdc")) {
        return pathname.getName().endsWith(".jar")
            && (StringUtils.startsWithIgnoreCase(pathname.getName(), pluginJarPrefix)
                || StringUtils.startsWithIgnoreCase(pathname.getName(), "connector-cdc-base"));
    }
    return pathname.getName().endsWith(".jar")
        && StringUtils.startsWithIgnoreCase(pathname.getName(), pluginJarPrefix);
}
```

### 3.7 获取插件依赖

```java
private List<URL> getPluginDependencyJarPaths(PluginIdentifier pluginIdentifier)
        throws IOException {
    Optional<String> pluginPrefix = getPluginMappingPrefix(pluginIdentifier);
    if (!pluginPrefix.isPresent()) {
        return Collections.emptyList();
    }

    List<URL> jars = new ArrayList<>();
    Path pluginRootDir = Common.pluginRootDir();

    if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
        return new ArrayList<>();
    }

    for (File file : pluginRootDir.toFile().listFiles()) {
        // 只读取当前连接器依赖和公共依赖
        if (file.isDirectory()
                && (!file.getName().startsWith("connector-")
                    || file.getName().equalsIgnoreCase(pluginPrefix.get()))) {
            jars.addAll(FileUtils.searchJarFiles(
                Paths.get(Common.pluginRootDir().toString(), file.getName())));
        } else if (!file.isDirectory()) {
            jars.add(file.toURI().toURL());
        }
    }

    return jars.stream()
        .filter(path -> path.toString().endsWith(".jar"))
        .collect(Collectors.toList());
}
```

---

## 四、具体实现类

### 4.1 SeaTunnelSourcePluginDiscovery

```java
public class SeaTunnelSourcePluginDiscovery extends AbstractPluginDiscovery<SeaTunnelSource> {

    public SeaTunnelSourcePluginDiscovery() {
        super();
    }

    public SeaTunnelSourcePluginDiscovery(
            BiConsumer<ClassLoader, List<URL>> addURLToClassLoader) {
        super(addURLToClassLoader);
    }

    @Override
    protected Class<SeaTunnelSource> getPluginBaseClass() {
        return SeaTunnelSource.class;  // Source 插件基类
    }

    @Override
    public LinkedHashMap<PluginIdentifier, OptionRule> getPlugins() {
        LinkedHashMap<PluginIdentifier, OptionRule> plugins = new LinkedHashMap<>();

        // 遍历所有 Factory，筛选 TableSourceFactory
        getPluginFactories().stream()
            .filter(factory -> TableSourceFactory.class.isAssignableFrom(factory.getClass()))
            .forEach(factory -> getPluginsByFactoryIdentifier(
                plugins,
                PluginType.SOURCE,
                factory.factoryIdentifier(),
                FactoryUtil.sourceFullOptionRule((TableSourceFactory) factory)));

        return plugins;
    }
}
```

### 4.2 SeaTunnelSinkPluginDiscovery

```java
public class SeaTunnelSinkPluginDiscovery extends AbstractPluginDiscovery<SeaTunnelSink> {

    // 需要过滤的内部 Sink
    private static final String MULTITABLESINK_FACTORYIDENTIFIER = "MultiTableSink";

    @Override
    protected Class<SeaTunnelSink> getPluginBaseClass() {
        return SeaTunnelSink.class;  // Sink 插件基类
    }

    @Override
    public LinkedHashMap<PluginIdentifier, OptionRule> getPlugins() {
        LinkedHashMap<PluginIdentifier, OptionRule> plugins = new LinkedHashMap<>();

        getPluginFactories().stream()
            .filter(factory ->
                // 过滤掉 MultiTableSink（内部使用）
                !factory.factoryIdentifier().equals(MULTITABLESINK_FACTORYIDENTIFIER)
                && TableSinkFactory.class.isAssignableFrom(factory.getClass()))
            .forEach(factory -> getPluginsByFactoryIdentifier(
                plugins,
                PluginType.SINK,
                factory.factoryIdentifier(),
                FactoryUtil.sinkFullOptionRule((TableSinkFactory) factory)));

        return plugins;
    }
}
```

### 4.3 SeaTunnelTransformPluginDiscovery

```java
public class SeaTunnelTransformPluginDiscovery
        extends AbstractPluginDiscovery<SeaTunnelTransform> {

    public SeaTunnelTransformPluginDiscovery() {
        super(Common.connectorDir());  // Transform 也在 connectors 目录
    }

    @Override
    protected Class<SeaTunnelTransform> getPluginBaseClass() {
        return SeaTunnelTransform.class;  // Transform 插件基类
    }

    @Override
    public LinkedHashMap<PluginIdentifier, OptionRule> getPlugins() {
        LinkedHashMap<PluginIdentifier, OptionRule> plugins = new LinkedHashMap<>();

        getPluginFactories().stream()
            .filter(factory ->
                TableTransformFactory.class.isAssignableFrom(factory.getClass()))
            .forEach(factory -> getPluginsByFactoryIdentifier(
                plugins,
                PluginType.TRANSFORM,
                factory.factoryIdentifier(),
                factory.optionRule()));

        return plugins;
    }
}
```

### 4.4 SeaTunnelFactoryDiscovery

```java
public class SeaTunnelFactoryDiscovery extends AbstractPluginDiscovery<Factory> {

    private final Class<? extends Factory> factoryClass;

    public SeaTunnelFactoryDiscovery(Class<? extends Factory> factoryClass) {
        super();
        this.factoryClass = factoryClass;
    }

    @Override
    protected Class<Factory> getPluginBaseClass() {
        return Factory.class;
    }

    @Override
    protected Factory loadPluginInstance(
            PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
        ServiceLoader<Factory> serviceLoader =
            ServiceLoader.load(getPluginBaseClass(), classLoader);

        for (Factory factory : serviceLoader) {
            // 检查 Factory 类型是否匹配
            if (factoryClass.isInstance(factory)) {
                String factoryIdentifier = factory.factoryIdentifier();
                String pluginName = pluginIdentifier.getPluginName();
                if (StringUtils.equalsIgnoreCase(factoryIdentifier, pluginName)) {
                    return factory;
                }
            }
        }
        return null;
    }
}
```

---

## 五、plugin-mapping.properties 配置

### 5.1 配置格式

```properties
# 格式: seatunnel.{source|sink|transform}.{PluginName} = {connector-artifactId}

# Source 插件映射
seatunnel.source.FakeSource = connector-fake
seatunnel.source.Kafka = connector-kafka
seatunnel.source.Jdbc = connector-jdbc
seatunnel.source.MySQL-CDC = connector-cdc-mysql
seatunnel.source.Postgres-CDC = connector-cdc-postgres

# Sink 插件映射
seatunnel.sink.Console = connector-console
seatunnel.sink.Kafka = connector-kafka
seatunnel.sink.Jdbc = connector-jdbc
seatunnel.sink.Doris = connector-doris

# 同一个连接器可同时作为 Source 和 Sink
seatunnel.source.Redis = connector-redis
seatunnel.sink.Redis = connector-redis
```

### 5.2 配置文件位置

```
SEATUNNEL_HOME/
├── connectors/
│   ├── plugin-mapping.properties    # 插件映射配置
│   ├── connector-jdbc-xxx.jar
│   ├── connector-kafka-xxx.jar
│   └── ...
└── plugins/
    ├── connector-jdbc/              # 连接器依赖目录
    │   └── lib/
    │       ├── mysql-connector-java.jar
    │       └── ...
    └── ...
```

### 5.3 配置解析流程

```
1. 用户配置文件
   source { Jdbc { ... } }
         ↓
2. 解析插件标识符
   PluginIdentifier.of("seatunnel", "source", "Jdbc")
         ↓
3. 查询 plugin-mapping.properties
   seatunnel.source.Jdbc = connector-jdbc
         ↓
4. 在 connectors 目录查找
   connectors/connector-jdbc-*.jar
         ↓
5. 加载插件实例
   ServiceLoader.load(SeaTunnelSource.class)
```

---

## 六、插件加载流程

### 6.1 完整流程图

```
┌─────────────────────────────────────────────────────────────┐
│                      用户作业配置                            │
│  source { Jdbc { url = "...", query = "..." } }             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   1. 解析插件标识符                          │
│  PluginIdentifier.of("seatunnel", "source", "Jdbc")         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                2. 查询 plugin-mapping.properties             │
│  seatunnel.source.Jdbc → connector-jdbc                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   3. 查找插件 JAR 文件                       │
│  connectors/connector-jdbc-2.3.x.jar                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   4. 查找依赖 JAR 文件                       │
│  plugins/connector-jdbc/lib/*.jar                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   5. 创建 ClassLoader                        │
│  URLClassLoader(connectorJar + dependencyJars)              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    6. SPI 加载插件                           │
│  ServiceLoader.load(SeaTunnelSource.class, classLoader)     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   7. 匹配插件名称                            │
│  pluginInstance.getPluginName().equals("Jdbc")              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   8. 返回插件实例                            │
│  JdbcSource (SeaTunnelSource implementation)                │
└─────────────────────────────────────────────────────────────┘
```

### 6.2 ClassLoader 策略

```java
// 默认策略：向当前 ClassLoader 添加 URL
private static final BiConsumer<ClassLoader, List<URL>> DEFAULT_URL_TO_CLASSLOADER =
    (classLoader, urls) -> {
        if (classLoader instanceof URLClassLoader) {
            // 通过反射调用 URLClassLoader.addURL()
            urls.forEach(url -> ReflectionUtils.invoke(classLoader, "addURL", url));
        } else {
            throw new UnsupportedOperationException("can't support custom load jar");
        }
    };

// 备选策略：创建新的 URLClassLoader
URLClassLoader classLoader = new URLClassLoader(urls,
    Thread.currentThread().getContextClassLoader());
```

### 6.3 SPI 配置文件

插件 JAR 包中必须包含 SPI 配置文件：

```
META-INF/services/
├── org.apache.seatunnel.api.source.SeaTunnelSource
├── org.apache.seatunnel.api.sink.SeaTunnelSink
├── org.apache.seatunnel.api.transform.SeaTunnelTransform
└── org.apache.seatunnel.api.table.factory.Factory
```

**org.apache.seatunnel.api.source.SeaTunnelSource** 示例：
```
org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSource
```

---

## 七、使用示例

### 7.1 发现 Source 插件

```java
// 创建 Source 插件发现器
SeaTunnelSourcePluginDiscovery discovery = new SeaTunnelSourcePluginDiscovery();

// 创建插件标识符
PluginIdentifier identifier = PluginIdentifier.of("seatunnel", "source", "Jdbc");

// 创建插件实例
SeaTunnelSource<?, ?, ?> source = discovery.createPluginInstance(identifier);

// 获取所有 Source 插件
LinkedHashMap<PluginIdentifier, OptionRule> allSources = discovery.getPlugins();
```

### 7.2 发现 Sink 插件

```java
SeaTunnelSinkPluginDiscovery discovery = new SeaTunnelSinkPluginDiscovery();

PluginIdentifier identifier = PluginIdentifier.of("seatunnel", "sink", "Console");

SeaTunnelSink<?, ?, ?, ?> sink = discovery.createPluginInstance(identifier);
```

### 7.3 发现 Transform 插件

```java
SeaTunnelTransformPluginDiscovery discovery = new SeaTunnelTransformPluginDiscovery();

PluginIdentifier identifier = PluginIdentifier.of("seatunnel", "transform", "Sql");

SeaTunnelTransform<?> transform = discovery.createPluginInstance(identifier);
```

### 7.4 获取插件配置选项

```java
SeaTunnelSourcePluginDiscovery discovery = new SeaTunnelSourcePluginDiscovery();

// 获取 Jdbc Source 的配置选项
ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> options =
    discovery.getOptionRules("Jdbc");

PluginIdentifier identifier = options.getLeft();
List<Option<?>> requiredOptions = options.getMiddle();  // 必需参数
List<Option<?>> optionalOptions = options.getRight();   // 可选参数
```

### 7.5 批量获取插件 JAR

```java
SeaTunnelSourcePluginDiscovery discovery = new SeaTunnelSourcePluginDiscovery();

List<PluginIdentifier> identifiers = Arrays.asList(
    PluginIdentifier.of("seatunnel", "source", "Jdbc"),
    PluginIdentifier.of("seatunnel", "source", "Kafka")
);

// 获取所有插件 JAR 路径
List<URL> jarPaths = discovery.getPluginJarPaths(identifiers);

// 获取插件 JAR 及其依赖
List<URL> allJars = discovery.getPluginJarAndDependencyPaths(identifiers);
```

---

## 八、与其他模块的关系

### 8.1 模块依赖图

```
┌─────────────────────────────────────────────────────────────┐
│                     seatunnel-core                          │
│                    (SeaTunnelStarter)                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                seatunnel-plugin-discovery                    │
│    (PluginDiscovery, AbstractPluginDiscovery)               │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  seatunnel-api  │ │ seatunnel-common│ │   Connectors    │
│ (Source, Sink)  │ │    (Utils)      │ │  (Plugin JARs)  │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

### 8.2 调用关系

1. **seatunnel-core** 启动时调用 PluginDiscovery
2. **PluginDiscovery** 读取 plugin-mapping.properties
3. **PluginDiscovery** 在 connectors 目录查找 JAR
4. **PluginDiscovery** 通过 SPI 加载插件实例
5. **seatunnel-core** 使用插件实例执行作业

---

## 九、设计模式分析

### 9.1 模板方法模式

`AbstractPluginDiscovery` 定义了插件发现的骨架算法：

```java
public abstract class AbstractPluginDiscovery<T> implements PluginDiscovery<T> {

    // 模板方法：创建插件实例
    public Optional<T> createOptionalPluginInstance(...) {
        // 1. 从 classpath 加载
        T instance = loadPluginInstance(...);
        if (instance != null) return Optional.of(instance);

        // 2. 从插件目录加载
        Optional<List<URL>> jarPaths = getPluginJarPath(...);
        // ...
    }

    // 钩子方法：子类实现
    protected abstract Class<T> getPluginBaseClass();
}
```

### 9.2 策略模式

ClassLoader 添加 URL 的策略可以自定义：

```java
// 默认策略
BiConsumer<ClassLoader, List<URL>> defaultStrategy = (cl, urls) -> {
    urls.forEach(url -> ReflectionUtils.invoke(cl, "addURL", url));
};

// 自定义策略（如 Flink/Spark 引擎）
BiConsumer<ClassLoader, List<URL>> customStrategy = (cl, urls) -> {
    // 引擎特定的 ClassLoader 处理
};

new SeaTunnelSourcePluginDiscovery(customStrategy);
```

### 9.3 工厂方法模式

各类型 PluginDiscovery 由不同工厂创建：

```java
// Source 插件发现
PluginDiscovery<SeaTunnelSource> sourceDiscovery =
    new SeaTunnelSourcePluginDiscovery();

// Sink 插件发现
PluginDiscovery<SeaTunnelSink> sinkDiscovery =
    new SeaTunnelSinkPluginDiscovery();

// Transform 插件发现
PluginDiscovery<SeaTunnelTransform> transformDiscovery =
    new SeaTunnelTransformPluginDiscovery();
```

### 9.4 缓存模式

插件 JAR 路径使用 ConcurrentHashMap 缓存：

```java
protected final ConcurrentHashMap<PluginIdentifier, Optional<List<URL>>> pluginJarPath =
    new ConcurrentHashMap<>(Common.COLLECTION_SIZE);

protected Optional<List<URL>> getPluginJarPath(PluginIdentifier pluginIdentifier) {
    return pluginJarPath.computeIfAbsent(pluginIdentifier, this::findPluginJarPath);
}
```

---

## 十、最佳实践

### 10.1 添加新插件映射

1. 编辑 `plugin-mapping.properties`：
```properties
seatunnel.source.MySource = connector-mysource
seatunnel.sink.MySink = connector-mysource
```

2. 确保连接器 JAR 在 connectors 目录：
```
connectors/
└── connector-mysource-2.3.x.jar
```

3. 确保 JAR 包含 SPI 配置：
```
META-INF/services/org.apache.seatunnel.api.source.SeaTunnelSource
META-INF/services/org.apache.seatunnel.api.table.factory.Factory
```

### 10.2 添加连接器依赖

1. 创建依赖目录：
```
plugins/
└── connector-mysource/
    └── lib/
        ├── dependency-1.jar
        └── dependency-2.jar
```

2. 系统会自动加载该目录下的所有 JAR

### 10.3 自定义 ClassLoader

```java
// 为 Flink 引擎自定义 ClassLoader 策略
BiConsumer<ClassLoader, List<URL>> flinkStrategy = (classLoader, urls) -> {
    if (classLoader instanceof FlinkUserClassLoader) {
        FlinkUserClassLoader flinkCL = (FlinkUserClassLoader) classLoader;
        urls.forEach(flinkCL::addURL);
    }
};

SeaTunnelSourcePluginDiscovery discovery =
    new SeaTunnelSourcePluginDiscovery(flinkStrategy);
```

---

## 十一、总结

### 11.1 模块特点

| 特点 | 说明 |
|------|------|
| SPI 机制 | 使用 Java ServiceLoader 动态加载插件 |
| 配置驱动 | 通过 plugin-mapping.properties 映射插件 |
| 延迟加载 | 按需加载插件 JAR，支持缓存 |
| 依赖管理 | 自动加载连接器依赖目录 |
| 可扩展 | 支持自定义 ClassLoader 策略 |

### 11.2 核心类

| 类 | 功能 |
|----|------|
| PluginDiscovery | 插件发现接口 |
| AbstractPluginDiscovery | 核心实现（模板方法） |
| SeaTunnelSourcePluginDiscovery | Source 插件发现 |
| SeaTunnelSinkPluginDiscovery | Sink 插件发现 |
| SeaTunnelTransformPluginDiscovery | Transform 插件发现 |
| SeaTunnelFactoryDiscovery | Factory 发现 |

### 11.3 关键配置

| 配置 | 说明 |
|------|------|
| plugin-mapping.properties | 插件名称到 JAR 的映射 |
| SEATUNNEL_HOME/connectors/ | 连接器 JAR 目录 |
| SEATUNNEL_HOME/plugins/ | 连接器依赖目录 |
| META-INF/services/ | SPI 配置文件 |

---

## 附录A：plugin-mapping.properties 完整示例

```properties
# Source 插件
seatunnel.source.FakeSource = connector-fake
seatunnel.source.Jdbc = connector-jdbc
seatunnel.source.Kafka = connector-kafka
seatunnel.source.MySQL-CDC = connector-cdc-mysql
seatunnel.source.Postgres-CDC = connector-cdc-postgres
seatunnel.source.MongoDB-CDC = connector-cdc-mongodb
seatunnel.source.HdfsFile = connector-file-hadoop
seatunnel.source.S3File = connector-file-s3
seatunnel.source.Redis = connector-redis
seatunnel.source.Elasticsearch = connector-elasticsearch

# Sink 插件
seatunnel.sink.Console = connector-console
seatunnel.sink.Assert = connector-assert
seatunnel.sink.Jdbc = connector-jdbc
seatunnel.sink.Kafka = connector-kafka
seatunnel.sink.Doris = connector-doris
seatunnel.sink.StarRocks = connector-starrocks
seatunnel.sink.Clickhouse = connector-clickhouse
seatunnel.sink.Elasticsearch = connector-elasticsearch
seatunnel.sink.HdfsFile = connector-file-hadoop
seatunnel.sink.S3File = connector-file-s3

# Transform 插件（可选）
# seatunnel.transform.Sql = seatunnel-transforms-v2
```

---

## 附录B：插件发现类图

```
┌─────────────────────────────────────────────────────────────┐
│                   <<interface>>                              │
│                   PluginDiscovery<T>                         │
├─────────────────────────────────────────────────────────────┤
│ + getPluginJarPaths(List<PluginIdentifier>): List<URL>      │
│ + createPluginInstance(PluginIdentifier): T                  │
│ + createOptionalPluginInstance(PluginIdentifier): Optional<T>│
│ + getAllPlugins(List<PluginIdentifier>): List<T>            │
│ + getPlugins(): LinkedHashMap<PluginIdentifier, OptionRule> │
└─────────────────────────────────────────────────────────────┘
                              △
                              │
┌─────────────────────────────────────────────────────────────┐
│                   <<abstract>>                               │
│               AbstractPluginDiscovery<T>                     │
├─────────────────────────────────────────────────────────────┤
│ - pluginDir: Path                                           │
│ - pluginMappingConfig: Config                               │
│ - pluginJarPath: ConcurrentHashMap                          │
├─────────────────────────────────────────────────────────────┤
│ # loadPluginInstance(PluginIdentifier, ClassLoader): T      │
│ # getPluginJarPath(PluginIdentifier): Optional<List<URL>>   │
│ # abstract getPluginBaseClass(): Class<T>                   │
└─────────────────────────────────────────────────────────────┘
                              △
          ┌───────────────────┼───────────────────┐
          │                   │                   │
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│SeaTunnelSource  │ │SeaTunnelSink    │ │SeaTunnelTransform│
│PluginDiscovery  │ │PluginDiscovery  │ │PluginDiscovery  │
├─────────────────┤ ├─────────────────┤ ├─────────────────┤
│getPluginBaseClass│ │getPluginBaseClass│ │getPluginBaseClass│
│= SeaTunnelSource│ │= SeaTunnelSink  │ │= SeaTunnelTransform│
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

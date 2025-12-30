# SeaTunnel Transforms V2 模块源码分析

> 文档编号: 007
> 模块路径: seatunnel-transforms-v2/
> 更新日期: 2025-12-29

---

## 一、模块概述

### 1.1 功能定位

`seatunnel-transforms-v2` 是 SeaTunnel 的数据转换模块，提供了丰富的数据处理能力：

- **字段处理**: 过滤、重命名、复制、分割、替换等
- **数据解析**: JSON Path 提取、正则提取
- **SQL 转换**: 支持 SQL 语法进行数据转换
- **动态编译**: 支持 Java/Groovy/Scala 运行时编译自定义转换逻辑
- **AI/NLP 能力**: 集成 LLM 和 Embedding 模型进行智能数据处理
- **数据校验**: 数据验证和校验规则

### 1.2 模块统计

| 类别 | 数量 |
|------|------|
| Java 源文件 | 164 |
| Transform 实现类 | 48 |
| Factory 类 | 21 |
| 支持的 AI 模型提供商 | 8+ |

### 1.3 依赖关系

```xml
<dependencies>
    <!-- 核心 API -->
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-api</artifactId>
    </dependency>

    <!-- SQL 解析 -->
    <dependency>
        <groupId>com.github.jsqlparser</groupId>
        <artifactId>jsqlparser</artifactId>
    </dependency>

    <!-- JSON Path -->
    <dependency>
        <groupId>com.jayway.jsonpath</groupId>
        <artifactId>json-path</artifactId>
    </dependency>

    <!-- 动态编译 -->
    <dependency>
        <groupId>org.apache.groovy</groupId>
        <artifactId>groovy</artifactId>
    </dependency>

    <!-- AWS Bedrock (AI) -->
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>bedrockruntime</artifactId>
    </dependency>
</dependencies>
```

---

## 二、核心类层次结构

### 2.1 Transform 抽象层次

```
SeaTunnelTransform<T> (API 接口)
    │
    ├── SeaTunnelMapTransform<T>          # 1:1 转换
    │       ↓
    │   AbstractCatalogSupportMapTransform
    │       │
    │       ├── SingleFieldOutputTransform    # 单字段输出
    │       ├── MultipleFieldOutputTransform  # 多字段输出
    │       └── FilterRowTransform            # 行过滤
    │
    └── SeaTunnelFlatMapTransform<T>      # 1:N 转换
            ↓
        AbstractCatalogSupportFlatMapTransform
            │
            └── SQLTransform                  # SQL 转换（可输出多行）
```

### 2.2 核心抽象类

#### 2.2.1 AbstractSeaTunnelTransform

**路径**: `transform/common/AbstractSeaTunnelTransform.java`

```java
public abstract class AbstractSeaTunnelTransform<T, R> implements SeaTunnelTransform<T> {

    protected final ErrorHandleWay rowErrorHandleWay;  // 错误处理方式
    protected CatalogTable inputCatalogTable;          // 输入表元数据
    protected volatile CatalogTable outputCatalogTable; // 输出表元数据

    // 获取输出表定义
    public CatalogTable getProducedCatalogTable() {
        if (outputCatalogTable == null) {
            synchronized (this) {
                if (outputCatalogTable == null) {
                    outputCatalogTable = transformCatalogTable();
                }
            }
        }
        return outputCatalogTable;
    }

    // 转换入口（带错误处理）
    public R transform(SeaTunnelRow row) {
        try {
            return transformRow(row);
        } catch (ErrorDataTransformException e) {
            if (rowErrorHandleWay.allowSkip()) {
                return null;  // 跳过错误行
            }
            throw e;
        }
    }

    // 子类实现：转换单行数据
    protected abstract R transformRow(SeaTunnelRow inputRow);

    // 子类实现：转换表结构
    protected abstract TableSchema transformTableSchema();

    // 子类实现：转换表标识
    protected abstract TableIdentifier transformTableIdentifier();
}
```

**设计要点**:
- 双重检查锁定（DCL）保证线程安全的懒加载
- 统一的错误处理机制
- 模板方法模式，子类只需实现核心转换逻辑

#### 2.2.2 SingleFieldOutputTransform

**路径**: `transform/common/SingleFieldOutputTransform.java`

**职责**: 处理单字段输出的转换场景（如字段替换、正则提取）

```java
public abstract class SingleFieldOutputTransform extends AbstractCatalogSupportMapTransform {

    private int fieldIndex;                              // 输出字段索引
    private SeaTunnelRowContainerGenerator rowContainerGenerator;  // 行容器生成器

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // 获取转换后的字段值
        Object fieldValue = getOutputFieldValue(new SeaTunnelRowAccessor(inputRow));

        // 生成输出行
        SeaTunnelRow outputRow = rowContainerGenerator.apply(inputRow);
        outputRow.setField(fieldIndex, fieldValue);
        return outputRow;
    }

    // 子类实现：计算输出字段值
    protected abstract Object getOutputFieldValue(SeaTunnelRowAccessor inputRow);

    // 子类实现：定义输出字段
    protected abstract Column getOutputColumn();
}
```

**优化技术**:
- 字段索引预计算，避免运行时查找
- 行容器复用策略（`SeaTunnelRowContainerGenerator.REUSE_ROW`）

#### 2.2.3 MultipleFieldOutputTransform

**路径**: `transform/common/MultipleFieldOutputTransform.java`

**职责**: 处理多字段输出的转换场景（如字段分割、JSON 解析）

```java
public abstract class MultipleFieldOutputTransform extends AbstractCatalogSupportMapTransform {

    private String[] outputFieldNames;  // 输出字段名数组
    private int[] fieldsIndex;          // 输出字段索引数组

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] fieldValues = getOutputFieldValues(new SeaTunnelRowAccessor(inputRow));

        SeaTunnelRow outputRow = rowContainerGenerator.apply(inputRow);
        for (int i = 0; i < outputFieldNames.length; i++) {
            outputRow.setField(fieldsIndex[i], fieldValues == null ? null : fieldValues[i]);
        }
        return outputRow;
    }

    // 子类实现：计算多个输出字段值
    protected abstract Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow);

    // 子类实现：定义输出字段列表
    protected abstract Column[] getOutputColumns();
}
```

---

## 三、转换器分类详解

### 3.1 字段操作类

| 转换器 | 插件名 | 功能描述 | 基类 |
|--------|--------|----------|------|
| FilterFieldTransform | Filter | 字段过滤（include/exclude） | AbstractCatalogSupportMapTransform |
| FieldRenameTransform | FieldRename | 字段重命名 | AbstractCatalogSupportMapTransform |
| CopyFieldTransform | Copy | 字段复制 | MultipleFieldOutputTransform |
| ReplaceTransform | Replace | 字段值替换 | SingleFieldOutputTransform |
| SplitTransform | Split | 字段分割 | MultipleFieldOutputTransform |
| FieldMapperTransform | FieldMapper | 字段映射 | AbstractCatalogSupportMapTransform |

#### 3.1.1 FilterFieldTransform 示例

**路径**: `transform/filter/FilterFieldTransform.java`

```java
public class FilterFieldTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "Filter";

    private int[] inputValueIndexList;      // 保留字段的索引
    private final List<String> includeFields;  // 包含字段列表
    private final List<String> excludeFields;  // 排除字段列表

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // 使用预计算的索引数组快速复制字段
        return inputRow.copy(inputValueIndexList);
    }

    @Override
    protected TableSchema transformTableSchema() {
        // 根据 include/exclude 配置构建输出 Schema
        if (Objects.nonNull(includeFields)) {
            // 仅保留指定字段
            for (String fieldName : includeFields) {
                int inputFieldIndex = seaTunnelRowType.indexOf(fieldName);
                inputValueIndexList[i] = inputFieldIndex;
                outputColumns.add(inputColumns.get(inputFieldIndex).copy());
            }
        }
        if (Objects.nonNull(excludeFields)) {
            // 排除指定字段
            for (Column column : inputColumns) {
                if (!excludeFields.contains(column.getName())) {
                    outputColumns.add(column.copy());
                }
            }
        }
        return TableSchema.builder().columns(outputColumns).build();
    }
}
```

**配置示例**:
```hocon
transform {
  Filter {
    source_table_name = "source"
    include_fields = ["id", "name", "age"]
    # 或使用 exclude_fields = ["password", "secret"]
  }
}
```

#### 3.1.2 SplitTransform 示例

**路径**: `transform/split/SplitTransform.java`

```java
public class SplitTransform extends MultipleFieldOutputTransform {
    public static String PLUGIN_NAME = "Split";

    private final SplitTransformConfig splitTransformConfig;
    private final int splitFieldIndex;

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object splitFieldValue = inputRow.getField(splitFieldIndex);
        if (splitFieldValue == null) {
            return splitTransformConfig.getEmptySplits();
        }

        // 按分隔符分割
        String[] splitFieldValues = splitFieldValue.toString()
            .split(splitTransformConfig.getSeparator(),
                   splitTransformConfig.getOutputFields().length);
        return splitFieldValues;
    }

    @Override
    protected Column[] getOutputColumns() {
        return Arrays.stream(splitTransformConfig.getOutputFields())
            .map(fieldName -> PhysicalColumn.of(fieldName, BasicType.STRING_TYPE, 200, true, "", ""))
            .toArray(Column[]::new);
    }
}
```

**配置示例**:
```hocon
transform {
  Split {
    source_table_name = "source"
    split_field = "full_name"
    separator = " "
    output_fields = ["first_name", "last_name"]
  }
}
```

### 3.2 SQL 转换类

#### 3.2.1 SQLTransform

**路径**: `transform/sql/SQLTransform.java`

**特点**:
- 支持 FlatMap 语义（一行可输出多行）
- 使用 JSqlParser 解析 SQL
- 支持 Zeta 引擎（内置）

```java
public class SQLTransform extends AbstractCatalogSupportFlatMapTransform {
    public static final String PLUGIN_NAME = "Sql";

    private final String query;
    private final EngineType engineType;
    private transient SQLEngine sqlEngine;

    @Override
    public void open() {
        sqlEngine = SQLEngineFactory.getSQLEngine(engineType);
        sqlEngine.init(inputTableName,
                       inputCatalogTable.getTableId().getTableName(),
                       inputCatalogTable.getSeaTunnelRowType(),
                       query);
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        return sqlEngine.transformBySQL(inputRow, outRowType);
    }
}
```

#### 3.2.2 Zeta SQL Engine 函数库

**路径**: `transform/sql/zeta/functions/`

| 函数类 | 提供的函数 |
|--------|-----------|
| StringFunction | CONCAT, SUBSTRING, UPPER, LOWER, TRIM, LENGTH, REPLACE 等 |
| NumericFunction | ABS, CEIL, FLOOR, ROUND, MOD, POWER 等 |
| DateTimeFunction | NOW, DATE_FORMAT, DATEDIFF, DATE_ADD 等 |
| ArrayFunction | ARRAY_CONTAINS, ARRAY_JOIN, ARRAY_MAX 等 |
| MapFunction | MAP_KEYS, MAP_VALUES, MAP_ENTRIES 等 |
| CastFunction | CAST 类型转换 |
| SystemFunction | UUID, COALESCE, NULLIF, IF, CASE 等 |

**配置示例**:
```hocon
transform {
  Sql {
    source_table_name = "source"
    query = """
      SELECT
        id,
        UPPER(name) as upper_name,
        age * 2 as double_age,
        CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END as category
      FROM source
      WHERE age > 0
    """
  }
}
```

### 3.3 JSON 处理类

#### 3.3.1 JsonPathTransform

**路径**: `transform/jsonpath/JsonPathTransform.java`

**功能**: 使用 JSONPath 表达式从 JSON 字段中提取数据

```java
public class JsonPathTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "JsonPath";

    // JSONPath 编译缓存
    private static final Map<String, JsonPath> JSON_PATH_CACHE = new ConcurrentHashMap<>();

    private Object doTransform(SeaTunnelDataType<?> inputDataType,
                               Object value,
                               ColumnConfig columnConfig,
                               JsonToObjectConverter converter) {
        // 缓存编译后的 JsonPath
        JSON_PATH_CACHE.computeIfAbsent(columnConfig.getPath(), JsonPath::compile);

        // 执行 JsonPath 查询
        Object result = JSON_PATH_CACHE.get(columnConfig.getPath()).read(jsonString);
        JsonNode jsonNode = JsonUtils.toJsonNode(result);
        return converter.convert(jsonNode, null);
    }
}
```

**配置示例**:
```hocon
transform {
  JsonPath {
    source_table_name = "source"
    columns = [
      {
        src_field = "data"
        path = "$.user.name"
        dest_field = "user_name"
        dest_type = "string"
      },
      {
        src_field = "data"
        path = "$.items[*].price"
        dest_field = "prices"
        dest_type = "array<double>"
      }
    ]
  }
}
```

### 3.4 动态编译类

#### 3.4.1 DynamicCompileTransform

**路径**: `transform/dynamiccompile/DynamicCompileTransform.java`

**功能**: 支持运行时编译 Java/Groovy/Scala 代码实现自定义转换逻辑

```java
public class DynamicCompileTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "DynamicCompile";

    private final String sourceCode;
    private final CompilePattern compilePattern;
    private AbstractParse DynamicCompileParse;

    public DynamicCompileTransform(ReadonlyConfig config, CatalogTable catalogTable) {
        CompileLanguage language = config.get(DynamicCompileTransformConfig.COMPILE_LANGUAGE);

        // 根据语言选择解析器
        switch (language) {
            case GROOVY:
                DynamicCompileParse = new GroovyClassParse();
                break;
            case JAVA:
                DynamicCompileParse = new JavaClassParse();
                break;
            case SCALA:
                DynamicCompileParse = new ScalaClassParse();
                break;
        }

        // 加载源码
        if (CompilePattern.SOURCE_CODE.equals(compilePattern)) {
            sourceCode = config.get(DynamicCompileTransformConfig.SOURCE_CODE);
        } else {
            sourceCode = FileUtils.readFileToStr(Paths.get(config.get(ABSOLUTE_PATH)));
        }
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        // 反射调用编译后的类方法
        return (Object[]) ReflectionUtils.invoke(
            getCompileLanguageInstance(),
            "getInlineOutputFieldValues",
            inputRow);
    }
}
```

**Groovy 代码示例**:
```groovy
class MyTransform {
    Column[] getInlineOutputColumns(CatalogTable table) {
        return [
            PhysicalColumn.of("full_name", BasicType.STRING_TYPE, 200, true, "", "")
        ]
    }

    Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor row) {
        def firstName = row.getField(0)
        def lastName = row.getField(1)
        return ["${firstName} ${lastName}"]
    }
}
```

### 3.5 AI/NLP 转换类

#### 3.5.1 LLMTransform

**路径**: `transform/nlpmodel/llm/LLMTransform.java`

**功能**: 调用大语言模型进行数据转换（文本分类、信息提取等）

```java
public class LLMTransform extends SingleFieldOutputTransform {
    private Model model;

    @Override
    public void open() {
        ModelProvider provider = config.get(ModelTransformConfig.MODEL_PROVIDER);
        switch (provider) {
            case OPENAI:
            case DOUBAO:
            case ZHIPU:
            case DEEPSEEK:
                model = new OpenAIModel(...);
                break;
            case MICROSOFT:
                model = new MicrosoftModel(...);
                break;
            case KIMIAI:
                model = new KimiAIModel(...);
                break;
            case CUSTOM:
                model = new CustomModel(...);
                break;
        }
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        List<String> values = model.inference(Collections.singletonList(seaTunnelRow));
        // 根据输出类型转换结果
        switch (outputDataType.getSqlType()) {
            case STRING:  return String.valueOf(values.get(0));
            case INT:     return Integer.parseInt(values.get(0));
            case BOOLEAN: return Boolean.parseBoolean(values.get(0));
        }
    }
}
```

**支持的模型提供商**:

| Provider | 说明 |
|----------|------|
| OPENAI | OpenAI GPT 系列 |
| MICROSOFT | Azure OpenAI |
| DOUBAO | 字节跳动豆包 |
| ZHIPU | 智谱 AI |
| KIMIAI | Kimi AI |
| DEEPSEEK | DeepSeek |
| CUSTOM | 自定义 API |

**配置示例**:
```hocon
transform {
  LLM {
    source_table_name = "source"
    model_provider = "OPENAI"
    api_key = "${OPENAI_API_KEY}"
    model = "gpt-4"
    prompt = "将以下文本分类为：positive、negative 或 neutral。文本：${text}"
    inference_columns = ["text"]
    output_column_name = "sentiment"
    output_data_type = "string"
  }
}
```

#### 3.5.2 EmbeddingTransform

**路径**: `transform/nlpmodel/embedding/EmbeddingTransform.java`

**功能**: 调用 Embedding 模型将文本/图像转换为向量

```java
public class EmbeddingTransform extends MultipleFieldOutputTransform {
    private transient Model model;
    private Integer dimension;
    private boolean isMultimodalFields = false;

    @Override
    public void open() {
        ModelProvider provider = config.get(ModelTransformConfig.MODEL_PROVIDER);
        switch (provider) {
            case OPENAI:
                model = new OpenAIModel(...);
                break;
            case AMAZON:
                model = new BedrockModel(...);  // AWS Bedrock
                break;
            case DOUBAO:
                model = new DoubaoModel(...);
                break;
            case QIANFAN:
                model = new QianfanModel(...);  // 百度千帆
                break;
            case ZHIPU:
                model = new ZhipuModel(...);
                break;
        }
        dimension = model.dimension();
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        List<ByteBuffer> vectorization = model.vectorization(fieldValues);
        return vectorization.toArray();
    }

    @Override
    public Column[] getOutputColumns() {
        Column[] columns = new Column[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            columns[i] = PhysicalColumn.of(
                fieldNames.get(i),
                VectorType.VECTOR_FLOAT_TYPE,  // 向量类型
                null,
                dimension,
                true, "", "");
        }
        return columns;
    }
}
```

**配置示例**:
```hocon
transform {
  Embedding {
    source_table_name = "source"
    model_provider = "OPENAI"
    api_key = "${OPENAI_API_KEY}"
    model = "text-embedding-ada-002"
    vectorization_fields = {
      content_vector = {
        field_name = "content"
      }
    }
  }
}
```

### 3.6 数据校验类

#### 3.6.1 DataValidatorTransform

**路径**: `transform/validator/DataValidatorTransform.java`

**功能**: 对数据进行规则校验，支持自定义校验规则

**规则类型**:
- 非空校验
- 范围校验
- 正则匹配
- 自定义 UDF 校验

---

## 四、Factory 与 SPI 机制

### 4.1 TableTransformFactory 接口

```java
public interface TableTransformFactory extends Factory {

    // 转换器标识符（配置中的插件名）
    String factoryIdentifier();

    // 定义配置选项规则
    OptionRule optionRule();

    // 创建转换器实例
    TableTransform createTransform(TableTransformFactoryContext context);
}
```

### 4.2 Factory 实现示例

**路径**: `transform/sql/SQLTransformFactory.java`

```java
@AutoService(Factory.class)  // Google AutoService 自动生成 SPI 配置
public class SQLTransformFactory implements TableTransformFactory {

    @Override
    public String factoryIdentifier() {
        return SQLTransform.PLUGIN_NAME;  // "Sql"
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .optional(KEY_QUERY)
            .optional(TransformCommonOptions.MULTI_TABLES)
            .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
            .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () -> new SQLMultiCatalogFlatMapTransform(
            context.getCatalogTables(),
            context.getOptions());
    }
}
```

### 4.3 SPI 注册

使用 Google AutoService 自动生成：

```
META-INF/services/org.apache.seatunnel.api.table.factory.Factory
```

包含所有 `@AutoService(Factory.class)` 注解的类。

---

## 五、多表转换支持

### 5.1 MultiCatalog 抽象

SeaTunnel 支持同时处理多个表的场景（如 CDC 多表同步），提供了多表版本的抽象类：

| 单表版本 | 多表版本 |
|----------|----------|
| AbstractCatalogSupportMapTransform | AbstractMultiCatalogMapTransform |
| AbstractCatalogSupportFlatMapTransform | AbstractMultiCatalogFlatMapTransform |
| SQLTransform | SQLMultiCatalogFlatMapTransform |
| ReplaceTransform | ReplaceMultiCatalogTransform |

### 5.2 表匹配机制

```hocon
transform {
  Sql {
    multi_tables = true
    table_match_regex = "db1\\.user.*"  # 正则匹配表名
    query = "SELECT * FROM ${table_name} WHERE status = 1"
  }
}
```

---

## 六、转换器完整列表

### 6.1 按类别分类

#### 字段操作类
| 插件名 | 类名 | 描述 |
|--------|------|------|
| Filter | FilterFieldTransform | 字段过滤（include/exclude） |
| FieldRename | FieldRenameTransform | 字段重命名 |
| TableRename | TableRenameTransform | 表名重命名 |
| Copy | CopyFieldTransform | 字段复制 |
| Replace | ReplaceTransform | 字段值替换（支持正则） |
| Split | SplitTransform | 字段分割 |
| FieldMapper | FieldMapperTransform | 字段映射 |

#### 数据解析类
| 插件名 | 类名 | 描述 |
|--------|------|------|
| JsonPath | JsonPathTransform | JSON Path 提取 |
| RegexExtract | RegexExtractTransform | 正则表达式提取 |
| Metadata | MetadataTransform | 元数据提取 |

#### SQL 处理类
| 插件名 | 类名 | 描述 |
|--------|------|------|
| Sql | SQLTransform | SQL 转换 |

#### 动态编译类
| 插件名 | 类名 | 描述 |
|--------|------|------|
| DynamicCompile | DynamicCompileTransform | 动态编译（Java/Groovy/Scala） |

#### AI/NLP 类
| 插件名 | 类名 | 描述 |
|--------|------|------|
| LLM | LLMTransform | 大语言模型推理 |
| Embedding | EmbeddingTransform | 文本/图像向量化 |

#### 行处理类
| 插件名 | 类名 | 描述 |
|--------|------|------|
| FilterRowKind | FilterRowKindTransform | 按 RowKind 过滤（CDC 场景） |
| RowKindExtractor | RowKindExtractorTransform | 提取 RowKind 信息 |

#### 表处理类
| 插件名 | 类名 | 描述 |
|--------|------|------|
| TableFilter | TableFilterTransform | 表过滤 |
| TableMerge | TableMergeTransform | 表合并 |

#### 数据校验类
| 插件名 | 类名 | 描述 |
|--------|------|------|
| DataValidator | DataValidatorTransform | 数据校验 |

#### 其他
| 插件名 | 类名 | 描述 |
|--------|------|------|
| DefineSinkType | DefineSinkTypeTransform | 定义 Sink 类型适配 |

---

## 七、开发新转换器

### 7.1 开发步骤

1. **选择基类**
   - 单字段输出 → `SingleFieldOutputTransform`
   - 多字段输出 → `MultipleFieldOutputTransform`
   - 行过滤 → `FilterRowTransform`
   - SQL/复杂转换 → `AbstractCatalogSupportFlatMapTransform`

2. **实现核心方法**
   ```java
   // 必须实现
   public String getPluginName();
   protected abstract R transformRow(SeaTunnelRow inputRow);
   protected abstract TableSchema transformTableSchema();
   ```

3. **创建 Factory 类**
   ```java
   @AutoService(Factory.class)
   public class MyTransformFactory implements TableTransformFactory {
       @Override
       public String factoryIdentifier() { return "MyTransform"; }
       // ...
   }
   ```

4. **注册到 plugin-mapping.properties**
   ```properties
   seatunnel.transform.MyTransform = seatunnel-transforms-v2
   ```

### 7.2 完整示例

```java
// 1. Transform 实现
public class UpperCaseTransform extends SingleFieldOutputTransform {
    public static final String PLUGIN_NAME = "UpperCase";
    private final String fieldName;
    private int fieldIndex;

    public UpperCaseTransform(ReadonlyConfig config, CatalogTable catalogTable) {
        super(catalogTable);
        this.fieldName = config.get(Options.key("field").stringType().noDefaultValue());
        this.fieldIndex = catalogTable.getSeaTunnelRowType().indexOf(fieldName);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        Object value = inputRow.getField(fieldIndex);
        return value == null ? null : value.toString().toUpperCase();
    }

    @Override
    protected Column getOutputColumn() {
        return inputCatalogTable.getTableSchema().getColumn(fieldName).copy();
    }
}

// 2. Factory 实现
@AutoService(Factory.class)
public class UpperCaseTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return UpperCaseTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .required(Options.key("field").stringType().noDefaultValue())
            .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () -> new UpperCaseMultiCatalogTransform(
            context.getCatalogTables(), context.getOptions());
    }
}
```

---

## 八、设计模式总结

### 8.1 使用的设计模式

| 模式 | 应用场景 |
|------|----------|
| **模板方法模式** | AbstractSeaTunnelTransform 定义转换流程骨架 |
| **工厂模式** | TableTransformFactory 创建转换器实例 |
| **策略模式** | 不同语言的动态编译解析器（Groovy/Java/Scala） |
| **享元模式** | JsonPath 缓存编译后的表达式 |
| **适配器模式** | SeaTunnelRowAccessor 适配行数据访问 |

### 8.2 性能优化技术

1. **索引预计算**: 在初始化时计算字段索引，避免运行时查找
2. **对象复用**: `SeaTunnelRowContainerGenerator.REUSE_ROW` 复用行对象
3. **缓存策略**: JsonPath 表达式缓存、SQL 执行计划缓存
4. **懒加载**: 双重检查锁定实现线程安全的懒初始化

---

## 九、配置示例汇总

### 9.1 字段过滤

```hocon
transform {
  Filter {
    source_table_name = "source"
    include_fields = ["id", "name", "email"]
  }
}
```

### 9.2 字段分割

```hocon
transform {
  Split {
    source_table_name = "source"
    split_field = "address"
    separator = ","
    output_fields = ["city", "street", "zip"]
  }
}
```

### 9.3 SQL 转换

```hocon
transform {
  Sql {
    source_table_name = "source"
    query = """
      SELECT
        id,
        CONCAT(first_name, ' ', last_name) as full_name,
        CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END as category
      FROM source
    """
  }
}
```

### 9.4 JSON 解析

```hocon
transform {
  JsonPath {
    source_table_name = "source"
    columns = [
      {
        src_field = "json_data"
        path = "$.user.name"
        dest_field = "user_name"
        dest_type = "string"
      }
    ]
  }
}
```

### 9.5 LLM 转换

```hocon
transform {
  LLM {
    source_table_name = "source"
    model_provider = "OPENAI"
    api_key = "${OPENAI_API_KEY}"
    model = "gpt-4"
    prompt = "分析以下评论的情感：${comment}"
    inference_columns = ["comment"]
    output_column_name = "sentiment"
    output_data_type = "string"
  }
}
```

### 9.6 Embedding 转换

```hocon
transform {
  Embedding {
    source_table_name = "source"
    model_provider = "OPENAI"
    api_key = "${OPENAI_API_KEY}"
    model = "text-embedding-ada-002"
    vectorization_fields = {
      text_vector = {
        field_name = "text"
      }
    }
  }
}
```

---

## 十、总结

`seatunnel-transforms-v2` 模块是 SeaTunnel 数据处理能力的核心，具有以下特点：

1. **丰富的转换能力**: 21 种转换器覆盖字段操作、SQL、动态编译、AI 等场景
2. **良好的扩展性**: 基于 Factory + SPI 机制，易于添加新转换器
3. **高性能设计**: 索引预计算、对象复用、表达式缓存等优化
4. **多表支持**: 完整的 MultiCatalog 抽象支持 CDC 多表场景
5. **AI 集成**: 内置 LLM 和 Embedding 支持，适配多种模型提供商

---

**相关文档**:
- [005-seatunnel-api模块源码分析.md](./005-seatunnel-api模块源码分析.md) - Transform API 定义
- [006-seatunnel-connectors-v2模块源码分析.md](./006-seatunnel-connectors-v2模块源码分析.md) - 连接器实现参考

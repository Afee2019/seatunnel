# SeaTunnel Formats 模块源码分析

> 文档编号: 008
> 模块路径: seatunnel-formats/
> 更新日期: 2025-12-29

---

## 一、模块概述

### 1.1 功能定位

`seatunnel-formats` 是 SeaTunnel 的数据格式处理模块，提供了多种数据序列化/反序列化能力：

- **通用格式**: JSON、Text、CSV
- **二进制格式**: Avro、Protobuf
- **CDC 格式**: Canal、Debezium、Maxwell、OGG
- **兼容格式**: Kafka Connect、Debezium 原生格式

### 1.2 模块结构

```
seatunnel-formats/
├── pom.xml
├── seatunnel-format-json/                    # JSON 格式（含 CDC）
├── seatunnel-format-text/                    # 文本格式
├── seatunnel-format-csv/                     # CSV 格式
├── seatunnel-format-avro/                    # Avro 格式
├── seatunnel-format-protobuf/                # Protobuf 格式
├── seatunnel-format-compatible-debezium-json/ # Debezium 兼容格式
└── seatunnel-format-compatible-connect-json/  # Kafka Connect 兼容格式
```

### 1.3 模块统计

| 子模块 | Java 文件数 | 主要功能 |
|--------|-------------|----------|
| seatunnel-format-json | 21 | JSON/Canal/Debezium/Maxwell/OGG |
| seatunnel-format-text | 7 | 文本文件解析 |
| seatunnel-format-csv | 7 | CSV 文件解析 |
| seatunnel-format-avro | 7 | Avro 二进制格式 |
| seatunnel-format-protobuf | 7 | Protobuf 二进制格式 |
| seatunnel-format-compatible-debezium-json | 3 | Debezium 原生兼容 |
| seatunnel-format-compatible-connect-json | 3 | Kafka Connect 兼容 |
| **总计** | **71** | - |

---

## 二、核心 API 接口

### 2.1 序列化/反序列化接口

来自 `seatunnel-api` 模块：

```java
// 反序列化接口
public interface DeserializationSchema<T> extends Serializable {
    // 单条消息反序列化
    T deserialize(byte[] message) throws IOException;

    // 批量反序列化（用于 CDC 场景，一条消息可能产生多行）
    default void deserialize(byte[] message, Collector<T> out) throws IOException {
        T deserialize = deserialize(message);
        if (deserialize != null) {
            out.collect(deserialize);
        }
    }

    // 获取输出数据类型
    SeaTunnelDataType<T> getProducedType();
}

// 序列化接口
public interface SerializationSchema extends Serializable {
    byte[] serialize(SeaTunnelRow row);
}
```

### 2.2 核心设计模式

1. **转换器模式**: `JsonToRowConverters` / `RowToJsonConverters`
2. **Builder 模式**: `TextDeserializationSchema.Builder`
3. **策略模式**: 不同 CDC 格式的解析策略

---

## 三、JSON 格式模块

### 3.1 模块结构

```
seatunnel-format-json/
├── JsonDeserializationSchema.java      # JSON 反序列化
├── JsonSerializationSchema.java        # JSON 序列化
├── JsonToRowConverters.java            # JSON → SeaTunnelRow
├── RowToJsonConverters.java            # SeaTunnelRow → JSON
├── JsonFormatOptions.java              # 配置选项
├── canal/                              # Canal CDC 格式
│   ├── CanalJsonDeserializationSchema.java
│   └── CanalJsonSerializationSchema.java
├── debezium/                           # Debezium CDC 格式
│   ├── DebeziumJsonDeserializationSchema.java
│   ├── DebeziumJsonSerializationSchema.java
│   └── DebeziumRowConverter.java
├── maxwell/                            # Maxwell CDC 格式
│   ├── MaxWellJsonDeserializationSchema.java
│   └── MaxWellJsonSerializationSchema.java
└── ogg/                                # Oracle GoldenGate 格式
    ├── OggJsonDeserializationSchema.java
    └── OggJsonSerializationSchema.java
```

### 3.2 JsonDeserializationSchema

**路径**: `format/json/JsonDeserializationSchema.java`

```java
public class JsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private final boolean failOnMissingField;    // 字段缺失是否报错
    private final boolean ignoreParseErrors;     // 是否忽略解析错误
    private final SeaTunnelRowType rowType;      // 目标行类型
    private final ObjectMapper objectMapper;     // Jackson 解析器
    private JsonToRowConverters.JsonToObjectConverter runtimeConverter;  // 运行时转换器

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return convertJsonNode(convertBytes(message));
    }

    // 支持数组消息（一次返回多行）
    public void collect(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        JsonNode jsonNode = convertBytes(message);
        if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                out.collect(convertJsonNode(arrayNode.get(i)));
            }
        } else {
            out.collect(convertJsonNode(jsonNode));
        }
    }
}
```

**特点**:
- 支持 `failOnMissingField` 和 `ignoreParseErrors` 两种错误处理模式
- 自动检测 Decimal 类型启用精确解析
- 支持控制字符（`ALLOW_UNESCAPED_CONTROL_CHARS`）

### 3.3 JsonToRowConverters

**路径**: `format/json/JsonToRowConverters.java`

**职责**: 根据 SeaTunnel 数据类型创建对应的 JSON 转换器

```java
public class JsonToRowConverters implements Serializable {

    // 核心转换接口
    public interface JsonToObjectConverter extends Serializable {
        Object convert(JsonNode jsonNode, String fieldName);
    }

    // 根据数据类型创建转换器
    public JsonToObjectConverter createConverter(SeaTunnelDataType<?> type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private JsonToObjectConverter createNotNullConverter(SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case BOOLEAN:
                return (jsonNode, fieldName) -> convertToBoolean(jsonNode);
            case INT:
                return (jsonNode, fieldName) -> convertToInt(jsonNode);
            case BIGINT:
                return (jsonNode, fieldName) -> convertToLong(jsonNode);
            case DATE:
                return (jsonNode, fieldName) -> convertToLocalDate(jsonNode, fieldName);
            case TIMESTAMP:
                return (jsonNode, fieldName) -> convertToLocalDateTime(jsonNode, fieldName);
            case ARRAY:
                return createArrayConverter((ArrayType<?, ?>) type);
            case MAP:
                return createMapConverter((MapType<?, ?>) type);
            case ROW:
                return createRowConverter((SeaTunnelRowType) type);
            // ... 其他类型
        }
    }
}
```

**支持的类型**:

| SQL Type | Java Type | 转换逻辑 |
|----------|-----------|----------|
| BOOLEAN | Boolean | `jsonNode.asBoolean()` |
| TINYINT | Byte | `Byte.parseByte(text)` |
| SMALLINT | Short | `Short.parseShort(text)` |
| INT | Integer | `jsonNode.asInt()` |
| BIGINT | Long | `jsonNode.asLong()` |
| FLOAT | Float | `jsonNode.asDouble()` cast |
| DOUBLE | Double | `jsonNode.asDouble()` |
| DECIMAL | BigDecimal | `jsonNode.decimalValue()` |
| STRING | String | `jsonNode.asText()` |
| BYTES | byte[] | `jsonNode.binaryValue()` |
| DATE | LocalDate | 自动匹配日期格式 |
| TIME | LocalTime | `HH:mm:ss.SSSSSSSSS` |
| TIMESTAMP | LocalDateTime | 自动匹配日期时间格式 |
| ARRAY | Object[] | 递归转换元素 |
| MAP | Map | 递归转换键值 |
| ROW | SeaTunnelRow | 递归转换字段 |

### 3.4 CDC 格式 - Canal

**路径**: `format/json/canal/CanalJsonDeserializationSchema.java`

**Canal JSON 消息结构**:
```json
{
  "data": [{"id": 1, "name": "test"}],
  "old": [{"id": 1, "name": "old_test"}],
  "type": "UPDATE",
  "database": "mydb",
  "table": "users",
  "ts": 1640000000000
}
```

**操作类型映射**:

| Canal Type | RowKind | 说明 |
|------------|---------|------|
| INSERT | INSERT | 插入操作 |
| UPDATE | UPDATE_BEFORE + UPDATE_AFTER | 更新前后两条 |
| DELETE | DELETE | 删除操作 |
| CREATE/ALTER/QUERY | 跳过 | DDL 操作忽略 |

```java
public void deserialize(ObjectNode jsonNode, Collector<SeaTunnelRow> out) {
    String op = jsonNode.get(FIELD_TYPE).asText();

    switch (op) {
        case OP_INSERT:
            for (JsonNode data : dataNode) {
                SeaTunnelRow row = convertJsonNode(data);
                out.collect(row);  // RowKind.INSERT (默认)
            }
            break;
        case OP_UPDATE:
            ArrayNode oldNode = (ArrayNode) jsonNode.get(FIELD_OLD);
            for (int i = 0; i < dataNode.size(); i++) {
                SeaTunnelRow before = convertJsonNode(oldNode.get(i));
                SeaTunnelRow after = convertJsonNode(dataNode.get(i));
                // 补全 before 中未变更的字段
                for (int f = 0; f < fieldCount; f++) {
                    if (before.isNullAt(f) && oldNode.findValue(fieldNames[f]) == null) {
                        before.setField(f, after.getField(f));
                    }
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(before);
                out.collect(after);
            }
            break;
        case OP_DELETE:
            for (JsonNode data : dataNode) {
                SeaTunnelRow row = convertJsonNode(data);
                row.setRowKind(RowKind.DELETE);
                out.collect(row);
            }
            break;
    }
}
```

### 3.5 CDC 格式 - Debezium

**路径**: `format/json/debezium/DebeziumJsonDeserializationSchema.java`

**Debezium JSON 消息结构**:
```json
{
  "payload": {
    "before": {"id": 1, "name": "old"},
    "after": {"id": 1, "name": "new"},
    "op": "u",
    "ts_ms": 1640000000000
  }
}
```

**操作类型映射**:

| Debezium Op | RowKind | 说明 |
|-------------|---------|------|
| r (read) | INSERT | 快照读取 |
| c (create) | INSERT | 插入操作 |
| u (update) | UPDATE_BEFORE + UPDATE_AFTER | 更新操作 |
| d (delete) | DELETE | 删除操作 |

```java
private void parsePayload(Collector<SeaTunnelRow> out, TablePath tablePath, JsonNode payload) {
    String op = payload.get(OP_KEY).asText();

    switch (op) {
        case OP_CREATE:
        case OP_READ:
            SeaTunnelRow insert = debeziumRowConverter.parse(payload.get(DATA_AFTER));
            insert.setRowKind(RowKind.INSERT);
            out.collect(insert);
            break;
        case OP_UPDATE:
            SeaTunnelRow before = debeziumRowConverter.parse(payload.get(DATA_BEFORE));
            SeaTunnelRow after = debeziumRowConverter.parse(payload.get(DATA_AFTER));
            before.setRowKind(RowKind.UPDATE_BEFORE);
            after.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(before);
            out.collect(after);
            break;
        case OP_DELETE:
            SeaTunnelRow delete = debeziumRowConverter.parse(payload.get(DATA_BEFORE));
            delete.setRowKind(RowKind.DELETE);
            out.collect(delete);
            break;
    }
}
```

### 3.6 CDC 格式 - Maxwell

**Maxwell JSON 消息结构**:
```json
{
  "data": {"id": 1, "name": "test"},
  "old": {"name": "old_test"},
  "type": "update",
  "database": "mydb",
  "table": "users",
  "ts": 1640000000
}
```

### 3.7 CDC 格式 - OGG (Oracle GoldenGate)

**OGG JSON 消息结构**:
```json
{
  "before": {"id": 1, "name": "old"},
  "after": {"id": 1, "name": "new"},
  "op_type": "U",
  "table": "MYDB.USERS"
}
```

---

## 四、文本格式模块

### 4.1 TextDeserializationSchema

**路径**: `format/text/TextDeserializationSchema.java`

**功能**: 解析分隔符分隔的文本数据（如 Hive 表数据）

```java
public class TextDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private final SeaTunnelRowType seaTunnelRowType;
    private final String[] separators;   // 多级分隔符
    private final String encoding;
    private final String nullFormat;
    private final TextLineSplitor splitor;

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        String content = new String(message, EncodingUtils.tryParseCharset(encoding));
        Map<Integer, String> splitsMap = splitLineBySeaTunnelRowType(content, seaTunnelRowType, 0);
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = convert(splitsMap.get(i), seaTunnelRowType.getFieldType(i), 0, fieldNames[i]);
        }
        return new SeaTunnelRow(objects);
    }
}
```

**多级分隔符支持**:

```java
// 默认分隔符定义
public static final String[] SEPARATOR = {
    "\u0001",  // Level 0: 字段分隔符 (Hive 默认)
    "\u0002",  // Level 1: 数组元素分隔符
    "\u0003"   // Level 2: Map 键值分隔符
};
```

**配置示例**:
```hocon
source {
  File {
    format = "text"
    field_delimiter = "\t"       # 自定义字段分隔符
    array_delimiter = ","        # 数组元素分隔符
    encoding = "UTF-8"
    null_format = "\\N"
  }
}
```

### 4.2 TextLineSplitor

**路径**: `format/text/splitor/TextLineSplitor.java`

```java
public interface TextLineSplitor extends Serializable {
    String[] spliteLine(String line, String delimiter);
}

// 默认实现
public class DefaultTextLineSplitor implements TextLineSplitor {
    @Override
    public String[] spliteLine(String line, String delimiter) {
        return line.split(delimiter, -1);
    }
}

// CSV 实现（处理引号）
public class CsvLineSplitor implements TextLineSplitor {
    // 处理引号包围的字段
}
```

---

## 五、CSV 格式模块

### 5.1 CsvDeserializationSchema

**路径**: `format/csv/CsvDeserializationSchema.java`

**功能**: 专门处理 CSV 格式，支持引号、转义等

```java
public class CsvDeserializationSchema implements Serializable {

    private final SeaTunnelRowType seaTunnelRowType;
    private final String[] separators;
    private final String encoding;
    private final String nullFormat;
    private final CsvLineProcessor processor;  // CSV 行处理器

    protected SeaTunnelRow deserialize(byte[] message) throws IOException {
        String content = new String(message, EncodingUtils.tryParseCharset(encoding));
        Map<Integer, String> splitsMap = splitLineBySeaTunnelRowType(content, seaTunnelRowType, 0);
        return getSeaTunnelRow(splitsMap);
    }
}
```

### 5.2 CsvLineProcessor

**路径**: `format/csv/processor/CsvLineProcessor.java`

```java
public interface CsvLineProcessor extends Serializable {
    String[] splitLine(String line, String delimiter);
}

// 默认实现
public class DefaultCsvLineProcessor implements CsvLineProcessor {
    @Override
    public String[] splitLine(String line, String delimiter) {
        // 处理引号包围的字段
        // 处理转义字符
        // 处理空字段
    }
}
```

### 5.3 CsvStringQuoteMode

```java
public enum CsvStringQuoteMode {
    MINIMAL,      // 仅在必要时加引号
    ALL,          // 所有字符串加引号
    NON_NUMERIC,  // 非数字类型加引号
    NONE          // 不加引号
}
```

---

## 六、Avro 格式模块

### 6.1 AvroDeserializationSchema

**路径**: `format/avro/AvroDeserializationSchema.java`

```java
public class AvroDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private final SeaTunnelRowType rowType;
    private final AvroToRowConverter converter;

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        GenericRecord record = this.converter.getReader().read(null, decoder);
        return converter.converter(record, rowType);
    }
}
```

### 6.2 类型转换

**SeaTunnelRowType → Avro Schema**:

**路径**: `format/avro/SeaTunnelRowTypeToAvroSchemaConverter.java`

| SeaTunnel Type | Avro Type |
|----------------|-----------|
| BOOLEAN | boolean |
| TINYINT | int |
| SMALLINT | int |
| INT | int |
| BIGINT | long |
| FLOAT | float |
| DOUBLE | double |
| STRING | string |
| BYTES | bytes |
| DATE | int (logicalType: date) |
| TIME | int (logicalType: time-millis) |
| TIMESTAMP | long (logicalType: timestamp-millis) |
| DECIMAL | bytes (logicalType: decimal) |
| ARRAY | array |
| MAP | map |
| ROW | record |

---

## 七、Protobuf 格式模块

### 7.1 ProtobufDeserializationSchema

**路径**: `format/protobuf/ProtobufDeserializationSchema.java`

```java
public class ProtobufDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private final SeaTunnelRowType rowType;
    private final ProtobufToRowConverter converter;
    private final String protoContent;      // .proto 文件内容
    private final String messageName;       // 消息类型名

    public ProtobufDeserializationSchema(CatalogTable catalogTable) {
        this.messageName = catalogTable.getOptions().get("protobuf_message_name");
        this.protoContent = catalogTable.getOptions().get("protobuf_schema");
        this.converter = new ProtobufToRowConverter(protoContent, messageName);
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        Descriptors.Descriptor descriptor = this.converter.getDescriptor();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, message);
        return this.converter.converter(descriptor, dynamicMessage, rowType);
    }
}
```

### 7.2 动态 Schema 编译

**路径**: `format/protobuf/CompileDescriptor.java`

```java
// 运行时编译 .proto 定义
public class CompileDescriptor {
    public static Descriptors.Descriptor compile(String protoContent, String messageName) {
        // 解析 .proto 文件内容
        // 动态生成 Descriptor
    }
}
```

---

## 八、兼容格式模块

### 8.1 Debezium 兼容格式

**路径**: `seatunnel-format-compatible-debezium-json/`

**功能**: 兼容 Debezium 原生 JSON 格式（包含 Schema 信息）

```java
public class CompatibleDebeziumJsonDeserializationSchema
    implements DeserializationSchema<SeaTunnelRow> {

    // 支持 Debezium 包含 schema 的完整格式
    // {
    //   "schema": { ... },
    //   "payload": { ... }
    // }
}
```

### 8.2 Kafka Connect 兼容格式

**路径**: `seatunnel-format-compatible-connect-json/`

**功能**: 兼容 Kafka Connect JSON 格式

```java
public class CompatibleKafkaConnectDeserializationSchema
    implements DeserializationSchema<SeaTunnelRow> {

    // 支持 Kafka Connect 格式的 CDC 消息
}
```

---

## 九、格式使用示例

### 9.1 JSON 格式

```hocon
source {
  Kafka {
    topic = "user_events"
    format = "json"
    format.fail_on_missing_field = false
    format.ignore_parse_errors = true
    schema = {
      fields {
        id = "bigint"
        name = "string"
        created_at = "timestamp"
      }
    }
  }
}
```

### 9.2 Canal CDC 格式

```hocon
source {
  Kafka {
    topic = "canal_user"
    format = "canal_json"
    format.database = "mydb"
    format.table = "users"
    schema = {
      fields {
        id = "bigint"
        name = "string"
      }
    }
  }
}
```

### 9.3 Debezium CDC 格式

```hocon
source {
  Kafka {
    topic = "debezium.mydb.users"
    format = "debezium_json"
    format.debezium_enabled_schema = true
    schema = {
      fields {
        id = "bigint"
        name = "string"
      }
    }
  }
}
```

### 9.4 Text 格式

```hocon
source {
  File {
    path = "/data/users.txt"
    format = "text"
    field_delimiter = "\t"
    encoding = "UTF-8"
    null_format = "\\N"
    schema = {
      fields {
        id = "bigint"
        name = "string"
      }
    }
  }
}
```

### 9.5 CSV 格式

```hocon
source {
  File {
    path = "/data/users.csv"
    format = "csv"
    field_delimiter = ","
    quote_char = "\""
    encoding = "UTF-8"
    schema = {
      fields {
        id = "bigint"
        name = "string"
      }
    }
  }
}
```

### 9.6 Avro 格式

```hocon
source {
  Kafka {
    topic = "user_avro"
    format = "avro"
    schema = {
      fields {
        id = "bigint"
        name = "string"
      }
    }
  }
}
```

### 9.7 Protobuf 格式

```hocon
source {
  Kafka {
    topic = "user_protobuf"
    format = "protobuf"
    protobuf_message_name = "User"
    protobuf_schema = """
      syntax = "proto3";
      message User {
        int64 id = 1;
        string name = 2;
      }
    """
    schema = {
      fields {
        id = "bigint"
        name = "string"
      }
    }
  }
}
```

---

## 十、设计模式与最佳实践

### 10.1 使用的设计模式

| 模式 | 应用场景 |
|------|----------|
| **转换器模式** | JsonToRowConverters 类型转换 |
| **Builder 模式** | TextDeserializationSchema.Builder |
| **策略模式** | TextLineSplitor 不同的分割策略 |
| **装饰器模式** | wrapIntoNullableConverter 添加 null 处理 |

### 10.2 性能优化技术

1. **格式化器缓存**: `fieldFormatterMap` 缓存日期格式化器
2. **类型检测优化**: `jsonNode.canConvertToInt()` 避免字符串解析
3. **对象复用**: `ObjectNode node` 复用减少 GC
4. **延迟初始化**: 转换器按需创建

### 10.3 错误处理策略

```java
// 两种互斥的错误处理模式
if (ignoreParseErrors && failOnMissingField) {
    throw new SeaTunnelJsonFormatException(
        "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
}
```

| 模式 | 行为 |
|------|------|
| `failOnMissingField=true` | 字段缺失抛出异常 |
| `ignoreParseErrors=true` | 解析失败返回 null |
| 两者都为 false | 字段缺失返回 null，解析失败抛出异常 |

---

## 十一、格式对比

### 11.1 性能对比

| 格式 | 序列化速度 | 反序列化速度 | 消息大小 | Schema |
|------|------------|--------------|----------|--------|
| JSON | 中等 | 中等 | 大 | 自描述 |
| Text | 快 | 快 | 中等 | 无 |
| CSV | 快 | 快 | 中等 | 无 |
| Avro | 快 | 快 | 小 | 外部 |
| Protobuf | 最快 | 最快 | 最小 | 外部 |

### 11.2 功能对比

| 格式 | CDC 支持 | 复杂类型 | 可读性 | 适用场景 |
|------|----------|----------|--------|----------|
| JSON | 是（多种） | 完整 | 高 | 通用、调试 |
| Text | 否 | 有限 | 高 | 日志、Hive |
| CSV | 否 | 有限 | 高 | 数据交换 |
| Avro | 否 | 完整 | 低 | 大数据生态 |
| Protobuf | 否 | 完整 | 低 | 高性能场景 |

---

## 十二、总结

`seatunnel-formats` 模块是 SeaTunnel 数据处理的基础设施，具有以下特点：

1. **格式丰富**: 支持 JSON、Text、CSV、Avro、Protobuf 等主流格式
2. **CDC 完整支持**: Canal、Debezium、Maxwell、OGG 四种 CDC 格式
3. **类型系统完整**: 支持所有 SeaTunnel 数据类型的转换
4. **错误处理灵活**: failOnMissingField 和 ignoreParseErrors 两种模式
5. **性能优化**: 格式化器缓存、类型检测优化等

---

**相关文档**:
- [005-seatunnel-api模块源码分析.md](./005-seatunnel-api模块源码分析.md) - 序列化接口定义
- [006-seatunnel-connectors-v2模块源码分析.md](./006-seatunnel-connectors-v2模块源码分析.md) - 连接器中的格式使用

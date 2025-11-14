# ClickHouse查询方言

<cite>
**Referenced Files in This Document**   
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts)
- [clickHouseExternal.ts](file://src/external/clickHouseExternal.ts)
- [baseDialect.ts](file://src/dialect/baseDialect.ts)
</cite>

## 目录
1. [简介](#简介)
2. [核心实现机制](#核心实现机制)
3. [ClickHouse特有功能支持](#clickhouse特有功能支持)
4. [查询转换逻辑](#查询转换逻辑)
5. [高级特性应用](#高级特性应用)
6. [性能优化策略](#性能优化策略)
7. [版本兼容性与差异分析](#版本兼容性与差异分析)
8. [结论](#结论)

## 简介

ClickHouse查询方言是Plywood框架中用于将Plywood表达式转换为ClickHouse SQL语句的核心组件。该文档全面阐述了`clickHouseDialect.ts`的实现机制，重点描述了Plywood表达式到ClickHouse SQL语句的转换逻辑。文档详细说明了ClickHouse特有功能的支持，包括近似计算函数、数组和嵌套类型处理、采样查询等高级特性，并通过实际示例展示高基数聚合、稀疏数据处理等场景下的查询生成。

**Section sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L1-L20)
- [clickHouseExternal.ts](file://src/external/clickHouseExternal.ts#L1-L10)

## 核心实现机制

ClickHouse方言的核心实现基于`SQLDialect`抽象类，通过继承和重写基类方法来实现ClickHouse特定的SQL语法转换。`ClickHouseDialect`类定义了多个静态映射表，用于处理时间分桶、时间部分提取和类型转换等操作。

```mermaid
classDiagram
class SQLDialect {
<<abstract>>
+nullConstant() string
+escapeName(name) string
+escapeLiteral(name) string
+timeToSQL(date) string
+concatExpression(a, b) string
+containsExpression(a, b) string
+isNotDistinctFromExpression(a, b) string
+castExpression(inputType, operand, cast) string
+timeFloorExpression(operand, duration, timezone) string
+timeBucketExpression(operand, duration, timezone) string
+timePartExpression(operand, part, timezone) string
+timeShiftExpression(operand, duration, timezone) string
+extractExpression(operand, regexp) string
+indexOfExpression(str, substr) string
}
class ClickHouseDialect {
+TIME_BUCKETING Record~string, string~
+DATE_TIME_FN Record~string, string~
+TIME_PART_TO_FUNCTION Record~string, string~
+CAST_TO_FUNCTION Record~string, Record~string, string~~
+escapeName(name) string
+escapeLiteral(name) string
+timeToSQL(date) string
+concatExpression(a, b) string
+containsExpression(a, b) string
+isNotDistinctFromExpression(a, b) string
+castExpression(inputType, operand, cast) string
+utcToWalltime(operand, timezone) string
+walltimeToUTC(operand, timezone) string
+timeFloorExpression(operand, duration, timezone) string
+timeBucketExpression(operand, duration, timezone) string
+timePartExpression(operand, part, timezone) string
+timeShiftExpression(operand, duration, timezone) string
+extractExpression(operand, regexp) string
+indexOfExpression(str, substr) string
}
ClickHouseDialect --|> SQLDialect : 继承
```

**Diagram sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L4-L161)
- [baseDialect.ts](file://src/dialect/baseDialect.ts#L43-L172)

**Section sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L4-L161)
- [baseDialect.ts](file://src/dialect/baseDialect.ts#L43-L172)

## ClickHouse特有功能支持

### 时间处理功能

ClickHouse方言提供了完善的时间处理功能，通过静态映射表实现时间分桶和时间部分提取。`TIME_BUCKETING`和`DATE_TIME_FN`映射表定义了不同时间粒度的格式化字符串和对应的ClickHouse函数。

```mermaid
flowchart TD
Start["时间分桶请求"] --> CheckDuration["检查时间粒度"]
CheckDuration --> |PT1S| ToStartOfSecond["调用toStartOfSecond()"]
CheckDuration --> |PT1M| ToStartOfMinute["调用toStartOfMinute()"]
CheckDuration --> |PT1H| ToStartOfHour["调用toStartOfHour()"]
CheckDuration --> |P1D| ToDate["调用toDate()"]
CheckDuration --> |P1W| ToStartOfWeek["调用toStartOfWeek()"]
CheckDuration --> |P1M| ToStartOfMonth["调用toStartOfMonth()"]
CheckDuration --> |P3M| ToStartOfQuarter["调用toStartOfQuarter()"]
CheckDuration --> |P1Y| ToStartOfYear["调用toStartOfYear()"]
ToStartOfSecond --> Format["格式化输出"]
ToStartOfMinute --> Format
ToStartOfHour --> Format
ToDate --> Format
ToStartOfWeek --> Format
ToStartOfMonth --> Format
ToStartOfQuarter --> Format
ToStartOfYear --> Format
Format --> Return["返回SQL表达式"]
```

**Diagram sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L10-L33)

### 类型转换功能

ClickHouse方言通过`CAST_TO_FUNCTION`静态映射表实现类型转换功能，支持在时间、数字和字符串类型之间的相互转换。

```mermaid
erDiagram
CAST_TO_FUNCTION {
string inputType
string outputType
string functionTemplate
}
CAST_TO_FUNCTION ||--o{ TIME_CONVERSION : "时间转换"
CAST_TO_FUNCTION ||--o{ NUMBER_CONVERSION : "数字转换"
CAST_TO_FUNCTION ||--o{ STRING_CONVERSION : "字符串转换"
TIME_CONVERSION {
string fromNumber
string toTime
}
NUMBER_CONVERSION {
string fromTime
string fromString
string toNumber
}
STRING_CONVERSION {
string fromNumber
string toString
}
```

**Diagram sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L50-L58)

**Section sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L50-L58)

## 查询转换逻辑

### 表达式转换流程

Plywood表达式到ClickHouse SQL的转换流程遵循严格的处理逻辑，从基本的字符串操作到复杂的时间函数转换。

```mermaid
sequenceDiagram
participant PlywoodExpr as Plywood表达式
participant Dialect as ClickHouseDialect
participant SQL as ClickHouse SQL
PlywoodExpr->>Dialect : 调用转换方法
Dialect->>Dialect : 检查表达式类型
alt 字符串连接
Dialect->>Dialect : 调用concatExpression()
Dialect->>SQL : 生成CONCAT()函数
else 包含检查
Dialect->>Dialect : 调用containsExpression()
Dialect->>SQL : 生成LOCATE()>0表达式
else 类型转换
Dialect->>Dialect : 调用castExpression()
Dialect->>Dialect : 查找CAST_TO_FUNCTION映射
Dialect->>SQL : 生成CAST()表达式
else 时间分桶
Dialect->>Dialect : 调用timeFloorExpression()
Dialect->>Dialect : 查找DATE_TIME_FN映射
Dialect->>SQL : 生成时间分桶SQL
end
Dialect->>SQL : 返回转换后的SQL
```

**Diagram sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L76-L135)

### 时间函数转换

时间函数的转换是ClickHouse方言的核心功能之一，通过`timePartExpression`方法实现各种时间部分的提取。

```mermaid
flowchart TD
A["timePartExpression调用"] --> B["查找TIME_PART_TO_FUNCTION映射"]
B --> C{"是否找到对应函数?"}
C --> |是| D["替换$$占位符为实际操作数"]
C --> |否| E["抛出异常"]
D --> F["返回转换后的SQL表达式"]
E --> G["返回错误信息"]
```

**Diagram sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L137-L148)

**Section sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L137-L148)

## 高级特性应用

### 近似计算支持

虽然当前实现中`extractExpression`方法尚未实现近似计算功能，但代码注释指明了实现方向，需要集成MySQL UDF库来支持正则表达式提取。

```mermaid
flowchart LR
A["extractExpression方法"] --> B["抛出未实现异常"]
B --> C["需要实现mysqludf/lib_mysqludf_preg"]
C --> D["支持正则表达式提取"]
D --> E["实现近似计算功能"]
```

**Section sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L150-L152)

### 数组和嵌套类型处理

ClickHouse方言通过`ClickHouseExternal`类的`postProcessIntrospect`方法处理ClickHouse的原生类型，包括数组和嵌套类型。

```mermaid
classDiagram
class ClickHouseExternal {
+engine string
+type string
+fromJS(parameters, requester) ClickHouseExternal
+postProcessIntrospect(columns) Attributes
+getSourceList(requester) Promise~string[]~
+getVersion(requester) Promise~string~
+getIntrospectAttributes() Promise~Attributes~
+capability(cap) boolean
}
class ClickHouseDescribeRow {
+name string
+type string
}
ClickHouseExternal --> ClickHouseDescribeRow : 使用
```

**Diagram sources**
- [clickHouseExternal.ts](file://src/external/clickHouseExternal.ts#L8-L23)

### 采样查询支持

ClickHouse方言通过`capability`方法控制查询能力，禁用了某些不支持的功能，同时启用了字符串分组功能。

```mermaid
flowchart TD
A["capability方法调用"] --> B{"功能检查"}
B --> |filter-on-attribute| C["返回false"]
B --> |shortcut-group-by| D["返回false"]
B --> |string-group-by| E["返回true"]
B --> |其他功能| F["调用父类capability"]
```

**Diagram sources**
- [clickHouseExternal.ts](file://src/external/clickHouseExternal.ts#L94-L103)

**Section sources**
- [clickHouseExternal.ts](file://src/external/clickHouseExternal.ts#L94-L103)

## 性能优化策略

### 高基数聚合处理

ClickHouse方言通过优化时间函数和类型转换来提高高基数聚合的性能。时间分桶操作使用ClickHouse内置的高效函数，避免了复杂的字符串操作。

```mermaid
flowchart LR
A["高基数聚合查询"] --> B["使用toStartOf*函数"]
B --> C["避免字符串格式化"]
C --> D["直接时间操作"]
D --> E["提高查询性能"]
```

**Section sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L20-L30)

### 稀疏数据处理

对于稀疏数据的处理，ClickHouse方言通过`isNotDistinctFromExpression`方法实现了NULL值的特殊处理，确保在稀疏数据场景下的查询准确性。

```mermaid
flowchart TD
A["稀疏数据查询"] --> B["调用isNotDistinctFromExpression"]
B --> C["生成(a=b)表达式"]
C --> D["正确处理NULL值"]
D --> E["确保查询准确性"]
```

**Section sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L74-L75)

## 版本兼容性与差异分析

### 版本兼容性

ClickHouse方言通过`ClickHouseExternal`类的静态方法支持版本检查和源列表获取，确保与不同版本的ClickHouse服务器兼容。

```mermaid
sequenceDiagram
participant Client as 客户端
participant External as ClickHouseExternal
participant Server as ClickHouse服务器
Client->>External : 调用getVersion()
External->>Server : 发送"SELECT version()"查询
Server-->>External : 返回版本信息
External-->>Client : 返回Promise<string>
Client->>External : 调用getSourceList()
External->>Server : 发送"SHOW TABLES"查询
Server-->>External : 返回表列表
External-->>Client : 返回Promise<string[]>
```

**Diagram sources**
- [clickHouseExternal.ts](file://src/external/clickHouseExternal.ts#L64-L83)

### 与其他列式数据库方言的差异

ClickHouse方言与其他列式数据库方言的主要差异体现在时间处理函数、字符串操作和类型转换上。

```mermaid
graph TD
A["SQLDialect"] --> B["ClickHouseDialect"]
A --> C["DruidDialect"]
A --> D["MySqlDialect"]
A --> E["PostgresDialect"]
B --> F["使用toStartOf*函数"]
B --> G["使用CONCAT()函数"]
B --> H["使用CAST()函数"]
C --> I["使用timeFloor函数"]
C --> J["使用CONCAT()函数"]
C --> K["使用CAST()函数"]
D --> L["使用DATE_FORMAT()函数"]
D --> M["使用CONCAT()函数"]
D --> N["使用CAST()函数"]
E --> O["使用date_trunc()函数"]
P --> Q["使用||操作符"]
P --> R["使用::类型转换"]
```

**Section sources**
- [clickHouseDialect.ts](file://src/dialect/clickHouseDialect.ts#L4-L161)
- [druidDialect.ts](file://src/dialect/druidDialect.ts#L1-L20)
- [mySqlDialect.ts](file://src/dialect/mySqlDialect.ts#L1-L20)
- [postgresDialect.ts](file://src/dialect/postgresDialect.ts#L1-L20)

## 结论

ClickHouse查询方言通过继承`SQLDialect`基类并重写特定方法，实现了Plywood表达式到ClickHouse SQL语句的高效转换。方言充分利用了ClickHouse的特有功能，如高效的时间处理函数和类型转换机制，为高基数聚合和稀疏数据处理提供了优化支持。尽管某些高级功能如近似计算尚未完全实现，但整体架构为未来功能扩展提供了良好的基础。通过与`ClickHouseExternal`类的协同工作，方言能够处理ClickHouse特有的数据类型和查询能力，确保了与ClickHouse服务器的良好兼容性。
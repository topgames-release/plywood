# PostgreSQL数据源

<cite>
**Referenced Files in This Document**   
- [postgresExternal.ts](file://src/external/postgresExternal.ts)
- [sqlExternal.ts](file://src/external/sqlExternal.ts)
- [attributeInfo.ts](file://src/datatypes/attributeInfo.ts)
- [types.ts](file://src/types.ts)
- [postgresDialect.ts](file://src/dialect/postgresDialect.ts)
</cite>

## 目录
1. [简介](#简介)
2. [核心实现](#核心实现)
3. [表结构查询与数据类型处理](#表结构查询与数据类型处理)
4. [数据源列表与版本查询](#数据源列表与版本查询)
5. [配置与使用示例](#配置与使用示例)

## 简介
`PostgresExternal` 类是Plywood框架中用于连接和操作PostgreSQL数据库的核心组件。它继承自 `SQLExternal` 基类，专门针对PostgreSQL的特性进行了实现和优化。该类的主要职责是通过执行SQL查询来获取数据库的元数据（如表结构），并将PostgreSQL特有的数据类型正确地映射到Plywood内部的类型系统中，从而为上层应用提供一致的数据访问接口。

**Section sources**
- [postgresExternal.ts](file://src/external/postgresExternal.ts#L1-L30)

## 核心实现
`PostgresExternal` 类的实现围绕着 `postProcessIntrospect`、`getSourceList` 和 `getVersion` 这三个静态方法展开。这些方法共同构成了与PostgreSQL数据库交互的基础。

类定义中，`static engine = "postgres"` 明确指定了其服务的数据库引擎，而 `fromJS` 和构造函数则负责根据配置参数创建实例。构造函数中调用的 `this._ensureEngine("postgres")` 确保了实例的引擎类型正确无误。

**Section sources**
- [postgresExternal.ts](file://src/external/postgresExternal.ts#L31-L45)

## 表结构查询与数据类型处理
### postProcessIntrospect 方法
`postProcessIntrospect` 方法是处理表结构查询结果的核心。它接收一个包含列信息的数组，并将其转换为Plywood内部的 `Attributes` 类型。

该方法首先从 `INFORMATION_SCHEMA.COLUMNS` 和 `INFORMATION_SCHEMA.ELEMENT_TYPES` 表中查询列的名称 (`column_name`)、SQL数据类型 (`data_type`) 以及对于数组类型，其元素的类型 (`element_types.data_type` 通过 `arrayType` 字段获取)。

```mermaid
flowchart TD
A[开始] --> B[遍历查询结果中的每一列]
B --> C{数据类型是否为 "array"?}
C --> |是| D[获取 arrayType 字段]
D --> E{arrayType 是什么类型?}
E --> |character| F[映射为 SET/STRING]
E --> |timestamp| G[映射为 SET/TIME]
E --> |integer, bigint, double precision, float| H[映射为 SET/NUMBER]
E --> |boolean| I[映射为 SET/BOOLEAN]
E --> |其他| J[返回 null]
C --> |否| K{判断其他基本类型}
K --> |timestamp| L[映射为 TIME]
K --> |character varying| M[映射为 STRING]
K --> |integer, bigint| N[映射为 NUMBER]
K --> |double precision, float| O[映射为 NUMBER]
K --> |boolean| P[映射为 BOOLEAN]
K --> |其他| Q[返回 null]
F --> R[创建 AttributeInfo]
G --> R
H --> R
I --> R
L --> R
M --> R
N --> R
O --> R
P --> R
R --> S[过滤掉 null 值]
S --> T[返回 Attributes 数组]
```

**Diagram sources**
- [postgresExternal.ts](file://src/external/postgresExternal.ts#L47-L95)

**Section sources**
- [postgresExternal.ts](file://src/external/postgresExternal.ts#L47-L95)
- [attributeInfo.ts](file://src/datatypes/attributeInfo.ts#L49-L229)
- [types.ts](file://src/types.ts#L22-L22)

### 数组类型支持
PostgreSQL的数组类型是其一大特色。`PostgresExternal` 通过 `arrayType` 字段识别数组元素的类型，并将其映射为Plywood的集合类型（SET）。例如，`integer[]` 被映射为 `SET/NUMBER`，`timestamp[]` 被映射为 `SET/TIME`。这种映射确保了在Plywood中可以对数组数据进行集合操作，如去重、求交集等。

## 数据源列表与版本查询
### getSourceList 方法
`getSourceList` 方法用于获取数据库中所有可用的表名。它通过执行一个标准的SQL查询来实现：

```sql
SELECT table_name AS "tab" 
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_type = 'BASE TABLE' AND table_schema = 'public'
```

此查询明确指定了 `table_schema = 'public'`，这意味着它只返回位于 `public` 模式下的表。查询结果是一个包含表名的对象数组，该方法会将其提取并排序后返回。

**Section sources**
- [postgresExternal.ts](file://src/external/postgresExternal.ts#L97-L106)

### getVersion 方法
`getVersion` 方法用于获取PostgreSQL服务器的版本号。它执行 `SELECT version()` 查询，该查询返回一个包含详细版本信息的字符串，例如：
`PostgreSQL 13.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-44), 64-bit`

该方法通过正则表达式 `/^PostgreSQL (\S+) on/` 提取其中的核心版本号（如 `13.4`），并将其返回。这种处理方式能够从复杂的返回结果中准确地提取出用户关心的版本信息。

**Section sources**
- [postgresExternal.ts](file://src/external/postgresExternal.ts#L108-L122)

## 配置与使用示例
要使用 `PostgresExternal`，需要提供一个包含数据库连接信息的配置对象。以下是一个典型的配置示例：

```javascript
const postgresConfig = {
  engine: 'postgres',
  source: 'my_table', // 要查询的表名
  requester: myRequester, // 一个能执行SQL查询的请求器
  attributes: [
    // 可以预先定义属性，或让系统通过 introspect 自动发现
  ]
};

// 创建 PostgresExternal 实例
const external = PostgresExternal.fromJS(postgresConfig);
```

通过上述配置，Plywood即可与PostgreSQL数据库建立连接，并利用 `PostgresExternal` 提供的功能进行数据查询和分析。

**Section sources**
- [postgresExternal.ts](file://src/external/postgresExternal.ts#L35-L41)
- [postgresExternal.ts](file://src/external/postgresExternal.ts#L124-L135)
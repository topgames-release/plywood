# API参考

<cite>
**本文档中引用的文件**  
- [src/index.ts](file://src/index.ts)
- [typings/public.d.ts](file://typings/public.d.ts)
- [src/expressions/index.ts](file://src/expressions/index.ts)
- [src/datatypes/index.ts](file://src/datatypes/index.ts)
- [src/external/index.ts](file://src/external/index.ts)
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts)
- [src/datatypes/dataset.ts](file://src/datatypes/dataset.ts)
</cite>

## 目录
1. [简介](#简介)
2. [核心API入口点](#核心api入口点)
3. [表达式系统](#表达式系统)
4. [数据类型与数据集](#数据类型与数据集)
5. [外部数据源](#外部数据源)
6. [计算与执行](#计算与执行)
7. [类型定义](#类型定义)

## 简介
Plywood 是一个用于构建复杂数据分析查询的库，其核心是基于表达式的查询语言。本API参考文档详细描述了通过 `src/index.ts` 暴露的所有公共接口。这些接口允许开发者以声明式的方式构建、操作和执行针对外部数据源（如Druid、MySQL等）的查询。文档基于 `typings/public.d.ts` 中的类型定义，确保了API签名的精确性。

## 核心API入口点

### `compute()` 方法
`compute()` 是执行异步表达式计算的主要入口点。它负责解析上下文、应用环境配置、解析引用并最终执行查询。

**功能描述**：该方法首先对表达式进行预处理，包括定义环境、检查引用和解析上下文。如果启用了 `useSubtotalsSpec` 优化，则会使用专门的优化路径 `_computeWithSubtotalsSpec`；否则，使用标准的 `_computeResolved` 流程。

**参数说明**：
- `context` (`Datum`): 计算的上下文，通常包含数据源（如 `DruidExternal`）和其他变量。
- `options` (`ComputeOptions`): 控制计算行为的选项，如查询超时、并发限制等。

**返回值**：返回一个 `Promise<PlywoodValue>`，解析为计算结果，如 `Dataset`、`Number` 或 `TimeRange`。

**使用示例**：
```typescript
// 伪代码示例
const query = $('main').split(...).apply(...);
query.compute(context, { timeout: 10000 }).then(result => {
  console.log(result);
});
```

**Section sources**
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts#L2400-L2499)

### `Expression.parse()` 方法
`Expression.parse()` 是将字符串形式的Plywood表达式解析为可执行的 `Expression` 对象的静态方法。

**功能描述**：该方法接受一个字符串（如 `"$main.filter($time.in('2023'))"`）并将其解析为一个表达式树。它支持JSON格式的表达式和简化的字符串语法。

**参数说明**：
- `str` (`string`): 要解析的表达式字符串。
- `timezone` (`Timezone`, 可选): 用于解析日期字符串的时区。

**返回值**：返回一个 `Expression` 对象。

**使用示例**：
```typescript
const expr = Expression.parse("$main.sum($revenue)");
```

**Section sources**
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts#L100-L150)

## 表达式系统

### `Expression` 基类
`Expression` 是所有表达式类型的抽象基类，定义了表达式的核心行为和API。

**功能描述**：它提供了表达式序列化（`toJS`, `fromJS`）、遍历（`forEach`, `every`）、替换（`substitute`）、简化（`simplify`）和计算（`calc`）等通用方法。所有具体的表达式（如 `AddExpression`, `FilterExpression`）都继承自此类。

**关键方法**：
- `substitute()`: 递归地将表达式中的子表达式替换为新表达式。
- `simplify()`: 返回一个逻辑等价但更简单的表达式。
- `getFn()`: 返回一个函数，该函数可以在给定数据行上计算表达式的值。

**Section sources**
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts#L200-L800)

### `ChainableExpression` 类
`ChainableExpression` 是 `Expression` 的子类，代表可以链接到其他表达式上的操作（如 `.filter()`, `.add()`）。

**功能描述**：它引入了 `operand` 属性，指向链中的前一个表达式。这使得可以构建类似 `$('main').filter(...).split(...)` 的链式调用。

**关键属性**：
- `operand`: 链中前一个表达式。

**Section sources**
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts#L2400-L2600)

### `ChainableUnaryExpression` 类
`ChainableUnaryExpression` 是 `ChainableExpression` 的子类，代表接受一个额外参数的链式操作（如 `.add($x)`, `.split(...)`）。

**功能描述**：它引入了 `expression` 属性，用于存储操作的参数。例如，在 `$('main').add($'revenue')` 中，`$'revenue'` 就是 `expression`。

**关键属性**：
- `expression`: 操作的参数表达式。

**Section sources**
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts#L2600-L2800)

## 数据类型与数据集

### `Dataset` 类
`Dataset` 类代表一个数据集，包含一个数据行（`Datum`）的数组和元数据（`Attributes`）。

**功能描述**：它是查询的主要输入和输出。`Dataset` 提供了多种数据操作方法，如选择列、应用计算、过滤、排序和聚合。

**关键方法**：
- `apply(name, ex)`: 计算一个新列并将其添加到数据集中。
- `filter(ex)`: 根据布尔表达式过滤数据行。
- `sum(ex)`, `count()`, `average(ex)`: 执行聚合计算。

**Section sources**
- [src/datatypes/dataset.ts](file://src/datatypes/dataset.ts#L0-L1428)

### `Datum` 接口
`Datum` 接口定义了单个数据行的结构。

**功能描述**：它是一个键值对的映射，其中键是属性名，值是 `PlywoodValue` 类型，可以是原始值（`number`, `string`, `Date`）或复杂类型（`NumberRange`, `TimeRange`, `Dataset`）。

**Section sources**
- [src/datatypes/dataset.ts](file://src/datatypes/dataset.ts#L0-L1428)

## 外部数据源

### `External` 类
`External` 类是所有外部数据源（如 `DruidExternal`, `MySqlExternal`）的基类。

**功能描述**：它定义了与外部系统交互的通用接口，如执行查询（`queryValue`）、模拟查询（`simulateValue`）和处理数据转换（`Inflater`）。

**关键方法**：
- `queryValue(terminal, rawQueries, customOptions)`: 执行查询并返回结果。
- `simulateValue(terminal, simulatedQueryGroup)`: 模拟查询执行，返回查询计划。

**Section sources**
- [src/external/baseExternal.ts](file://src/external/baseExternal.ts)

### `ExternalExpression` 类
`ExternalExpression` 是 `Expression` 的一种，用于将 `External` 对象嵌入到表达式树中。

**功能描述**：它允许将外部数据源作为表达式的一部分，从而可以对来自外部的数据进行操作。

**Section sources**
- [src/expressions/externalExpression.ts](file://src/expressions/externalExpression.ts)

## 计算与执行

### `_computeResolved()` 方法
`_computeResolved()` 是标准的计算流程，通过循环解析和执行表达式中的外部依赖来完成计算。

**功能描述**：它使用 `promiseWhile` 循环，反复调用 `getReadyExternals()` 来发现待执行的外部查询，然后使用 `fillExpressionExternalAlterationAsync()` 执行这些查询，并用结果替换表达式中的外部引用，直到所有外部依赖都被解决。

**关键步骤**：
1.  调用 `getReadyExternals()` 获取待执行的外部查询。
2.  异步执行这些查询。
3.  调用 `applyReadyExternals()` 将查询结果应用到表达式中。
4.  重复以上步骤，直到没有更多外部查询。

**Section sources**
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts#L2400-L2499)

### `_computeWithSubtotalsSpec()` 方法
`_computeWithSubtotalsSpec()` 是一个优化的计算路径，旨在通过合并多个查询来提高性能。

**功能描述**：它首先使用 `simulateQueryPlan()` 生成所有可能的查询计划，然后尝试将这些查询合并为一个带有 `subtotalsSpec` 的单一 `groupBy` 查询，从而减少与外部系统的交互次数。

**关键步骤**：
1.  调用 `simulateQueryPlan()` 获取查询计划。
2.  调用 `_extractSplitExpressionsFromExternals()` 提取所有 `split` 表达式。
3.  调用 `_mergeQueriesWithSubtotalsSpec()` 合并查询。
4.  调用 `_executeSubtotalsQuery()` 执行合并后的查询。

**Section sources**
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts#L3100-L3199)

## 类型定义

### `PlywoodValue` 联合类型
`PlywoodValue` 定义了Plywood中所有可能的值类型。

**定义**：
```typescript
type PlywoodValue = null | boolean | number | string | Date | NumberRange | TimeRange | StringRange | Set | Dataset | External;
```

**说明**：此类型涵盖了从原始数据类型到复杂数据结构的所有值。

**Section sources**
- [src/datatypes/dataset.ts](file://src/datatypes/dataset.ts#L0-L1428)

### `ComputeOptions` 接口
`ComputeOptions` 接口定义了控制计算过程的选项。

**定义**：
```typescript
interface ComputeOptions extends Environment {
  customOptions?: any;
  rawQueries?: any[];
  maxQueries?: number;
  maxRows?: number;
  maxComputeCycles?: number;
  concurrentQueryLimit?: number;
  timeout?: number;
  beforePerSplitRequestFn?: (external: External) => void;
  afterSplitRequestFn?: (queriesMade: number) => void;
}
```

**说明**：这些选项允许开发者配置超时、并发限制、查询日志等。

**Section sources**
- [src/expressions/baseExpression.ts](file://src/expressions/baseExpression.ts#L0-L800)
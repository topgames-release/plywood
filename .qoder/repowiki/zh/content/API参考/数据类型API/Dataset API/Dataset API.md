# Dataset API

<cite>
**本文档引用的文件**   
- [dataset.ts](file://src/datatypes/dataset.ts)
- [dataset.mocha.js](file://test/datatypes/dataset.mocha.js)
</cite>

## 目录
1. [简介](#简介)
2. [Dataset类结构](#dataset类结构)
3. [数据操作方法](#数据操作方法)
4. [聚合方法](#聚合方法)
5. [类型推断与嵌套数据处理](#类型推断与嵌套数据处理)
6. [使用示例](#使用示例)

## 简介
Dataset API 提供了一套完整的数据处理功能，用于创建、转换和分析数据集。该API的核心是`Dataset`类，它封装了数据操作、转换和聚合功能。本文档详细介绍了`Dataset`类的结构、构造方法、核心属性以及各种数据操作和聚合方法的实现机制与使用场景。

**Section sources**
- [dataset.ts](file://src/datatypes/dataset.ts#L313-L1425)

## Dataset类结构
`Dataset`类是数据处理的核心，其主要属性包括：
- **attributes**: 数据集的属性信息数组，包含每个字段的名称和类型
- **keys**: 用于标识数据集的键值数组
- **data**: 实际的数据数组，每个元素是一个包含属性值的对象

`Dataset`类通过`fromJS`静态方法创建实例，该方法接受一个包含数据和属性信息的参数对象。如果未提供属性信息，系统会自动根据数据内容进行类型推断。

**Section sources**
- [dataset.ts](file://src/datatypes/dataset.ts#L313-L1425)

## 数据操作方法
### select方法
`select`方法用于选择数据集中的特定列。它接受一个字符串数组作为参数，返回包含指定列的新数据集。该方法会保留原始数据的行结构，只筛选出指定的列。

### apply方法
`apply`方法用于在数据集中添加新的计算列。它接受一个名称和一个表达式作为参数，将表达式计算结果作为新列添加到数据集中。该方法支持函数式编程风格，可以进行复杂的数值计算和数据转换。

### filter方法
`filter`方法用于根据条件筛选数据。它接受一个表达式作为参数，该表达式定义了筛选条件。只有满足条件的数据行才会被保留在结果数据集中。

### sort方法
`sort`方法用于对数据集进行排序。它接受一个表达式和排序方向（升序或降序）作为参数。排序操作基于表达式的计算结果，可以对数值、字符串或时间类型的数据进行排序。

### limit方法
`limit`方法用于限制返回的数据行数。它接受一个数字参数，返回数据集的前N行。当数据集的行数小于或等于限制值时，返回原始数据集。

**Section sources**
- [dataset.ts](file://src/datatypes/dataset.ts#L313-L1425)

## 聚合方法
### count方法
`count`方法返回数据集中的行数。这是一个简单的计数操作，不接受任何参数。

### sum方法
`sum`方法计算指定列的数值总和。它接受一个表达式作为参数，对该表达式计算结果进行求和。

### average方法
`average`方法计算指定列的平均值。它接受一个表达式作为参数，计算该表达式结果的算术平均值。

### min和max方法
`min`和`max`方法分别返回指定列的最小值和最大值。它们都接受一个表达式作为参数，用于确定比较的值。

### countDistinct方法
`countDistinct`方法计算指定列中不同值的数量。它接受一个表达式作为参数，统计该表达式结果的不同值个数。

### quantile方法
`quantile`方法计算指定列的分位数。它接受一个表达式和分位数值（0-1之间）作为参数，返回对应分位数的值。

### collect方法
`collect`方法收集指定列的所有值，返回一个包含所有值的集合。它接受一个表达式作为参数，将该表达式的所有计算结果收集到一个集合中。

**Section sources**
- [dataset.ts](file://src/datatypes/dataset.ts#L313-L1425)

## 类型推断与嵌套数据处理
### getFullType方法
`getFullType`方法用于获取数据集的完整类型信息。它返回一个包含数据集结构和类型信息的对象，对于嵌套的数据集，会递归地获取其类型信息。这个方法在数据类型推断和模式验证中非常有用。

### hasExternal方法
`hasExternal`方法用于检查数据集中是否包含外部数据源。它返回一个布尔值，指示数据集是否引用了外部数据。这个方法在处理嵌套数据源时特别有用，可以帮助识别数据集中的外部依赖关系。

**Section sources**
- [dataset.ts](file://src/datatypes/dataset.ts#L313-L1425)

## 使用示例
以下是一些常见的使用场景示例：

1. **创建Dataset实例**：通过`Dataset.fromJS()`方法从JSON数据创建数据集实例
2. **数据转换**：使用`select`、`apply`、`filter`等方法对数据进行筛选、计算和转换
3. **执行聚合操作**：使用`count`、`sum`、`average`等方法对数据进行统计分析
4. **处理嵌套数据**：利用`getFullType`方法获取嵌套数据集的类型信息，使用`hasExternal`方法检查外部数据源

这些示例展示了如何结合使用各种方法来完成复杂的数据处理任务。

**Section sources**
- [dataset.ts](file://src/datatypes/dataset.ts#L313-L1425)
- [dataset.mocha.js](file://test/datatypes/dataset.mocha.js#L0-L1783)
# limitSpec 排序修复文档

## 问题描述

在 `src/expressions/baseExpression.ts` 文件中的 `_buildSimpleSplit` 方法存在一个问题：该方法在生成每一层的 split 操作时，没有正确应用 query 中的 `limitSpec` 参数进行排序。

### 具体问题
- `_buildSimpleSplit` 方法应该根据 `limitSpec.columns` 中指定的维度（dimension）和排序方向（direction）来对结果进行排序
- 当前实现忽略了排序规则，导致返回的数据没有按照预期的升序或降序排列

### 期望行为
当 limitSpec 配置如下时：
```json
{
  "type": "default",
  "columns": [
    {
      "dimension": "ad_revenue_roas",
      "direction": "descending"
    }
  ],
  "limit": 10000
}
```

`_buildSimpleSplit` 方法应该：
1. 识别 limitSpec 中的排序配置
2. 根据指定的 dimension（如 "ad_revenue_roas"）进行排序
3. 按照指定的 direction（"ascending" 或 "descending"）确定排序顺序
4. 在生成 split 时应用这些排序规则

## 修复方案

### 1. 添加排序方法
在 `_buildSimpleSplit` 方法中添加了对 `limitSpec` 排序的支持：

```typescript
// 应用 limitSpec 排序
this._applySortingToSplitData(splitData, query);
```

### 2. 实现 `_applySortingToSplitData` 方法
新增了一个私有方法来处理排序逻辑：

```typescript
private _applySortingToSplitData(splitData: any[], query: any): void {
  if (!query || !query.limitSpec || !query.limitSpec.columns || !Array.isArray(query.limitSpec.columns)) {
    return;
  }

  const sortColumns = query.limitSpec.columns;
  if (sortColumns.length === 0) {
    return;
  }

  // 支持多列排序，按照 columns 数组的顺序进行排序
  splitData.sort((a: any, b: any) => {
    for (const sortColumn of sortColumns) {
      const dimension = sortColumn.dimension;
      const direction = sortColumn.direction || "ascending";

      const aValue = a[dimension];
      const bValue = b[dimension];

      let comparison = 0;

      // 处理 null/undefined 值
      if (aValue == null && bValue == null) {
        comparison = 0;
      } else if (aValue == null) {
        comparison = 1; // null 值排在后面
      } else if (bValue == null) {
        comparison = -1; // null 值排在后面
      } else {
        // 根据排序方向进行比较
        if (direction === "descending") {
          // 降序：大的值排在前面
          if (typeof aValue === "number" && typeof bValue === "number") {
            comparison = bValue - aValue;
          } else {
            // 字符串降序比较
            const aStr = String(aValue);
            const bStr = String(bValue);
            comparison = bStr < aStr ? -1 : bStr > aStr ? 1 : 0;
          }
        } else {
          // 升序：小的值排在前面
          if (typeof aValue === "number" && typeof bValue === "number") {
            comparison = aValue - bValue;
          } else {
            // 字符串升序比较
            const aStr = String(aValue);
            const bStr = String(bValue);
            comparison = aStr < bStr ? -1 : aStr > bStr ? 1 : 0;
          }
        }
      }

      // 如果当前列的比较结果不为 0，则返回结果
      if (comparison !== 0) {
        return comparison;
      }

      // 如果当前列相等，继续比较下一列
    }

    return 0;
  });
}
```

## 修复特性

### ✅ 支持的功能
1. **单列排序**：支持按单个维度进行升序或降序排序
2. **多列排序**：支持按多个维度进行复合排序，按 columns 数组顺序优先级排序
3. **数值排序**：正确处理数值类型的排序
4. **字符串排序**：正确处理字符串类型的排序
5. **null 值处理**：null/undefined 值统一排在最后
6. **方向支持**：支持 "ascending" 和 "descending" 两种排序方向
7. **向后兼容**：当没有 limitSpec 时，保持原始数据顺序不变

### 🧪 测试验证
创建了全面的测试用例验证修复：

1. **基本排序测试**：验证单列升序和降序排序
2. **多列排序测试**：验证复合排序逻辑
3. **null 值处理测试**：验证 null/undefined 值的正确处理
4. **无 limitSpec 测试**：验证向后兼容性

所有测试均通过 ✅

## 影响范围

### 修改的文件
- `src/expressions/baseExpression.ts`：主要修复文件

### 新增的方法
- `_applySortingToSplitData(splitData: any[], query: any): void`：处理 limitSpec 排序的私有方法

### 修改的方法
- `_buildSimpleSplit()`：在返回结果前添加排序处理

## 使用示例

修复后，当查询包含 limitSpec 时，返回的数据将按照指定规则排序：

```javascript
// 查询配置
const query = {
  limitSpec: {
    type: "default",
    columns: [
      {
        dimension: "ad_revenue_roas",
        direction: "descending"
      }
    ],
    limit: 10000
  }
};

// 修复前：数据顺序随机
// 修复后：数据按 ad_revenue_roas 降序排列
```

## 总结

此修复解决了 `_buildSimpleSplit` 方法忽略 limitSpec 排序配置的问题，现在该方法能够：

1. ✅ 正确识别 limitSpec 中的排序配置
2. ✅ 根据指定的 dimension 进行排序
3. ✅ 按照指定的 direction 确定排序顺序
4. ✅ 支持多列排序和复杂排序场景
5. ✅ 妥善处理边界情况（null 值、无 limitSpec 等）

修复已通过全面测试验证，确保功能正确性和向后兼容性。

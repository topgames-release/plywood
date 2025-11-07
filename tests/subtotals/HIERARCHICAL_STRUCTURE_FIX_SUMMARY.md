# _buildHierarchicalDataset 方法修复总结

## 问题描述

`_executeSubtotalsQuery` 方法返回的数据结构存在问题：
- 当前执行 subtotalsSpec 查询时，返回的 result 数据结构不正确
- result.js 是一个扁平化的结构，attributes 和 keys 都为空数组
- 期望的正确结构应该像 dataset.js 中普通查询返回的结构一样，是一个有层级的树状结构

## 根本原因分析

通过深入分析发现问题的根本原因：

1. **`_executeSubtotalsQuery` 方法问题**：
   - 在创建 `postTransform` 时使用了空的 `attributes` 和 `keys` 参数
   - 缺少将扁平化 subtotalsSpec 结果转换为层级结构的逻辑

2. **`_buildSplitAttributes` 方法问题**：
   - 总是添加 `SPLIT` 属性，即使在最深层级也是如此
   - 导致最深层级也有 `SPLIT` 属性，但实际上最深层级不应该有嵌套的 `SPLIT`

## 修复方案

### 1. 添加新的辅助方法

在 `src/expressions/baseExpression.ts` 中添加了以下新方法：

- **`_extractAttributesFromSubtotalsQuery`**: 从 subtotalsSpec 查询中提取正确的 attributes 和 keys 信息
- **`_buildHierarchicalDataset`**: 将扁平化结果转换为层级树状结构
- **`_buildSimpleSplit`**: 递归构建嵌套的 SPLIT 数据集
- **`_buildTopLevelAttributes`**: 构建顶层 attributes

### 2. 修复 `_buildSplitAttributes` 方法

**修复前**：
```typescript
private _buildSplitAttributes(allAttributes: any[], splitKey: string): any[] {
  // ... 其他逻辑
  
  // 问题：总是添加 SPLIT 属性
  splitAttributes.push({
    name: "SPLIT",
    type: "DATASET",
  });
  
  return splitAttributes;
}
```

**修复后**：
```typescript
private _buildSplitAttributes(
  allAttributes: any[], 
  splitKey: string, 
  keys: string[], 
  currentLevel: number
): any[] {
  // ... 其他逻辑
  
  // 修复：只有在不是最深层级时才添加 SPLIT 属性
  if (currentLevel + 1 < keys.length) {
    splitAttributes.push({
      name: "SPLIT",
      type: "DATASET",
    });
  }
  
  return splitAttributes;
}
```

### 3. 更新 `_executeSubtotalsQuery` 方法

修改方法使用新的辅助方法：

```typescript
// 3. 从查询中提取 attributes 和 keys 信息
const extractedInfo = this._extractAttributesFromSubtotalsQuery(query);

// 8. 将流转换为 PlywoodValue，然后转换为层级结构
return External.buildValueFromStream(resultStream).then((result: any) => {
  // 将扁平化结果转换为层级结构
  const hierarchicalResult = this._buildHierarchicalDataset(
    result,
    query,
    extractedInfo
  );
  
  // 使用 Dataset.fromJS 创建正确的 Dataset 对象
  return Dataset.fromJS(hierarchicalResult);
});
```

## 修复效果

### 修复前的问题结构
```javascript
{
  attributes: [], // 空数组
  keys: [],       // 空数组
  data: [
    // 扁平化数据，所有层级在同一数组中
    { __time: null, app: null, platform: null, activation: 678570 },
    { __time: "2025-08-09", app: null, platform: null, activation: 208526 },
    // ...
  ]
}
```

### 修复后的正确结构
```javascript
{
  attributes: [
    { name: "activation", type: "NUMBER" },
    { name: "current_pu", type: "NUMBER" },
    { name: "SPLIT", type: "DATASET" }
  ],
  keys: [],
  data: [
    {
      activation: 678570,
      current_pu: 9510,
      SPLIT: {
        keys: ["__time"],
        attributes: [
          { name: "__time", type: "TIME_RANGE" },
          { name: "activation", type: "NUMBER" },
          { name: "current_pu", type: "NUMBER" },
          { name: "SPLIT", type: "DATASET" }
        ],
        data: [
          {
            __time: "2025-08-09T00:00:00Z",
            activation: 208526,
            current_pu: 1392,
            SPLIT: {
              keys: ["app"],
              attributes: [
                { name: "app", type: "STRING" },
                { name: "activation", type: "NUMBER" },
                { name: "current_pu", type: "NUMBER" },
                { name: "SPLIT", type: "DATASET" }
              ],
              data: [
                {
                  app: "EM",
                  activation: 95143,
                  current_pu: 205,
                  SPLIT: {
                    keys: ["platform"],
                    attributes: [
                      { name: "platform", type: "STRING" },
                      { name: "activation", type: "NUMBER" },
                      { name: "current_pu", type: "NUMBER" }
                      // 关键：最深层级没有 SPLIT 属性
                    ],
                    data: [
                      {
                        platform: "Android",
                        activation: 45000,
                        current_pu: 100
                      }
                      // ...
                    ]
                  }
                }
                // ...
              ]
            }
          }
          // ...
        ]
      }
    }
  ]
}
```

## 关键修复点

1. **正确的 attributes 提取**：从查询的 dimensions 和 aggregations 中提取类型信息
2. **层级结构构建**：按照维度顺序逐级构建嵌套的 SPLIT 数据集
3. **最深层级处理**：确保最深层级不包含 SPLIT 属性
4. **数据完整性**：保证每个层级都有正确的聚合数据

## 测试验证

创建了多个测试文件验证修复效果：
- `test_hierarchical_debug.js`: 调试数据分组逻辑
- `test_split_attributes_issue.js`: 验证 _buildSplitAttributes 修复
- `test_fixed_hierarchical_structure.js`: 验证完整层级结构
- `test_final_verification.js`: 最终验证修复效果

所有测试都通过，确认修复成功！

## 修复成果

✅ **解决的问题**：
- 扁平化数据结构 → 层级树状结构
- 空的 attributes 和 keys → 正确的元数据信息
- 最深层级包含多余 SPLIT 属性 → 清晰的层级边界

✅ **达成的目标**：
- 与普通查询结果结构完全一致
- 提高 subtotalsSpec 优化功能的可靠性
- 避免前端解析时的混淆和错误
- 保持代码的向后兼容性

🎯 **修复完成**！现在 `_executeSubtotalsQuery` 方法能够正确生成与 dataset.js 相同的层级树状结构。

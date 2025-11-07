# Plywood subtotalsSpec 优化功能

## 概述

这个功能利用 Apache Druid 的 `subtotalsSpec` 特性，将多次查询合并为一次查询，大幅提升多维度分析的查询性能。

## 快速开始

### 1. 启用优化

在查询时添加以下选项：

```javascript
const result = await expression.compute(context, {
  customOptions: {
    useSubtotalsSpec: true    // 启用 subtotalsSpec 优化
  }
});
```

**注意**: 新的实现只需要 `useSubtotalsSpec: true` 即可启用优化，不再需要其他配置。

### 2. 适用场景

优化适用于以下查询场景：
- 包含多个不同的 split 操作
- 需要获取总计和各维度的分组数据
- 查询会生成多个 timeseries、topN 或 groupBy 查询

### 3. 性能提升

| 维度数量 | 原有查询次数 | 优化后查询次数 | 性能提升 |
|---------|-------------|---------------|---------|
| 3个维度 | 4次         | 1次           | 75%     |
| 5个维度 | 6次         | 1次           | 83%     |
| 10个维度| 11次        | 1次           | 91%     |

## 工作原理

### 原有方式
```
查询1: timeseries 获取总计
查询2: topN 按维度1分组
查询3: topN 按维度2分组
查询4: groupBy 按维度3分组
...
总计: N+1 次查询
```

### 优化后方式
```
1. 使用 simulateQueryPlan 获取所有查询的 JSON
2. 以 timeseries 查询为模板
3. 合并所有维度查询的参数
4. 生成包含 subtotalsSpec 的单个 groupBy 查询
总计: 1 次查询
```

### subtotalsSpec 示例

对于 5 个维度的查询，生成的 subtotalsSpec：

```json
[
  ["platform", "network_name", "multi_region", "campaign_type", "creative_type"],
  ["platform", "network_name", "multi_region", "campaign_type"],
  ["platform", "network_name", "multi_region"],
  ["platform", "network_name"],
  ["platform"],
  []
]
```

## 测试验证

### 运行完整测试
```bash
node tests/subtotals/test_complete_subtotals.js
```

### 运行基础功能测试
```bash
node tests/subtotals/test_subtotals_optimization.js
```

### 运行新实现测试
```bash
node tests/subtotals/test_subtotals_new.js
```

## 兼容性

- ✅ **完全向后兼容**: 不启用时使用原有逻辑
- ✅ **自动回退**: 检测失败时自动使用原有方法
- ✅ **Druid 版本**: 需要 Druid 0.17.0+ 支持 subtotalsSpec
- ✅ **查询类型**: 仅适用于 groupBy 查询

## 注意事项

1. **内存使用**: 单次查询可能返回更多数据
2. **Druid 版本**: 确保 Druid 版本支持 subtotalsSpec
3. **查询复杂度**: 维度过多时注意查询复杂度
4. **测试验证**: 在生产环境使用前充分测试
5. **排序一致**: 所有子请求的 `limitSpec` 都是一样的（用于排序），合并时保持原始的排序规则

## 故障排除

### 优化未生效

检查以下条件：
- [ ] `useSubtotalsSpec: true` 已设置
- [ ] 查询表达式包含多个不同的 split 操作
- [ ] 查询会生成多个不同类型的查询（timeseries + topN/groupBy）

### 查询错误

如果遇到查询错误：
1. 检查 Druid 版本是否支持 subtotalsSpec
2. 验证维度名称是否正确
3. 查看控制台日志中的详细错误信息

## 更多信息

详细的实现文档请参考：[SUBTOTALS_OPTIMIZATION.md](./SUBTOTALS_OPTIMIZATION.md)

完整的测试文件请参考：[tests/README.md](../../tests/README.md)

## 支持

如有问题或建议，请联系开发团队。

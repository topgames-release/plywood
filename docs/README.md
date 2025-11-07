# Plywood 文档

这里包含了 Plywood 项目的所有文档，按功能和类型进行组织。

## 📁 文档结构

```
docs/
├── README.md                    # 文档索引（本文件）
├── features/                    # 功能特性文档
│   ├── SUBTOTALS_OPTIMIZATION.md   # subtotalsSpec 优化详细文档
│   └── README_SUBTOTALS.md         # subtotalsSpec 优化使用指南
├── api/                         # API 文档
│   └── (待添加)
└── examples/                    # 示例代码
    └── (待添加)
```

## 🚀 功能特性

### subtotalsSpec 优化
- **详细文档**: [features/SUBTOTALS_OPTIMIZATION.md](./features/SUBTOTALS_OPTIMIZATION.md)
- **使用指南**: [features/README_SUBTOTALS.md](./features/README_SUBTOTALS.md)
- **功能描述**: 利用 Apache Druid 的 subtotalsSpec 特性，将多次查询合并为一次查询，大幅提升多维度分析的查询性能

#### 快速开始
```javascript
const result = await expression.compute(context, {
  customOptions: {
    useSubtotalsSpec: true    // 启用 subtotalsSpec 优化
  }
});
```

#### 性能提升
- **查询数量**: 从 N+1 次减少到 1 次
- **性能提升**: 60-90% 的查询时间减少
- **适用场景**: 多维度分析查询

## 📚 其他文档

### 项目根目录文档
- `README.md` - 项目主要说明
- `CHANGELOG.md` - 版本更新日志
- `package.json` - 项目配置

### 测试文件
项目根目录下的测试文件：
- `test_subtotals_optimization.js` - 基础功能测试
- `test_subtotals_real.js` - 真实场景测试
- `test_subtotals_new.js` - 新实现测试
- `test_complete_subtotals.js` - 完整功能测试
- `test_dimension_dedup.js` - 维度去重测试
- `test_virtualcolumns_merge.js` - virtualColumns 合并测试
- `test_subtotals_outputname.js` - outputName 修复测试
- `example_subtotals_usage.js` - 使用示例

## 🔍 文档搜索

### 按功能查找
- **查询优化**: `features/SUBTOTALS_OPTIMIZATION.md`
- **使用指南**: `features/README_SUBTOTALS.md`

### 按类型查找
- **详细技术文档**: `features/` 目录
- **API 参考**: `api/` 目录（待添加）
- **示例代码**: `examples/` 目录（待添加）

## 📝 文档维护

### 添加新功能文档
1. 在 `features/` 目录下创建功能文档
2. 更新本文档的索引
3. 添加相应的测试文件

### 文档命名规范
- **功能文档**: `FEATURE_NAME.md`
- **使用指南**: `README_FEATURE_NAME.md`
- **API 文档**: `API_NAME.md`

### 文档更新流程
1. 功能开发完成后立即更新文档
2. 确保文档与代码同步
3. 定期检查文档的准确性

## 🎯 贡献指南

### 文档贡献
- 遵循 Markdown 格式规范
- 包含清晰的示例代码
- 提供完整的使用说明
- 添加适当的测试验证

### 文档审查
- 技术准确性
- 语言清晰度
- 示例完整性
- 结构合理性

---

**注意**: 这个文档结构有助于：
1. 🔍 **便于检索**: 按功能和类型分类
2. 📚 **便于管理**: 清晰的目录结构
3. 🚀 **便于维护**: 统一的命名规范
4. 💡 **便于扩展**: 预留了 API 和示例目录

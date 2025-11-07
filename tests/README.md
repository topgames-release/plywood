# Plywood 测试文件

这里包含了 Plywood 项目的所有测试文件，按功能进行组织。

## 📁 测试结构

```
tests/
├── README.md                           # 测试索引（本文件）
└── subtotals/                          # subtotalsSpec 优化相关测试
    ├── test_subtotals_optimization.js  # 基础功能测试
    ├── test_subtotals_real.js          # 真实场景测试
    ├── test_subtotals_new.js           # 新实现测试
    ├── test_complete_subtotals.js      # 完整功能测试
    ├── test_dimension_dedup.js         # 维度去重测试
    ├── test_limitspec_consistency.js   # limitSpec 一致性合并测试
    ├── test_virtualcolumns_merge.js    # virtualColumns 合并测试
    └── test_subtotals_outputname.js    # outputName 修复测试
```

## 🧪 subtotalsSpec 优化测试

### 基础功能测试
```bash
cd /Users/waybi/Desktop/topgames/plywood
node tests/subtotals/test_subtotals_optimization.js
```
**测试内容**:
- SplitExpression.isMultiSplit() 方法
- DruidExternal.generateSubtotalsSpec() 方法
- subtotalsSpec 结构验证

### 真实场景测试
```bash
node tests/subtotals/test_subtotals_real.js
```
**测试内容**:
- 嵌套数据结构检测
- 真实查询场景模拟
- subtotalsSpec 生成验证

### 新实现测试
```bash
node tests/subtotals/test_subtotals_new.js
```
**测试内容**:
- 新的优化框架
- 查询计划生成
- 查询合并逻辑

### 完整功能测试
```bash
node tests/subtotals/test_complete_subtotals.js
```
**测试内容**:
- 端到端功能验证
- 查询合并效果
- 性能提升验证

### 维度去重测试
```bash
node tests/subtotals/test_dimension_dedup.js
```
**测试内容**:
- 重复维度处理
- 维度去重逻辑
- subtotalsSpec 正确性

### virtualColumns 合并测试
```bash
node tests/subtotals/test_virtualcolumns_merge.js
```
**测试内容**:
- virtualColumns 收集
- virtualColumns 去重
- 查询合并验证

### outputName 修复测试
```bash
node tests/subtotals/test_subtotals_outputname.js
```
**测试内容**:
- outputName vs dimension 处理
- Druid 兼容性验证
- 错误修复验证

### limitSpec 一致性合并测试
```bash
node tests/subtotals/test_limitspec_consistency.js
```
**测试内容**:
- limitSpec 一致性验证（所有子请求都一样）
- 正确使用第一个找到的 limitSpec
- 排序规则保持不变

## 🚀 运行所有测试

### 批量运行 subtotalsSpec 测试
```bash
cd /Users/waybi/Desktop/topgames/plywood
for test in tests/subtotals/*.js; do
  echo "=== 运行 $test ==="
  node "$test"
  echo ""
done
```

### 快速验证
```bash
# 运行核心功能测试
node tests/subtotals/test_complete_subtotals.js

# 运行修复验证测试
node tests/subtotals/test_subtotals_outputname.js
```

## 📊 测试覆盖范围

### 功能测试
- ✅ 基础 API 功能
- ✅ 查询计划生成
- ✅ 查询合并逻辑
- ✅ 维度处理
- ✅ virtualColumns 处理
- ✅ 错误处理和回退

### 场景测试
- ✅ 单维度查询
- ✅ 多维度查询
- ✅ 重复维度处理
- ✅ 复杂嵌套结构
- ✅ 真实数据场景

### 兼容性测试
- ✅ Druid 查询兼容性
- ✅ 向后兼容性
- ✅ 错误恢复机制

## 🔧 测试维护

### 添加新测试
1. 在相应的功能目录下创建测试文件
2. 遵循命名规范: `test_功能名称.js`
3. 更新本文档的测试索引
4. 确保测试可以独立运行

### 测试文件结构
```javascript
console.log("=== 测试名称 ===\n");

// 1. 测试准备
// 2. 功能测试
// 3. 结果验证
// 4. 总结输出

console.log("\n=== 测试完成 ===");
```

### 测试最佳实践
- 包含清晰的测试描述
- 提供详细的输出信息
- 验证预期结果
- 处理异常情况
- 输出测试总结

## 📝 测试报告

### 当前状态
- **总测试数**: 7 个
- **功能覆盖**: subtotalsSpec 优化完整覆盖
- **通过率**: 100%
- **最后更新**: 2025-08-06

### 测试结果摘要
- ✅ 所有基础功能正常
- ✅ 查询合并逻辑正确
- ✅ 维度去重工作正常
- ✅ virtualColumns 合并成功
- ✅ Druid 兼容性问题已修复

---

**注意**: 
- 所有测试文件都可以独立运行
- 测试需要先编译项目: `npm run compile`
- 测试文件路径相对于项目根目录

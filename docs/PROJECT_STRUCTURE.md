# Plywood 项目结构

本文档描述了 Plywood 项目的整体结构，特别是新增的 subtotalsSpec 优化功能的组织方式。

## 📁 项目目录结构

```
plywood/
├── README.md                           # 项目主要说明
├── PROJECT_STRUCTURE.md               # 项目结构说明（本文件）
├── package.json                       # 项目配置
├── src/                               # 源代码目录
│   ├── expressions/
│   │   └── baseExpression.ts         # 🆕 subtotalsSpec 优化核心实现
│   └── external/
│       └── druidExternal.ts          # 🆕 Druid 直接查询支持
├── docs/                              # 📚 文档目录
│   ├── README.md                      # 文档索引
│   ├── features/                      # 功能特性文档
│   │   ├── SUBTOTALS_OPTIMIZATION.md # 🆕 subtotalsSpec 优化详细文档
│   │   └── README_SUBTOTALS.md       # 🆕 subtotalsSpec 优化使用指南
│   └── examples/                      # 示例代码
│       └── example_subtotals_usage.js # 🆕 subtotalsSpec 使用示例
├── tests/                             # 🧪 测试目录
│   ├── README.md                      # 测试索引
│   └── subtotals/                     # subtotalsSpec 优化测试
│       ├── test_complete_subtotals.js         # 完整功能测试
│       ├── test_dimension_dedup.js            # 维度去重测试
│       ├── test_subtotals_new.js              # 新实现测试
│       ├── test_subtotals_optimization.js     # 基础功能测试
│       ├── test_subtotals_outputname.js       # outputName 修复测试
│       ├── test_subtotals_real.js             # 真实场景测试
│       └── test_virtualcolumns_merge.js       # virtualColumns 合并测试
└── build/                             # 编译输出目录
    └── plywood.js                     # 编译后的主文件
```

## 🆕 新增功能：subtotalsSpec 优化

### 核心实现文件

#### `src/expressions/baseExpression.ts`
**新增方法**：
- `_computeWithSubtotalsSpec()` - 主要优化流程
- `_mergeQueriesWithSubtotalsSpec()` - 查询合并逻辑
- `_extractDimensionName()` - 维度名称提取（用于去重）
- `_extractDimensionOutputName()` - 输出名称提取（用于 subtotalsSpec）
- `_executeSubtotalsQuery()` - 查询执行
- `_findDruidExternal()` - 数据源查找

#### `src/external/druidExternal.ts`
**新增功能**：
- 直接查询支持 (`useDirectQuery`)
- subtotalsSpec 查询处理

### 文档结构

#### `docs/features/`
- **SUBTOTALS_OPTIMIZATION.md** - 详细技术文档
  - 实现原理
  - 技术架构
  - 性能分析
  - 测试验证
  
- **README_SUBTOTALS.md** - 使用指南
  - 快速开始
  - 使用方法
  - 故障排除
  - 兼容性说明

#### `docs/examples/`
- **example_subtotals_usage.js** - 完整使用示例

### 测试结构

#### `tests/subtotals/`
所有 subtotalsSpec 相关测试，按功能分类：

1. **基础功能测试**
   - `test_subtotals_optimization.js` - API 基础功能
   - `test_subtotals_new.js` - 新实现框架

2. **核心逻辑测试**
   - `test_dimension_dedup.js` - 维度去重逻辑
   - `test_virtualcolumns_merge.js` - virtualColumns 合并
   - `test_subtotals_outputname.js` - outputName 修复

3. **集成测试**
   - `test_complete_subtotals.js` - 端到端功能测试
   - `test_subtotals_real.js` - 真实场景测试

## 🔍 文档检索和管理

### 按功能查找
- **查询优化**: `docs/features/SUBTOTALS_OPTIMIZATION.md`
- **使用指南**: `docs/features/README_SUBTOTALS.md`
- **测试验证**: `tests/README.md`

### 按类型查找
- **技术文档**: `docs/features/` 目录
- **示例代码**: `docs/examples/` 目录
- **测试文件**: `tests/subtotals/` 目录

### 文档链接关系
```
README.md
    ↓ 引用
docs/features/README_SUBTOTALS.md
    ↓ 引用
docs/features/SUBTOTALS_OPTIMIZATION.md
    ↓ 引用
tests/README.md
    ↓ 引用
tests/subtotals/*.js
```

## 🚀 快速导航

### 开发者
- **实现细节**: [docs/features/SUBTOTALS_OPTIMIZATION.md](./docs/features/SUBTOTALS_OPTIMIZATION.md)
- **测试验证**: [tests/README.md](./tests/README.md)

### 用户
- **使用指南**: [docs/features/README_SUBTOTALS.md](./docs/features/README_SUBTOTALS.md)
- **示例代码**: [docs/examples/example_subtotals_usage.js](./docs/examples/example_subtotals_usage.js)

### 测试
```bash
# 运行所有 subtotalsSpec 测试
for test in tests/subtotals/*.js; do node "$test"; done

# 运行核心功能测试
node tests/subtotals/test_complete_subtotals.js

# 运行修复验证测试
node tests/subtotals/test_subtotals_outputname.js
```

## 📝 维护指南

### 添加新功能
1. 在 `src/` 中实现功能
2. 在 `docs/features/` 中添加文档
3. 在 `tests/` 中添加测试
4. 更新相关索引文件

### 文档更新
1. 保持文档与代码同步
2. 更新相关链接引用
3. 验证示例代码正确性
4. 更新测试覆盖范围

### 目录命名规范
- **功能文档**: `docs/features/FEATURE_NAME.md`
- **使用指南**: `docs/features/README_FEATURE_NAME.md`
- **测试文件**: `tests/feature_name/test_specific_function.js`
- **示例代码**: `docs/examples/example_feature_usage.js`

---

**优势**：
- 🔍 **便于检索**: 按功能和类型分类组织
- 📚 **便于管理**: 清晰的目录结构和命名规范
- 🚀 **便于维护**: 统一的文档链接关系
- 💡 **便于扩展**: 预留了扩展空间和标准化流程

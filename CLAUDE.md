# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 开发命令

### 构建和编译
```bash
npm run compile          # 编译 TypeScript 和 PEG.js
./compile               # 直接执行编译脚本
./compile-tsc           # 仅编译 TypeScript
./compile-pegjs         # 仅编译 PEG.js 语法解析器
```

### 测试
```bash
npm test                # 运行标准测试套件
npm run pretest         # 编译代码（测试前预检查）
npm run full-test       # 运行完整测试套件
./travis-test           # 运行 Travis CI 测试（多版本测试）
./run-tests             # 快速测试运行器
```

### 单独测试运行
```bash
# 测试特定类型
mocha test/expression/*            # 表达式测试
mocha test/overall/*              # 整体功能测试
mocha test/external/*             # 外部数据源测试
mocha test/subtotals/*            # subtotals 优化测试

# 单个测试文件
mocha test/overall/compute.mocha.js
```

## 架构概览

Plywood 是一个基于 Split-Apply-Combine 模式的查询规划和执行库，专门为大规模数据集的交互式可视化设计。

### 核心模块结构

1. **表达式系统** (`src/expressions/`)
   - `baseExpression.ts`: 表达式基类，包含计算逻辑和 subtotalsSpec 优化
   - `splitExpression.ts`: 分组表达式，支持多维度分组
   - 各种操作表达式：聚合、过滤、数学运算等

2. **数据类型** (`src/datatypes/`)
   - `dataset.ts`: 数据集核心类型
   - `timeRange.ts`, `numberRange.ts`, `stringRange.ts`: 范围类型
   - `set.ts`: 集合类型

3. **外部数据源** (`src/external/`)
   - `druidExternal.ts`: Druid 数据源适配器（支持 subtotalsSpec）
   - `mySqlExternal.ts`, `postgresExternal.ts`: SQL 数据源
   - `druidSqlExternal.ts`: Druid SQL 查询支持

4. **查询方言** (`src/dialect/`)
   - `druidDialect.ts`: Druid 查询方言
   - `mySqlDialect.ts`, `postgresDialect.ts`: SQL 方言

5. **执行器** (`src/executor/`)
   - `basicExecutor.ts`: 基础查询执行器

### 关键特性：subtotalsSpec 查询优化

项目实现了针对 Apache Druid 的 subtotalsSpec 优化，可将多维度分析查询从 N+1 次减少到 1 次：

- **优化原理**: 利用 Druid 的 `subtotalsSpec` 特性在单次查询中获取多个聚合层级
- **性能提升**: 减少 60-90% 的查询时间和网络开销
- **使用方法**: 在 `compute()` 调用中启用 `customOptions.useSubtotalsSpec: true`
- **实现位置**: [`src/expressions/baseExpression.ts`](src/expressions/baseExpression.ts) 和 [`src/external/druidExternal.ts`](src/external/druidExternal.ts)

详细文档请参考：[docs/features/SUBTOTALS_OPTIMIZATION.md](docs/features/SUBTOTALS_OPTIMIZATION.md)

## 表达式语言

Plywood 拥有自己的表达式语言，支持：
- 数据聚合（sum, count, average 等）
- 数据过滤（filter, match 等）
- 数据转换（timeBucket, numberBucket 等）
- 复杂的 Split-Apply-Combine 操作

表达式可以编译到多种数据库查询，并返回嵌套数据结构以便可视化库消费。

## 测试策略

- **单元测试**: 每个模块都有对应的测试文件
- **功能测试**: [`test/functional/`](test/functional/) 包含端到端测试
- **模拟测试**: [`test/simulate/`](test/simulate/) 测试查询生成逻辑
- **特定测试**: [`test/subtotals/`](test/subtotals/) 专门测试 subtotalsSpec 优化

## TypeScript 配置

项目使用 TypeScript 3.5.3，配置文件通过 `tdi` 工具管理。编译后的文件输出到 `build/` 目录。

## 依赖管理

主要依赖包括：
- `@topgames/chronoshift`: 时间处理库
- `immutable-class`: 不可变数据结构
- `moment-timezone`: 时区处理

开发依赖包括测试框架（mocha, chai）和构建工具（typescript, tdi）。
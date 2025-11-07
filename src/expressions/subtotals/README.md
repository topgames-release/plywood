# subtotalsSpec 模块（骨架）

本目录包含 subtotalsSpec 优化重构的目标模块：

- subtotalsSpecTypes.ts：内部轻量类型/别名
- subtotalsSpecQueryBuilder.ts：查询合并与 subtotalsSpec 生成
- subtotalsSpecExecutor.ts：查询执行、inflaters 构建、并行与合并
- subtotalsSpecResultProcessor.ts：结果属性推导、层级构建、排序

第一阶段仅创建骨架，不接入调用路径以确保最小影响；后续阶段将逐步迁移 baseExpression.ts 内的实现体，并在每步迁移后运行编译与测试验证。


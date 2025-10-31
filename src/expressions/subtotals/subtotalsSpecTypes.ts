/*
 * Types and lightweight contracts for subtotalsSpec refactoring modules.
 * 保持 TS 3.5.3 兼容，不使用可选链/空值合并等新语法。
 */

// 基础别名，后续逐步收紧具体类型定义（第一阶段以 any 为占位避免编译阻塞）
export type QueryPlan = any[][]; // simulateQueryPlan() 的产物（分步查询列表）
export type GroupByQuery = any;   // Druid groupBy 查询 JSON
export type TimeseriesQuery = any; // Druid timeseries 查询 JSON
export type DatasetLike = any;    // 结果数据集占位（与 Plywood Dataset 兼容）

export type SplitExpressionsMap = Map<string, any>;

// 执行上下文与选项（与现有 ComputeOptions/Datum 保持松耦合）
export interface ExecContext {
  context: any; // Datum
  options: any; // ComputeOptions
}

// 稳定 key 生成/比较等占位类型
export type StableKey = string | number;
export type Comparator<T> = (a: T, b: T) => number;


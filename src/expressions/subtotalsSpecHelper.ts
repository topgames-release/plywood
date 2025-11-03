/*
 * Helper for subtotalsSpec optimization orchestration.
 *
 * 职责说明：
 * - 将 baseExpression.ts 中 subtotalsSpec 优化的“流程编排”集中到单独文件
 * - 不改变外部 API：baseExpression._computeWithSubtotalsSpec 仍存在，但内部委托到本帮助类
 * - 当前阶段尽量小改动以保证测试稳定：具体实现细节（查询合并、层级构建、inflater 生成、TimeRange 转换等）继续复用
 *   baseExpression 内既有的私有方法，通过“宿主表达式实例”的内部方法调用完成
 * - 后续若需要进一步彻底迁移实现，可逐步把私有方法的实现体搬迁到本类中
 */

import { Datum, PlywoodValue } from "../datatypes/index";
import {
  buildMergedQuery,
  extractTimeseriesQuery,
} from "./subtotals/subtotalsSpecQueryBuilder";
import { executeQueriesInParallel } from "./subtotals/subtotalsSpecExecutor";

/**
 * SubtotalsSpecHelper
 *
 * 仅负责 orchestrate（编排）：
 * 1) 提取 splitExpressions
 * 2) 生成 queryPlan
 * 3) 选择 timeseries 模板查询
 * 4) 合并为 subtotalsSpec groupBy 查询
 * 5) 并行执行并构建最终 Dataset
 *
 * 说明：
 * - 具体的实现细节依赖宿主 BaseExpression 实例上的私有方法（以 any 形式调用，保持行为一致）
 * - 这样可以在不大规模改动的前提下，先实现代码边界划分和集中入口
 */
export class SubtotalsSpecHelper {
  /**
   * 入口：使用 subtotalsSpec 优化进行计算
   *
   * @param host 表达式的宿主实例（BaseExpression 的实例），用于调用内部私有方法
   * @param context 计算上下文
   * @param options 计算选项
   * @param expressionForQueryPlan 用于生成查询计划的独立表达式副本
   */
  public static computeWithSubtotalsSpec(
    host: any,
    context: Datum,
    options: any,
    expressionForQueryPlan: any
  ): Promise<PlywoodValue> {
    // 1) 提取 split expressions（使用宿主实例上的私有方法）
    const splitExpressionsMap: Map<string, any> = (
      host as any
    )._extractSplitExpressionsFromExternals(
      host,
      (options && (options as any).concurrentQueryLimit) || Infinity
    );

    // 2) 使用独立表达式副本生成查询计划
    const queryPlan: any[][] = (
      expressionForQueryPlan as any
    ).simulateQueryPlan(context, options);
    if (!Array.isArray(queryPlan) || queryPlan.length === 0) {
      throw new Error("没有生成查询计划");
    }

    // 3) 提取 timeseries 总计查询
    const timeseriesQuery: any | null = extractTimeseriesQuery(host, queryPlan);

    // 4) 合并查询为一个带有 subtotalsSpec 的 groupBy 查询
    // 从 options 中提取 maxQueries 参数（默认 500）
    const maxQueries =
      options && typeof options.maxQueries === "number"
        ? options.maxQueries
        : 500;
    const mergedQuery: any = buildMergedQuery(host, queryPlan, maxQueries);

    // 5) 并行执行两个查询并合并结果 -> 返回 Dataset
    return executeQueriesInParallel(
      host,
      timeseriesQuery,
      mergedQuery,
      context,
      options,
      splitExpressionsMap
    ) as Promise<PlywoodValue>;
  }
}

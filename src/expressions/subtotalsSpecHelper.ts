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
import { Expression } from "./baseExpression";
import { ApplyExpression } from "./applyExpression";
import { AndExpression } from "./andExpression";
import { FilterExpression } from "./filterExpression";
import { LiteralExpression } from "./literalExpression";
import { NotExpression } from "./notExpression";
import { OrExpression } from "./orExpression";
import { RefExpression } from "./refExpression";
import { LessThanExpression } from "./lessThanExpression";
import { LessThanOrEqualExpression } from "./lessThanOrEqualExpression";
import { GreaterThanExpression } from "./greaterThanExpression";
import { GreaterThanOrEqualExpression } from "./greaterThanOrEqualExpression";
import {
  buildMergedQuery,
  extractTimeseriesQuery,
} from "./subtotals/subtotalsSpecQueryBuilder";
import { executeQueriesInParallel } from "./subtotals/subtotalsSpecExecutor";
import {
  DruidHavingFilter,
  DruidHavingComparison,
} from "./subtotals/subtotalsSpecTypes";

type ComparisonExpression =
  | LessThanExpression
  | LessThanOrEqualExpression
  | GreaterThanExpression
  | GreaterThanOrEqualExpression;

type ComparisonOperator =
  | "lessThan"
  | "lessThanOrEqual"
  | "greaterThan"
  | "greaterThanOrEqual";

interface NormalizedComparison {
  aggregation: string;
  operator: ComparisonOperator;
  value: number;
}

interface OperandAggregation {
  kind: "aggregation";
  name: string;
}

interface OperandLiteral {
  kind: "literal";
  value: number;
}

type OperandInfo =
  | OperandAggregation
  | OperandLiteral
  | { kind: "other" };

type AggregationNameLookup = Map<string, true>;

const COMPARISON_OPERATORS: ReadonlyArray<ComparisonOperator> = [
  "lessThan",
  "lessThanOrEqual",
  "greaterThan",
  "greaterThanOrEqual",
];

const REVERSED_OPERATOR: Record<ComparisonOperator, ComparisonOperator> = {
  lessThan: "greaterThan",
  lessThanOrEqual: "greaterThanOrEqual",
  greaterThan: "lessThan",
  greaterThanOrEqual: "lessThanOrEqual",
};

function collectAggregateNames(expression: Expression): AggregationNameLookup {
  const names: AggregationNameLookup = new Map();
  expression.forEach((ex) => {
    if (ex instanceof ApplyExpression && ex.expression.isAggregate()) {
      names.set(ex.name, true);
    }
  });
  return names;
}

function isComparisonExpression(ex: Expression): ex is ComparisonExpression {
  return COMPARISON_OPERATORS.indexOf(ex.op as ComparisonOperator) !== -1;
}

function operandInfoFromExpression(
  ex: Expression,
  aggregateNames: AggregationNameLookup
): OperandInfo {
  if (ex instanceof RefExpression && aggregateNames.has(ex.name) && ex.nest === 0) {
    return { kind: "aggregation", name: ex.name };
  }

  if (ex instanceof LiteralExpression) {
    const literalValue = ex.value;
    if (typeof literalValue === "number" && !isNaN(literalValue)) {
      return { kind: "literal", value: literalValue };
    }
  }

  return { kind: "other" };
}

function normalizeComparison(
  comparison: ComparisonExpression,
  aggregateNames: AggregationNameLookup
): NormalizedComparison | null {
  const operandInfo = operandInfoFromExpression(
    comparison.operand,
    aggregateNames
  );
  const expressionInfo = operandInfoFromExpression(
    comparison.expression,
    aggregateNames
  );

  if (operandInfo.kind === "aggregation" && expressionInfo.kind === "literal") {
    return {
      aggregation: operandInfo.name,
      operator: comparison.op as ComparisonOperator,
      value: expressionInfo.value,
    };
  }

  if (operandInfo.kind === "literal" && expressionInfo.kind === "aggregation") {
    const reversed = REVERSED_OPERATOR[comparison.op as ComparisonOperator];
    return {
      aggregation: expressionInfo.name,
      operator: reversed,
      value: operandInfo.value,
    };
  }

  return null;
}

function adjustForInclusiveUpperBound(value: number): number {
  return Number.isInteger(value) ? value + 1 : value + Number.EPSILON;
}

function adjustForInclusiveLowerBound(value: number): number {
  return Number.isInteger(value) ? value - 1 : value - Number.EPSILON;
}

function comparisonToHaving(
  normalized: NormalizedComparison
): DruidHavingFilter | null {
  const { aggregation, operator, value } = normalized;

  switch (operator) {
    case "lessThan":
      return { type: "lessThan", aggregation, value } as DruidHavingComparison;
    case "greaterThan":
      return { type: "greaterThan", aggregation, value } as DruidHavingComparison;
    case "lessThanOrEqual":
      return {
        type: "lessThan",
        aggregation,
        value: adjustForInclusiveUpperBound(value),
      } as DruidHavingComparison;
    case "greaterThanOrEqual":
      return {
        type: "greaterThan",
        aggregation,
        value: adjustForInclusiveLowerBound(value),
      } as DruidHavingComparison;
    default:
      return null;
  }
}

function reduceLogicalHaving(
  type: "and" | "or",
  children: DruidHavingFilter[]
): DruidHavingFilter | null {
  if (children.length === 0) return null;
  if (children.length === 1) return children[0];

  const flattened: DruidHavingFilter[] = [];
  children.forEach((child) => {
    if (child.type === type) {
      flattened.push.apply(flattened, child.havingSpecs);
    } else {
      flattened.push(child);
    }
  });

  return type === "and"
    ? { type: "and", havingSpecs: flattened }
    : { type: "or", havingSpecs: flattened };
}

function booleanExpressionToHaving(
  expression: Expression,
  aggregateNames: AggregationNameLookup
): DruidHavingFilter | null {
  if (expression instanceof AndExpression) {
    const parts = expression
      .getExpressionList()
      .map((part) => booleanExpressionToHaving(part, aggregateNames))
      .filter((part): part is DruidHavingFilter => part !== null);
    return reduceLogicalHaving("and", parts);
  }

  if (expression instanceof OrExpression) {
    const parts = expression
      .getExpressionList()
      .map((part) => booleanExpressionToHaving(part, aggregateNames))
      .filter((part): part is DruidHavingFilter => part !== null);
    return reduceLogicalHaving("or", parts);
  }

  if (expression instanceof NotExpression) {
    const inner = booleanExpressionToHaving(expression.operand, aggregateNames);
    return inner ? { type: "not", havingSpec: inner } : null;
  }

  if (isComparisonExpression(expression)) {
    const normalized = normalizeComparison(expression, aggregateNames);
    return normalized ? comparisonToHaving(normalized) : null;
  }

  return null;
}

function combineHavingFilters(
  filters: DruidHavingFilter[]
): DruidHavingFilter | null {
  if (filters.length === 0) return null;
  if (filters.length === 1) return filters[0];

  const flattened: DruidHavingFilter[] = [];
  filters.forEach((filter) => {
    if (filter.type === "and") {
      flattened.push.apply(flattened, filter.havingSpecs);
    } else {
      flattened.push(filter);
    }
  });

  if (flattened.length === 1) return flattened[0];
  return { type: "and", havingSpecs: flattened };
}

export function extractHavingFilters(
  expression: Expression
): DruidHavingFilter | null {
  const aggregateNames = collectAggregateNames(expression);
  if (aggregateNames.size === 0) return null;

  const collected: DruidHavingFilter[] = [];
  const serialized = new Map<string, true>();

  expression.forEach((ex) => {
    if (ex instanceof FilterExpression) {
      const having = booleanExpressionToHaving(ex.expression, aggregateNames);
      if (having) {
        const key = JSON.stringify(having);
        if (!serialized.has(key)) {
          serialized.set(key, true);
          collected.push(having);
        }
      }
    }
  });

  return combineHavingFilters(collected);
}

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
    host: Expression,
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

    // 4) 从表达式中提取 having 过滤条件
    const explicitHaving = extractHavingFilters(host);

    // 4) 合并查询为一个带有 subtotalsSpec 的 groupBy 查询
    // 从 options 中提取 maxQueries 参数（默认 500）
    const maxQueries =
      options && typeof options.maxQueries === "number"
        ? options.maxQueries
        : 500;
    const mergedQuery: any = buildMergedQuery(
      host,
      queryPlan,
      maxQueries,
      explicitHaving
    );

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

  /**
   * 公开的辅助方法：便于测试直接复用提取逻辑。
   */
  public static extractHavingFiltersFromExpression(
    expression: Expression
  ): DruidHavingFilter | null {
    return extractHavingFilters(expression);
  }
}

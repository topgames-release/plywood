/*
 * Query builder for subtotalsSpec optimization.
 * 第二阶段：迁移查询构建实现，去除对宿主私有方法的依赖。
 */

import {
  QueryPlan,
  GroupByQuery,
  TimeseriesQuery,
  DruidHavingFilter,
} from "./subtotalsSpecTypes";

// —— 工具函数 ——
function extractDimensionName(dim: any): string | null {
  if (typeof dim === "string") return dim;
  if (dim && dim.dimension) return dim.dimension;
  if (dim && dim.outputName) return dim.outputName;
  return null;
}

function extractDimensionOutputName(dim: any): string | null {
  if (typeof dim === "string") return dim;
  if (dim && dim.outputName) return dim.outputName;
  if (dim && dim.dimension) return dim.dimension;
  return null;
}

function extractSortFromTopNMetric(query: any): any | null {
  if (!query || query.queryType !== "topN" || !query.metric) return null;

  const metric = query.metric;
  let direction = "ascending";
  let dimensionOrder = "lexicographic";

  // 处理倒序
  if (metric.type === "inverted") {
    direction = "descending";
    if (metric.metric && metric.metric.type === "dimension") {
      dimensionOrder = metric.metric.ordering || "lexicographic";
    }
  } else if (metric.type === "dimension") {
    dimensionOrder = metric.ordering || "lexicographic";
  } else {
    return null; // 不是维度排序
  }

  const dimension = extractDimensionOutputName(query.dimension);
  if (!dimension) return null;
  return { dimension, direction, dimensionOrder };
}

function mergeSortColumns(dimensionQueries: any[]): any[] {
  const sortMap = new Map<string, any>(); // key: dimension 名称

  // 1) 维度排序优先（来自 topN.metric）
  for (const q of dimensionQueries) {
    if (q.queryType === "topN") {
      const spec = extractSortFromTopNMetric(q);
      if (spec && !sortMap.has(spec.dimension))
        sortMap.set(spec.dimension, spec);
    }
  }

  // 2) 指标排序（来自各自的 limitSpec.columns）
  for (const q of dimensionQueries) {
    const cols = q && q.limitSpec && q.limitSpec.columns;
    if (Array.isArray(cols)) {
      for (const col of cols) {
        if (!col || !col.dimension) continue;
        if (!sortMap.has(col.dimension)) sortMap.set(col.dimension, col);
      }
    }
  }

  return Array.from(sortMap.values());
}

/**
 * 从 queryPlan 中提取一个 timeseries 总计查询（优先具名聚合）。
 */
export function extractTimeseriesQuery(
  _host: any,
  queryPlan: QueryPlan
): TimeseriesQuery | null {
  let fallback: any = null;
  let preferred: any = null;
  for (const group of queryPlan as any[][]) {
    for (const query of group) {
      if (query.queryType !== "timeseries") continue;
      if (!fallback) fallback = { ...query };
      const aggs = Array.isArray(query.aggregations) ? query.aggregations : [];
      const hasNamedAggs =
        aggs.length > 0 &&
        aggs.some((a: any) => a && a.name && a.name !== "__VALUE__");
      if (hasNamedAggs) {
        preferred = { ...query };
        break;
      }
    }
    if (preferred) break;
  }
  return preferred || fallback;
}

/**
 * 从 queryPlan 合并出一个包含 subtotalsSpec 的 groupBy 查询。
 * @param _host 宿主表达式实例
 * @param queryPlan 查询计划
 * @param maxQueries 最大查询数量限制（用于 limitSpec.limit），默认 10000
 */
export function buildMergedQuery(
  _host: any,
  queryPlan: QueryPlan,
  maxQueries?: number,
  explicitHaving?: DruidHavingFilter | null
): GroupByQuery {
  // 找到第一个 timeseries 查询作为模板，并收集维度查询
  let templateQuery: any = null;
  const dimensionQueries: any[] = [];

  for (const group of queryPlan as any[][]) {
    for (const query of group) {
      if (query.queryType === "timeseries" && !templateQuery) {
        templateQuery = { ...query };
      } else if (query.queryType === "topN" || query.queryType === "groupBy") {
        dimensionQueries.push(query);
      }
    }
  }

  if (!templateQuery) throw new Error("未找到 timeseries 查询作为模板");
  if (dimensionQueries.length === 0) throw new Error("未找到维度查询");

  // 以 timeseries 为模板，生成 groupBy 查询
  const mergedQuery: any = {
    ...templateQuery,
    queryType: "groupBy",
    granularity: "all",
  };

  // 收集 virtualColumns（去重）
  const virtualColumnsMap = new Map<string, any>();
  if (Array.isArray(templateQuery.virtualColumns)) {
    for (const vc of templateQuery.virtualColumns) {
      if (vc && vc.name) virtualColumnsMap.set(vc.name, vc);
    }
  }

  // 维度与 virtualColumns 的合并
  const dimensionsMap = new Map<string, any>();
  for (const dq of dimensionQueries) {
    if (Array.isArray(dq.virtualColumns)) {
      for (const vc of dq.virtualColumns) {
        if (vc && vc.name && !virtualColumnsMap.has(vc.name))
          virtualColumnsMap.set(vc.name, vc);
      }
    }

    if (dq.dimension) {
      const dimName = extractDimensionName(dq.dimension);
      if (dimName && !dimensionsMap.has(dimName))
        dimensionsMap.set(dimName, dq.dimension);
    } else if (Array.isArray(dq.dimensions)) {
      for (const dim of dq.dimensions) {
        const dimName = extractDimensionName(dim);
        if (dimName && !dimensionsMap.has(dimName))
          dimensionsMap.set(dimName, dim);
      }
    }
  }

  // 生成 subtotalsSpec
  if (dimensionsMap.size > 0) {
    const dimensions = Array.from(dimensionsMap.values());
    mergedQuery.dimensions = dimensions;

    const dimensionOutputNames = dimensions
      .map((d: any) => extractDimensionOutputName(d))
      .filter((name) => name !== null) as string[];

    const subtotalsSpec: string[][] = [];
    for (let i = dimensionOutputNames.length; i > 0; i--) {
      subtotalsSpec.push(dimensionOutputNames.slice(0, i));
    }
    // 追加空数组表示总计层
    subtotalsSpec.push([]);
    mergedQuery.subtotalsSpec = subtotalsSpec;
  }

  if (virtualColumnsMap.size > 0) {
    mergedQuery.virtualColumns = Array.from(virtualColumnsMap.values());
  }

  // 合并 aggregations / postAggregations（以维度查询为准；必要时用模板回退）
  const aggMap = new Map<string, any>();
  const postAggMap = new Map<string, any>();

  for (const dq of dimensionQueries) {
    if (Array.isArray(dq.aggregations)) {
      for (const agg of dq.aggregations) {
        if (!agg || !agg.name) continue;
        if (agg.name === "__VALUE__") continue; // 跳过占位名
        if (!aggMap.has(agg.name)) aggMap.set(agg.name, agg);
      }
    }
    if (Array.isArray(dq.postAggregations)) {
      for (const pa of dq.postAggregations) {
        if (!pa || !pa.name) continue;
        if (!postAggMap.has(pa.name)) postAggMap.set(pa.name, pa);
      }
    }
  }

  const templateAggs = Array.isArray(templateQuery.aggregations)
    ? templateQuery.aggregations
    : [];
  const templatePostAggs = Array.isArray(templateQuery.postAggregations)
    ? templateQuery.postAggregations
    : [];

  if (aggMap.size === 0 && templateAggs.length > 0) {
    for (const agg of templateAggs) {
      if (!agg || !agg.name) continue;
      if (agg.name === "__VALUE__" && templateAggs.length > 1) continue;
      if (!aggMap.has(agg.name)) aggMap.set(agg.name, agg);
    }
  }

  if (postAggMap.size === 0 && templatePostAggs.length > 0) {
    for (const pa of templatePostAggs) {
      if (!pa || !pa.name) continue;
      if (!postAggMap.has(pa.name)) postAggMap.set(pa.name, pa);
    }
  }

  if (aggMap.size > 0) mergedQuery.aggregations = Array.from(aggMap.values());
  if (postAggMap.size > 0)
    mergedQuery.postAggregations = Array.from(postAggMap.values());

  if (explicitHaving) {
    mergedQuery.having = explicitHaving;
  } else {
    // 合并 having（按首次出现）
    for (const dq of dimensionQueries) {
      if (dq.having && !mergedQuery.having) mergedQuery.having = dq.having;
    }
  }

  // 合并排序
  const sortColumns = mergeSortColumns(dimensionQueries);
  if (sortColumns.length > 0) {
    mergedQuery.limitSpec = {
      type: "default",
      columns: sortColumns,
      limit:
        typeof maxQueries === "number" && maxQueries > 0 ? maxQueries : 10000,
    };
  }

  return mergedQuery;
}

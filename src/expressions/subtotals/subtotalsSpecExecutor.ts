/*
 * Executor for subtotalsSpec optimization.
 * 第三阶段：迁移执行器实现，保留 ResultProcessor 相关逻辑在宿主（下阶段迁移）。
 */

import {
  GroupByQuery,
  TimeseriesQuery,
  SplitExpressionsMap,
  DatasetLike,
} from "./subtotalsSpecTypes";
import { External } from "../../external/baseExternal";
import { Dataset } from "../../datatypes/index";
import {
  extractAttributesFromSubtotalsQuery,
  mergeTotalRowIntoSubtotalsResult,
  buildHierarchicalDataset,
  stripDummyPrefix,
} from "./subtotalsSpecResultProcessor";

// —— 工具函数 ——

function buildInflaters(query: any, splitExpressions: Map<string, any>): any[] {
  const inflaters: any[] = [];
  if (!query || !Array.isArray(query.dimensions)) return inflaters;

  query.dimensions.forEach((dimension: any) => {
    const rawName = dimension.outputName || dimension.dimension; // 可能包含 '***'
    const labelForMap = stripDummyPrefix(rawName);
    const labelForInflater = rawName; // 与返回数据字段名一致

    const expression = splitExpressions.get(labelForMap);
    if (expression) {
      const inflater = External.getInteligentInflater(
        expression,
        labelForInflater
      );
      if (inflater) inflaters.push(inflater);
    }
  });

  return inflaters;
}

/**
 * 执行包含 subtotalsSpec 的 groupBy 查询。
 */
export function executeSubtotalsQuery(
  host: any,
  query: GroupByQuery,
  druidExternal: any,
  options: any,
  splitExpressions: SplitExpressionsMap
): Promise<any> {
  if (!druidExternal) throw new Error("未找到 DruidExternal 数据源");

  const rawQueries = options && options.rawQueries;
  const customOptions = options && options.customOptions;
  const { engine } = druidExternal;
  const requester = druidExternal.requester;
  if (!requester) {
    throw new Error("DruidExternal 缺少 requester，请确保创建时传入 requester");
  }

  // 提取 attributes 与 keys（已迁移至 ResultProcessor）
  const extractedInfo = extractAttributesFromSubtotalsQuery(
    host,
    query,
    splitExpressions
  );

  // 生成 inflaters
  const inflaters = buildInflaters(query, splitExpressions);

  // 查询上下文
  const queryContext: any = {
    timestamp: null,
    ignorePrefix: "!",
    dummyPrefix: "***",
  };

  // postTransform
  const postTransform = External.postTransformFactory(
    inflaters,
    extractedInfo.attributes,
    extractedInfo.keys,
    null // zeroTotalApplies
  );

  const queryAndPostTransform = { query, context: queryContext, postTransform };

  const resultStream = External.performQueryAndPostTransform(
    queryAndPostTransform,
    requester,
    engine,
    rawQueries,
    customOptions || {}
  );

  return External.buildValueFromStream(resultStream);
}

/**
 * 执行 timeseries 总计查询，返回首行作为总计行。
 */
export function executeTotalRowQuery(
  _host: any,
  query: TimeseriesQuery | null,
  druidExternal: any,
  options: any,
  extractedInfo: { attributes: any[]; keys: string[] }
): Promise<any> {
  if (!query) return Promise.resolve(null);
  if (!druidExternal) throw new Error("未找到 DruidExternal 数据源");

  const rawQueries = options && options.rawQueries;
  const customOptions = options && options.customOptions;
  const { engine } = druidExternal;
  const requester = druidExternal.requester;
  if (!requester) {
    throw new Error("DruidExternal 缺少 requester，请确保创建时传入 requester");
  }

  const queryContext: any = {
    timestamp: null,
    ignorePrefix: "!",
    dummyPrefix: "***",
  };
  const postTransform = External.postTransformFactory(
    [], // timeseries 无维度
    extractedInfo && extractedInfo.attributes ? extractedInfo.attributes : [],
    [], // 无 keys
    null
  );

  const queryAndPostTransform = { query, context: queryContext, postTransform };
  const resultStream = External.performQueryAndPostTransform(
    queryAndPostTransform,
    requester,
    engine,
    rawQueries,
    customOptions || {}
  );

  return External.buildValueFromStream(resultStream).then((result: any) => {
    if (
      result &&
      result.data &&
      Array.isArray(result.data) &&
      result.data.length > 0
    ) {
      return result.data[0];
    }
    return null;
  });
}

/**
 * 并行执行 subtotals groupBy 与 timeseries（如存在），并进行合并与层级构建。
 */
export function executeQueriesInParallel(
  host: any,
  timeseriesQuery: TimeseriesQuery | null,
  mergedQuery: GroupByQuery,
  context: any,
  options: any,
  splitExpressionsMap: SplitExpressionsMap
): Promise<DatasetLike> {
  // 找到 DruidExternal
  const druidExternal = (host as any)._findDruidExternal(context);
  if (!druidExternal) throw new Error("未找到 DruidExternal 数据源");

  // 提取 attributes/keys（用于 inflaters 与结果处理）
  const extractedInfo = extractAttributesFromSubtotalsQuery(
    host,
    mergedQuery,
    splitExpressionsMap
  );

  // 1) subtotalsSpec 查询
  const subtotalsPromise = executeSubtotalsQuery(
    host,
    mergedQuery,
    druidExternal,
    options,
    splitExpressionsMap
  );

  // 2) timeseries 总计查询（可选）
  let totalRowPromise: Promise<any> | null = null;
  if (timeseriesQuery) {
    totalRowPromise = executeTotalRowQuery(
      host,
      timeseriesQuery,
      druidExternal,
      options,
      extractedInfo
    );
  }

  const promises: Promise<any>[] = [subtotalsPromise];
  if (totalRowPromise) promises.push(totalRowPromise);

  return Promise.all(promises).then((results) => {
    const subtotalsResult = results[0];
    const totalRowData = results[1];

    // 合并总计行
    const merged = mergeTotalRowIntoSubtotalsResult(
      host,
      subtotalsResult,
      totalRowData,
      extractedInfo.keys
    );

    // 层级构建
    const hierarchical = buildHierarchicalDataset(
      host,
      merged,
      mergedQuery,
      extractedInfo
    );

    return Dataset.fromJS(hierarchical) as any;
  });
}

/**
 * 暴露的 inflaters 生成函数（如需单测或复用）。
 */
export function generateInflaters(
  query: any,
  splitExpressionsMap: SplitExpressionsMap
): any[] {
  return buildInflaters(query, splitExpressionsMap);
}

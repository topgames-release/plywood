/*
 * Result processor for subtotalsSpec optimization.
 * 第四阶段：迁移结果处理实现为纯函数，供 Executor 调用。
 */

import { SplitExpressionsMap, DatasetLike } from "./subtotalsSpecTypes";
import { Duration, Timezone } from "@topgames/chronoshift";
import { TimeRange } from "../../datatypes/index";
import { NumberBucketExpression } from "../numberBucketExpression";
import { TimeBucketExpression } from "../timeBucketExpression";

// —— 工具函数 ——
export function stripDummyPrefix(name: string): string {
  const dummyPrefix = "***";
  if (typeof name === "string" && name.indexOf(dummyPrefix) === 0)
    return name.slice(dummyPrefix.length);
  return name;
}

function resolveActualKeyName(requestedKey: string, data: any[]): string {
  if (!Array.isArray(data) || data.length === 0) return requestedKey;
  const prefixed = "***" + requestedKey;
  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;
    if (Object.prototype.hasOwnProperty.call(row, requestedKey))
      return requestedKey;
    if (Object.prototype.hasOwnProperty.call(row, prefixed)) return prefixed;
  }
  return requestedKey;
}

function getTimeDimensionInfo(
  query: any,
  keyName: string
): { duration: Duration; timezone: Timezone } | null {
  if (!query || !Array.isArray(query.dimensions)) return null;
  for (const dim of query.dimensions) {
    const rawName = dim.outputName || dim.dimension;
    const outputName = stripDummyPrefix(rawName);
    if (outputName !== keyName) continue;
    if (
      (dim.dimension === "__time" || outputName === "__time") &&
      dim.extractionFn &&
      dim.extractionFn.type === "timeFormat" &&
      dim.extractionFn.granularity &&
      (dim.extractionFn.granularity.type === "period" ||
        typeof dim.extractionFn.granularity.period === "string")
    ) {
      const period = dim.extractionFn.granularity.period;
      const tz = dim.extractionFn.granularity.timeZone || "Etc/UTC";
      try {
        const duration = Duration.fromJS(period);
        const timezone = Timezone.fromJS(tz);
        return { duration, timezone };
      } catch (e) {
        return null;
      }
    }
  }
  return null;
}

function maybeConvertTimeKey(query: any, keyName: string, keyValue: any): any {
  if (keyName !== "__time") return keyValue;
  if (keyValue == null) return keyValue;
  const info = getTimeDimensionInfo(query, keyName);
  if (!info) return keyValue;
  let start: Date;
  if (keyValue instanceof Date) {
    start = keyValue as Date;
  } else if (typeof keyValue === "string" || typeof keyValue === "number") {
    let parsed: any = keyValue;
    if (typeof parsed === "string") {
      let s = parsed;
      if (/^\d{4}-\d{2}-\d{2}T\d{2}Z$/.test(s)) s = s.replace(/Z$/, ":00:00Z");
      else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}Z$/.test(s))
        s = s.replace(/Z$/, ":00Z");
      else if (/^\d{4}-\d{2}-\d{2}T\d{2}$/.test(s)) s = s + ":00:00Z";
      else if (/^\d{4}-\d{2}-\d{2}$/.test(s)) s = s + "T00:00:00Z";
      parsed = s;
    }
    const d = new Date(parsed);
    if (isNaN(d.getTime())) return keyValue;
    start = d;
  } else {
    return keyValue;
  }
  try {
    const end = info.duration.shift(start, info.timezone, 1);
    return new TimeRange({ start, end });
  } catch {
    return keyValue;
  }
}

// —— 导出函数 ——
export function extractAttributesFromSubtotalsQuery(
  _host: any,
  query: any,
  splitExpressions: SplitExpressionsMap
): { attributes: any[]; keys: string[] } {
  const attributes: any[] = [];
  const keys: string[] = [];
  if (query.dimensions && Array.isArray(query.dimensions)) {
    query.dimensions.forEach((dimension: any) => {
      const rawName = dimension.outputName || dimension.dimension;
      const outputName = stripDummyPrefix(rawName);
      keys.push(outputName);
      let attributeType = "STRING";
      const expression = splitExpressions.get(outputName);
      if (expression) {
        if (expression instanceof NumberBucketExpression)
          attributeType = "NUMBER_RANGE";
        else if (expression instanceof TimeBucketExpression)
          attributeType = "TIME_RANGE";
        else if (expression.type) attributeType = expression.type;
      } else if (
        dimension.outputType === "LONG" ||
        dimension.outputType === "FLOAT"
      ) {
        attributeType = "NUMBER";
      } else if (dimension.dimension === "__time" || outputName === "__time") {
        attributeType = "TIME_RANGE";
      }
      attributes.push({ name: outputName, type: attributeType });
    });
  }
  if (query.aggregations && Array.isArray(query.aggregations)) {
    query.aggregations.forEach((aggregation: any) => {
      attributes.push({ name: aggregation.name, type: "NUMBER" });
    });
  }
  if (query.postAggregations && Array.isArray(query.postAggregations)) {
    query.postAggregations.forEach((postAggregation: any) => {
      attributes.push({ name: postAggregation.name, type: "NUMBER" });
    });
  }
  return { attributes, keys };
}

export function mergeTotalRowIntoSubtotalsResult(
  _host: any,
  subtotalsResult: any,
  totalRowData: any,
  keys: string[]
): any {
  if (!subtotalsResult || !Array.isArray(subtotalsResult.data))
    return subtotalsResult;
  if (!totalRowData) return subtotalsResult;
  const data = subtotalsResult.data;
  const existingTotalRowIndex = data.findIndex((row: any) =>
    keys.every((k) => {
      const actualKey = resolveActualKeyName(k, [row]);
      return row[actualKey] === null || row[actualKey] === undefined;
    })
  );
  const enhancedTotalRow = { ...totalRowData };
  // 确保所有维度键为 null
  keys.forEach((k) => {
    const actualKey = resolveActualKeyName(k, data);
    if (!(actualKey in enhancedTotalRow)) enhancedTotalRow[actualKey] = null;
  });
  // 单 measure 时：将 timeseries 的 __VALUE__ 映射到真实度量名（如 activation）
  try {
    const attrs = Array.isArray(subtotalsResult.attributes)
      ? subtotalsResult.attributes
      : [];
    const candidateMeasures = attrs
      .filter((a: any) => a && a.type === "NUMBER" && !keys.includes(a.name))
      .map((a: any) => a.name);
    const effectiveMeasures = candidateMeasures.filter(
      (n: string) =>
        n !== "MillisecondsInInterval" && !/_MillisecondsInInterval$/.test(n)
    );
    if (
      effectiveMeasures.length === 1 &&
      Object.prototype.hasOwnProperty.call(enhancedTotalRow, "__VALUE__")
    ) {
      (enhancedTotalRow as any)[effectiveMeasures[0]] = (
        enhancedTotalRow as any
      ).__VALUE__;
      delete (enhancedTotalRow as any).__VALUE__;
    }
  } catch {}
  // 采用非破坏式合并，避免丢失 postAggregations 等已存在字段
  if (existingTotalRowIndex >= 0) {
    const original = data[existingTotalRowIndex] || {};
    data[existingTotalRowIndex] = { ...original, ...enhancedTotalRow };
  } else {
    data.unshift(enhancedTotalRow);
  }
  return { ...subtotalsResult, data };
}

function buildTopLevelAttributes(allAttributes: any[], keys: string[]): any[] {
  const topLevelAttributes: any[] = [];
  allAttributes.forEach((attr) => {
    if (!keys.includes(attr.name)) topLevelAttributes.push(attr);
  });
  topLevelAttributes.push({ name: "SPLIT", type: "DATASET" });
  return topLevelAttributes;
}

function buildSplitAttributes(
  allAttributes: any[],
  splitKey: string,
  keys: string[],
  currentLevel: number
): any[] {
  const splitAttributes: any[] = [];
  const keyAttribute = allAttributes.find((attr) => attr.name === splitKey);
  if (keyAttribute) splitAttributes.push(keyAttribute);
  allAttributes.forEach((attr) => {
    if (attr.type === "NUMBER") splitAttributes.push(attr);
  });
  if (currentLevel + 1 < keys.length)
    splitAttributes.push({ name: "SPLIT", type: "DATASET" });
  return splitAttributes;
}

export function applySortingToSplitData(splitData: any[], query: any): void {
  if (!query || !query.limitSpec || !Array.isArray(query.limitSpec.columns))
    return;
  const sortColumns = query.limitSpec.columns;
  if (sortColumns.length === 0) return;
  const coerceForCompare = (v: any): number | string | null => {
    if (v == null) return null;
    if (typeof v === "number") return v;
    if (v instanceof Date) return v.valueOf();
    if (typeof v === "object") {
      const maybeStart: any = (v as any).start;
      if (maybeStart instanceof Date) return maybeStart.valueOf();
      if (typeof maybeStart === "number") return maybeStart;
    }
    return String(v);
  };
  splitData.sort((a: any, b: any) => {
    for (const sortColumn of sortColumns) {
      const dimension = sortColumn.dimension;
      const direction = sortColumn.direction || "ascending";
      const aValue = coerceForCompare(a[dimension]);
      const bValue = coerceForCompare(b[dimension]);
      let comparison = 0;
      if (aValue == null && bValue == null) comparison = 0;
      else if (aValue == null) comparison = 1;
      else if (bValue == null) comparison = -1;
      else if (direction === "descending") {
        if (typeof aValue === "number" && typeof bValue === "number")
          comparison = (bValue as number) - (aValue as number);
        else {
          const aStr = String(aValue),
            bStr = String(bValue);
          comparison = bStr < aStr ? -1 : bStr > aStr ? 1 : 0;
        }
      } else {
        if (typeof aValue === "number" && typeof bValue === "number")
          comparison = (aValue as number) - (bValue as number);
        else {
          const aStr = String(aValue),
            bStr = String(bValue);
          comparison = aStr < bStr ? -1 : aStr > bStr ? 1 : 0;
        }
      }
      if (comparison !== 0) return comparison;
    }
    return 0;
  });
}

function buildSimpleSplit(
  data: any[],
  keys: string[],
  attributes: any[],
  level: number,
  query: any
): any {
  if (level >= keys.length) return null;
  const currentKey = keys[level];
  const actualKey = resolveActualKeyName(currentKey, data);
  const groups: Map<string, { rows: any[]; actualValue: any }> = new Map();
  const keyToStableString = (v: any): string => {
    if (v == null) return "__NULL__";
    if (typeof v === "number") return String(v);
    if (v instanceof Date) return String(v.valueOf());
    if (typeof v === "object") {
      const maybeStart: any = (v as any).start;
      if (maybeStart instanceof Date) return String(maybeStart.valueOf());
      if (typeof maybeStart === "number") return String(maybeStart);
    }
    return String(v);
  };
  data.forEach((row: any) => {
    const keyValue = row[actualKey];
    if (keyValue !== null && keyValue !== undefined) {
      const groupKey = keyToStableString(keyValue);
      if (!groups.has(groupKey))
        groups.set(groupKey, { rows: [], actualValue: keyValue });
      groups.get(groupKey)!.rows.push(row);
    }
  });
  const splitData: any[] = [];
  groups.forEach(({ rows: groupData, actualValue }) => {
    const splitItem: any = {};
    const maybeValue = maybeConvertTimeKey(query, currentKey, actualValue);
    splitItem[currentKey] = maybeValue;
    const aggregateRow = groupData.find((row: any) => {
      const curMatches =
        keyToStableString(row[actualKey]) === keyToStableString(actualValue);
      if (!curMatches) return false;
      if (level + 1 >= keys.length) return true;
      return keys.slice(level + 1).every((k) => {
        const ak = resolveActualKeyName(k, [row]);
        return row[ak] === null || row[ak] === undefined;
      });
    });
    if (aggregateRow) {
      attributes.forEach((attr: any) => {
        if (attr.type === "NUMBER" && aggregateRow[attr.name] !== undefined) {
          splitItem[attr.name] = aggregateRow[attr.name];
        }
      });
    }
    if (level + 1 < keys.length) {
      const nestedSplit = buildSimpleSplit(
        groupData,
        keys,
        attributes,
        level + 1,
        query
      );
      if (nestedSplit && nestedSplit.data.length > 0)
        splitItem.SPLIT = nestedSplit;
    }
    splitData.push(splitItem);
  });
  applySortingToSplitData(splitData, query);
  return {
    keys: [currentKey],
    attributes: buildSplitAttributes(attributes, currentKey, keys, level),
    data: splitData,
  };
}

// —— 修复辅助：在无 __time 维度时基于 intervals 计算毫秒并仅在顶层注入 ——
function hasExplicitTimeDimensionInGroupBy(query: any): boolean {
  if (!query || !Array.isArray(query.dimensions)) return false;
  return query.dimensions.some((dim: any) => {
    const dimName = (dim && (dim.dimension || dim.outputName)) || null;
    return dimName === "__time";
  });
}

function parseISOIntervalToMillis(interval: string): number | null {
  if (typeof interval !== "string") return null;
  const parts = interval.split("/");
  if (parts.length !== 2) return null;

  const normalize = (s: string): string => {
    let t = (s || "").trim();
    // 补全缺省的时间/秒/时区，保证 Date 可解析
    if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t + "T00:00:00Z";
    if (/^\d{4}-\d{2}-\d{2}T\d{2}$/.test(t)) return t + ":00:00Z";
    if (/^\d{4}-\d{2}-\d{2}T\d{2}Z$/.test(t)) return t.replace(/Z$/, ":00:00Z");
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(t)) return t + ":00Z";
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}Z$/.test(t))
      return t.replace(/Z$/, ":00Z");
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(t)) return t + "Z";
    return t;
  };

  const startStr = normalize(parts[0]);
  const endStr = normalize(parts[1]);
  const start = new Date(startStr).valueOf();
  const end = new Date(endStr).valueOf();
  if (!isFinite(start) || !isFinite(end)) return null;
  const diff = end - start;
  return diff >= 0 ? diff : null;
}

function computeIntervalMillisFromQuery(query: any): number | null {
  if (!query) return null;
  const { intervals } = query as any;
  if (!intervals) return null;
  if (Array.isArray(intervals) && intervals.length > 0) {
    // 取第一个区间
    const first = intervals[0];
    if (typeof first === "string") return parseISOIntervalToMillis(first);
    if (
      first &&
      typeof first.start === "string" &&
      typeof first.end === "string"
    ) {
      return parseISOIntervalToMillis(`${first.start}/${first.end}`);
    }
    return null;
  }
  if (typeof intervals === "string") return parseISOIntervalToMillis(intervals);
  return null;
}

export function buildHierarchicalDataset(
  _host: any,
  flatResult: any,
  query: any,
  extractedInfo: { attributes: any[]; keys: string[] }
): any {
  if (!flatResult || !Array.isArray(flatResult.data)) return flatResult;
  const { attributes, keys } = extractedInfo;
  const { subtotalsSpec } = query;
  if (
    !subtotalsSpec ||
    !Array.isArray(subtotalsSpec) ||
    subtotalsSpec.length === 0
  )
    return flatResult;

  const data = flatResult.data;
  const actualKeys = keys.map((k) => resolveActualKeyName(k, data));
  const totalRow = data.find((row: any) =>
    actualKeys.every((ak) => row[ak] == null)
  );
  if (!totalRow) return flatResult;

  // 构建顶层数据：仅从总计行复制数值指标
  const topLevelData: any = {};
  attributes.forEach((attr: any) => {
    if (attr.type === "NUMBER" && totalRow[attr.name] !== undefined)
      topLevelData[attr.name] = totalRow[attr.name];
  });

  // 构建 SPLIT（不向嵌套层注入 *_MillisecondsInInterval）
  if (keys.length > 0) {
    const splitData = buildSimpleSplit(data, keys, attributes, 0, query);
    if (splitData && splitData.data.length > 0) topLevelData.SPLIT = splitData;
  }

  // 注入 MillisecondsInInterval（仅顶层）：
  // 触发条件：groupBy.dimensions 不包含 __time；
  // 数值来源：timeseries 模板继承到 merged groupBy 的 intervals（start/end 差）
  const hasTimeDim = hasExplicitTimeDimensionInGroupBy(query);
  const intervalMs = hasTimeDim ? null : computeIntervalMillisFromQuery(query);

  let finalTopLevelAttributes = buildTopLevelAttributes(attributes, keys);
  if (intervalMs != null) {
    try {
      // 计算需要追加后缀字段的指标集合（排除维度键与已存在的 *_MillisecondsInInterval）
      const metricNames: string[] = attributes
        .filter(
          (a: any) =>
            a &&
            a.type === "NUMBER" &&
            !keys.includes(a.name) &&
            a.name !== "__VALUE__" &&
            a.name !== "MillisecondsInInterval" &&
            !/_MillisecondsInInterval$/.test(a.name)
        )
        .map((a: any) => a.name);

      // 仅在顶层数据注入，不更改用于 SPLIT 的 attributes
      if (topLevelData["MillisecondsInInterval"] === undefined)
        topLevelData["MillisecondsInInterval"] = intervalMs;
      for (const m of metricNames) {
        const fieldName = `${m}_MillisecondsInInterval`;
        if (topLevelData[fieldName] === undefined)
          topLevelData[fieldName] = intervalMs;
      }

      // 顶层 attributes 需要包含新增的字段；嵌套 SPLIT 不包含
      const baseTopLevel = finalTopLevelAttributes.filter(
        (a) => a && a.name !== "SPLIT"
      );
      // Replace Set with an object-backed membership to avoid env/polyfill issues
      const __haveBacking: Record<string, true> = Object.create(null);
      for (const a of Array.isArray(baseTopLevel) ? baseTopLevel : []) {
        if (a && typeof a.name === "string") __haveBacking[a.name] = true;
      }
      const have = {
        has: (k: string) =>
          Object.prototype.hasOwnProperty.call(__haveBacking, k),
        add: (k: string) => {
          __haveBacking[k] = true as true;
        },
      } as { has: (k: string) => boolean; add: (k: string) => void };
      if (!have.has("MillisecondsInInterval")) {
        baseTopLevel.push({ name: "MillisecondsInInterval", type: "NUMBER" });
        have.add("MillisecondsInInterval");
      }
      for (const m of metricNames) {
        const fn = `${m}_MillisecondsInInterval`;
        if (!have.has(fn)) baseTopLevel.push({ name: fn, type: "NUMBER" });
      }
      // 保持 SPLIT 的位置与存在性
      if (finalTopLevelAttributes.some((a) => a.name === "SPLIT")) {
        baseTopLevel.push({ name: "SPLIT", type: "DATASET" });
      }
      finalTopLevelAttributes = baseTopLevel;
    } catch (e) {
      try {
        console.error("[subtotalsSpec][inject-ms] block error (non-fatal)", {
          error: ((e as any) && (e as any).message) || String(e),
        });
      } catch {}
      // 非致命：出现异常时不影响整体流程，继续返回已构建的数据
    }
  }

  return {
    attributes: finalTopLevelAttributes,
    keys: [],
    data: [topLevelData],
  };
}

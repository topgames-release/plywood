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
  const topLevelData: any = {};
  attributes.forEach((attr: any) => {
    if (attr.type === "NUMBER" && totalRow[attr.name] !== undefined)
      topLevelData[attr.name] = totalRow[attr.name];
  });
  if (keys.length > 0) {
    const splitData = buildSimpleSplit(data, keys, attributes, 0, query);
    if (splitData && splitData.data.length > 0) topLevelData.SPLIT = splitData;
  }
  return {
    attributes: buildTopLevelAttributes(attributes, keys),
    keys: [],
    data: [topLevelData],
  };
}

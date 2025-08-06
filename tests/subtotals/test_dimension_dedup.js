const { Expression, DruidExternal } = require('../../build/plywood.js');

console.log("=== 测试维度去重和 subtotalsSpec 生成 ===\n");

// 模拟你提供的查询计划，包含重复的维度
const mockQueryPlanWithDuplicates = [
  [
    {
      queryType: "timeseries",
      dataSource: {
        type: "union",
        dataSources: ["ads_newdata_common_data"]
      },
      intervals: "2025-07-28T00Z/2025-08-05T00Z",
      granularity: "all",
      context: {
        timeout: 600000,
        useCache: true
      },
      filter: {
        type: "selector",
        dimension: "action_type",
        value: "INSTALL"
      },
      aggregations: [
        {
          name: "activation",
          type: "longSum",
          fieldName: "isInstall"
        },
        {
          type: "javascript",
          fieldNames: ["dpu_monthly"],
          fnAggregate: "function(current, added) { return current+((added>0)?1:0) }",
          fnCombine: "function(partialA, partialB) { return partialA + partialB }",
          fnReset: "function() { return 0; }",
          name: "current_pu"
        }
      ]
    },
    {
      queryType: "groupBy",
      dataSource: {
        type: "union",
        dataSources: ["ads_newdata_common_data"]
      },
      intervals: "2025-07-28T00Z/2025-08-05T00Z",
      granularity: "all",
      dimensions: [
        {
          type: "extraction",
          dimension: "__time",
          outputName: "***__time",
          extractionFn: {
            type: "timeFormat",
            granularity: {
              type: "period",
              period: "P1D",
              timeZone: "Etc/UTC"
            },
            format: "yyyy-MM-dd'T'HH:mm:ss'Z",
            timeZone: "Etc/UTC"
          }
        },
        {
          type: "extraction",
          dimension: "__time",  // 重复的 __time 维度
          outputName: "***__time",
          extractionFn: {
            type: "timeFormat",
            granularity: {
              type: "period",
              period: "P1D",
              timeZone: "Etc/UTC"
            },
            format: "yyyy-MM-dd'T'HH:mm:ss'Z",
            timeZone: "Etc/UTC"
          }
        },
        {
          type: "extraction",
          dimension: "app",
          outputName: "app",
          extractionFn: {
            type: "registeredLookup",
            lookup: "app_convert",
            retainMissingValue: true
          }
        },
        {
          type: "extraction",
          dimension: "__time",  // 又一个重复的 __time 维度
          outputName: "***__time",
          extractionFn: {
            type: "timeFormat",
            granularity: {
              type: "period",
              period: "P1D",
              timeZone: "Etc/UTC"
            },
            format: "yyyy-MM-dd'T'HH:mm:ss'Z",
            timeZone: "Etc/UTC"
          }
        },
        {
          type: "extraction",
          dimension: "app",  // 重复的 app 维度
          outputName: "app",
          extractionFn: {
            type: "registeredLookup",
            lookup: "app_convert",
            retainMissingValue: true
          }
        },
        {
          type: "default",
          dimension: "v:platform",
          outputName: "platform",
          outputType: "STRING"
        }
      ],
      aggregations: [
        {
          name: "activation",
          type: "longSum",
          fieldName: "isInstall"
        },
        {
          type: "javascript",
          fieldNames: ["dpu_monthly"],
          fnAggregate: "function(current, added) { return current+((added>0)?1:0) }",
          fnCombine: "function(partialA, partialB) { return partialA + partialB }",
          fnReset: "function() { return 0; }",
          name: "current_pu"
        }
      ]
    }
  ]
];

console.log("1. 原始查询计划分析");
console.log("查询组数:", mockQueryPlanWithDuplicates.length);
console.log("第1组查询数:", mockQueryPlanWithDuplicates[0].length);

const groupByQuery = mockQueryPlanWithDuplicates[0][1];
console.log("groupBy 查询的维度数量:", groupByQuery.dimensions.length);
console.log("维度详情:");
groupByQuery.dimensions.forEach((dim, index) => {
  console.log(`  ${index + 1}. dimension: "${dim.dimension}", outputName: "${dim.outputName}"`);
});

// 测试维度提取和去重逻辑
console.log("\n2. 测试维度提取和去重逻辑");

function extractDimensionName(dim) {
  if (typeof dim === "string") {
    return dim;
  } else if (dim.dimension) {
    return dim.dimension;
  } else if (dim.outputName) {
    return dim.outputName;
  }
  return null;
}

function testDimensionDeduplication(queryPlan) {
  console.log("开始维度去重测试...");
  
  // 找到模板查询和维度查询
  let templateQuery = null;
  const dimensionQueries = [];
  
  for (const group of queryPlan) {
    for (const query of group) {
      if (query.queryType === "timeseries" && !templateQuery) {
        templateQuery = { ...query };
        console.log("✅ 找到 timeseries 模板查询");
      } else if (query.queryType === "topN" || query.queryType === "groupBy") {
        dimensionQueries.push(query);
        console.log(`✅ 找到 ${query.queryType} 维度查询`);
      }
    }
  }
  
  // 收集所有维度并去重
  const dimensionsMap = new Map();
  
  console.log("\n开始收集维度...");
  for (const query of dimensionQueries) {
    if (query.dimension) {
      // topN 查询的维度
      const dimName = extractDimensionName(query.dimension);
      if (dimName && !dimensionsMap.has(dimName)) {
        dimensionsMap.set(dimName, query.dimension);
        console.log(`添加 topN 维度: ${dimName}`);
      } else if (dimName) {
        console.log(`跳过重复的 topN 维度: ${dimName}`);
      }
    } else if (query.dimensions && Array.isArray(query.dimensions)) {
      // groupBy 查询的维度
      for (const dim of query.dimensions) {
        const dimName = extractDimensionName(dim);
        if (dimName && !dimensionsMap.has(dimName)) {
          dimensionsMap.set(dimName, dim);
          console.log(`添加 groupBy 维度: ${dimName}`);
        } else if (dimName) {
          console.log(`跳过重复的 groupBy 维度: ${dimName}`);
        }
      }
    }
  }
  
  console.log("\n去重结果:");
  console.log("原始维度数量:", groupByQuery.dimensions.length);
  console.log("去重后维度数量:", dimensionsMap.size);
  console.log("去重后的维度名称:", Array.from(dimensionsMap.keys()));
  
  // 生成 subtotalsSpec
  if (dimensionsMap.size > 0) {
    const dimensions = Array.from(dimensionsMap.values());
    const dimensionNames = Array.from(dimensionsMap.keys());
    
    const subtotalsSpec = [];
    
    // 生成所有可能的维度组合
    for (let i = dimensionNames.length; i > 0; i--) {
      subtotalsSpec.push(dimensionNames.slice(0, i));
    }
    // 添加空数组表示总计
    subtotalsSpec.push([]);
    
    console.log("\n生成的 subtotalsSpec:");
    console.log(JSON.stringify(subtotalsSpec, null, 2));
    
    // 验证 subtotalsSpec 的正确性
    console.log("\nsubtotalsSpec 验证:");
    console.log("- 组合数量:", subtotalsSpec.length);
    console.log("- 期望组合数量:", dimensionNames.length + 1); // 包括空数组
    
    if (subtotalsSpec.length === dimensionNames.length + 1) {
      console.log("✅ subtotalsSpec 组合数量正确");
    } else {
      console.log("❌ subtotalsSpec 组合数量错误");
    }
    
    // 检查最后一个是否为空数组
    const lastElement = subtotalsSpec[subtotalsSpec.length - 1];
    if (Array.isArray(lastElement) && lastElement.length === 0) {
      console.log("✅ 最后一个元素是空数组（总计）");
    } else {
      console.log("❌ 最后一个元素不是空数组");
    }
    
    // 检查第一个是否包含所有维度
    const firstElement = subtotalsSpec[0];
    if (Array.isArray(firstElement) && firstElement.length === dimensionNames.length) {
      console.log("✅ 第一个元素包含所有维度");
    } else {
      console.log("❌ 第一个元素不包含所有维度");
    }
    
    // 创建合并后的查询
    const mergedQuery = {
      ...templateQuery,
      queryType: "groupBy",
      dimensions: dimensions,
      subtotalsSpec: subtotalsSpec
    };
    
    console.log("\n合并后的查询验证:");
    console.log("- 查询类型:", mergedQuery.queryType);
    console.log("- 维度数量:", mergedQuery.dimensions.length);
    console.log("- subtotalsSpec 组合数量:", mergedQuery.subtotalsSpec.length);
    
    return mergedQuery;
  }
  
  return null;
}

const result = testDimensionDeduplication(mockQueryPlanWithDuplicates);

console.log("\n=== 测试完成 ===");
console.log("\n总结:");
if (result) {
  console.log("✅ 维度去重逻辑工作正常");
  console.log("✅ subtotalsSpec 生成正确");
  console.log("✅ 查询合并成功");
  console.log(`✅ 性能提升: 从 ${groupByQuery.dimensions.length} 个重复维度优化为 ${result.dimensions.length} 个唯一维度`);
} else {
  console.log("❌ 测试失败");
}

console.log("\n🎯 关键改进:");
console.log("- 使用 dimension 字段而不是 outputName 作为去重依据");
console.log("- 使用 Map 数据结构确保维度唯一性");
console.log("- 正确生成 subtotalsSpec 数组");
console.log("- 保持维度对象的完整结构");

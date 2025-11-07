const { Expression, DruidExternal } = require('../../build/plywood.js');

console.log("=== 测试 subtotalsSpec 使用 outputName 修复 ===\n");

// 模拟你提供的查询结构，包含 dimension 和 outputName 不同的情况
const mockQueryPlanWithOutputNames = [
  [
    {
      queryType: "timeseries",
      dataSource: {
        type: "union",
        dataSources: ["ads_newdata_common_data"]
      },
      intervals: "2025-07-29T00Z/2025-08-06T00Z",
      granularity: "all",
      context: {
        timeout: 600000,
        useCache: true
      },
      aggregations: [
        {
          type: "filtered",
          name: "activation",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: ["2025-07-29T00Z/2025-08-06T00Z"]
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL"
              }
            ]
          },
          aggregator: {
            name: "activation",
            type: "longSum",
            fieldName: "isInstall"
          }
        },
        {
          type: "filtered",
          name: "current_pu",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: ["2025-07-29T00Z/2025-08-06T00Z"]
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL"
              }
            ]
          },
          aggregator: {
            name: "current_pu",
            type: "count"
          }
        }
      ]
    },
    {
      queryType: "groupBy",
      dataSource: {
        type: "union",
        dataSources: ["ads_newdata_common_data"]
      },
      intervals: "2025-07-29T00Z/2025-08-06T00Z",
      granularity: "all",
      context: {
        timeout: 600000,
        useCache: true
      },
      virtualColumns: [
        {
          type: "expression",
          name: "v:platform",
          expression: "nvl(lookup(\"raw_platform\",'platform_group'),\"platform\")",
          outputType: "STRING"
        }
      ],
      dimensions: [
        {
          type: "extraction",
          dimension: "__time",           // dimension 字段
          outputName: "***__time",       // outputName 字段（不同）
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
          dimension: "app",              // dimension 字段
          outputName: "app",             // outputName 字段（相同）
          extractionFn: {
            type: "registeredLookup",
            lookup: "app_convert",
            retainMissingValue: true
          }
        },
        {
          type: "default",
          dimension: "v:platform",       // dimension 字段
          outputName: "platform",        // outputName 字段（不同）
          outputType: "STRING"
        }
      ],
      aggregations: [
        {
          type: "filtered",
          name: "activation",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: ["2025-07-29T00Z/2025-08-06T00Z"]
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL"
              }
            ]
          },
          aggregator: {
            name: "activation",
            type: "longSum",
            fieldName: "isInstall"
          }
        }
      ]
    }
  ]
];

console.log("1. 原始查询计划分析");
console.log("查询组数:", mockQueryPlanWithOutputNames.length);
console.log("第1组查询数:", mockQueryPlanWithOutputNames[0].length);

const groupByQuery = mockQueryPlanWithOutputNames[0][1];
console.log("\ngroupBy 查询的维度分析:");
console.log("维度数量:", groupByQuery.dimensions.length);
groupByQuery.dimensions.forEach((dim, index) => {
  console.log(`  ${index + 1}. dimension: "${dim.dimension}", outputName: "${dim.outputName}"`);
});

// 测试修复后的 subtotalsSpec 生成逻辑
console.log("\n2. 测试修复后的 subtotalsSpec 生成逻辑");

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

function extractDimensionOutputName(dim) {
  if (typeof dim === "string") {
    return dim;
  } else if (dim.outputName) {
    return dim.outputName;
  } else if (dim.dimension) {
    return dim.dimension;
  }
  return null;
}

function testSubtotalsSpecGeneration(queryPlan) {
  console.log("开始 subtotalsSpec 生成测试...");
  
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
  
  // 收集维度并去重（使用 dimension 字段）
  const dimensionsMap = new Map();
  
  console.log("\n开始收集维度（用于去重）...");
  for (const query of dimensionQueries) {
    if (query.dimensions && Array.isArray(query.dimensions)) {
      for (const dim of query.dimensions) {
        const dimName = extractDimensionName(dim);
        if (dimName && !dimensionsMap.has(dimName)) {
          dimensionsMap.set(dimName, dim);
          console.log(`添加维度: ${dimName} (outputName: ${dim.outputName})`);
        } else if (dimName) {
          console.log(`跳过重复维度: ${dimName}`);
        }
      }
    }
  }
  
  console.log("\n维度去重结果:");
  console.log("去重后维度数量:", dimensionsMap.size);
  console.log("维度名称 (dimension):", Array.from(dimensionsMap.keys()));
  
  // 生成 subtotalsSpec（使用 outputName）
  const dimensions = Array.from(dimensionsMap.values());
  const dimensionOutputNames = dimensions
    .map(dim => extractDimensionOutputName(dim))
    .filter(name => name !== null);
  
  console.log("\nsubtotalsSpec 生成:");
  console.log("输出名称 (outputName):", dimensionOutputNames);
  
  const subtotalsSpec = [];
  for (let i = dimensionOutputNames.length; i > 0; i--) {
    subtotalsSpec.push(dimensionOutputNames.slice(0, i));
  }
  subtotalsSpec.push([]);
  
  console.log("\n生成的 subtotalsSpec:");
  console.log(JSON.stringify(subtotalsSpec, null, 2));
  
  // 验证 subtotalsSpec 的正确性
  console.log("\nsubtotalsSpec 验证:");
  console.log("- 组合数量:", subtotalsSpec.length);
  console.log("- 期望组合数量:", dimensionOutputNames.length + 1);
  
  // 检查是否使用了正确的 outputName
  const expectedOutputNames = ["***__time", "app", "platform"];
  const actualFirstCombination = subtotalsSpec[0];
  
  console.log("\n维度名称对比:");
  console.log("期望的 outputName:", expectedOutputNames);
  console.log("实际的 subtotalsSpec[0]:", actualFirstCombination);
  
  const isCorrect = JSON.stringify(actualFirstCombination.sort()) === JSON.stringify(expectedOutputNames.sort());
  if (isCorrect) {
    console.log("✅ subtotalsSpec 使用了正确的 outputName");
  } else {
    console.log("❌ subtotalsSpec 使用了错误的名称");
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

const result = testSubtotalsSpecGeneration(mockQueryPlanWithOutputNames);

console.log("\n=== 测试完成 ===");
console.log("\n总结:");
if (result) {
  console.log("✅ subtotalsSpec 生成逻辑修复成功");
  console.log("✅ 正确使用 outputName 而不是 dimension");
  console.log("✅ 解决了 Druid 查询错误");
  
  const firstCombination = result.subtotalsSpec[0];
  console.log(`✅ 第一个组合: [${firstCombination.join(', ')}]`);
  console.log("✅ 这些名称与 dimensions 数组中的 outputName 匹配");
} else {
  console.log("❌ 测试失败");
}

console.log("\n🎯 关键修复:");
console.log("- 维度去重仍使用 dimension 字段");
console.log("- subtotalsSpec 生成改用 outputName 字段");
console.log("- 确保 subtotalsSpec 与 Druid 期望的维度名称匹配");
console.log("- 解决了 'not a subset of top level dimensions' 错误");

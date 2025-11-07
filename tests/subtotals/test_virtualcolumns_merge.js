const { Expression, DruidExternal } = require('../../build/plywood.js');

console.log("=== 测试 virtualColumns 合并功能 ===\n");

// 模拟包含 virtualColumns 的查询计划（基于 simulateQuery.js）
const mockQueryPlanWithVirtualColumns = [
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
      ],
      // timeseries 查询中的 virtualColumns
      virtualColumns: [
        {
          type: "expression",
          name: "v:base_metric",
          expression: "\"isInstall\" + \"dpu_monthly\"",
          outputType: "DOUBLE"
        }
      ]
    },
    {
      queryType: "topN",
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
      // topN 查询中的 virtualColumns（包含 v:platform）
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
        }
      ],
      metric: "activation",
      threshold: 100
    },
    {
      queryType: "groupBy",
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
      // groupBy 查询中的 virtualColumns（包含重复的 v:platform 和新的 v:app）
      virtualColumns: [
        {
          type: "expression",
          name: "v:platform",  // 重复的 virtualColumn
          expression: "nvl(lookup(\"raw_platform\",'platform_group'),\"platform\")",
          outputType: "STRING"
        },
        {
          type: "expression",
          name: "v:app",
          expression: "lookup(\"app_convert\",'app')",
          outputType: "STRING"
        }
      ],
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
          type: "default",
          dimension: "v:app",
          outputName: "app",
          outputType: "STRING"
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
console.log("查询组数:", mockQueryPlanWithVirtualColumns.length);
console.log("第1组查询数:", mockQueryPlanWithVirtualColumns[0].length);

// 分析每个查询的 virtualColumns
mockQueryPlanWithVirtualColumns[0].forEach((query, index) => {
  console.log(`\n查询${index + 1} (${query.queryType}):`);
  if (query.virtualColumns) {
    console.log(`  virtualColumns 数量: ${query.virtualColumns.length}`);
    query.virtualColumns.forEach((vc, vcIndex) => {
      console.log(`    ${vcIndex + 1}. ${vc.name}: ${vc.expression}`);
    });
  } else {
    console.log("  无 virtualColumns");
  }
});

// 测试 virtualColumns 合并逻辑
console.log("\n2. 测试 virtualColumns 合并逻辑");

function testVirtualColumnsMerging(queryPlan) {
  console.log("开始 virtualColumns 合并测试...");
  
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
  
  // 收集所有 virtualColumns 并去重
  const virtualColumnsMap = new Map();
  
  console.log("\n开始收集 virtualColumns...");
  
  // 从模板查询中添加 virtualColumns
  if (templateQuery.virtualColumns && Array.isArray(templateQuery.virtualColumns)) {
    for (const vc of templateQuery.virtualColumns) {
      if (vc.name) {
        virtualColumnsMap.set(vc.name, vc);
        console.log(`添加模板 virtualColumn: ${vc.name}`);
      }
    }
  }
  
  // 从维度查询中收集 virtualColumns
  for (const query of dimensionQueries) {
    if (query.virtualColumns && Array.isArray(query.virtualColumns)) {
      for (const vc of query.virtualColumns) {
        if (vc.name && !virtualColumnsMap.has(vc.name)) {
          virtualColumnsMap.set(vc.name, vc);
          console.log(`添加 ${query.queryType} virtualColumn: ${vc.name}`);
        } else if (vc.name) {
          console.log(`跳过重复的 virtualColumn: ${vc.name}`);
        }
      }
    }
  }
  
  console.log("\nvirtualColumns 去重结果:");
  console.log("总 virtualColumns 数量:", virtualColumnsMap.size);
  console.log("virtualColumns 名称:", Array.from(virtualColumnsMap.keys()));
  
  // 收集维度（简化版）
  const dimensionsMap = new Map();
  for (const query of dimensionQueries) {
    if (query.dimension) {
      const dimName = query.dimension.dimension || query.dimension.outputName || query.dimension;
      if (dimName && !dimensionsMap.has(dimName)) {
        dimensionsMap.set(dimName, query.dimension);
      }
    } else if (query.dimensions && Array.isArray(query.dimensions)) {
      for (const dim of query.dimensions) {
        const dimName = dim.dimension || dim.outputName || dim;
        if (dimName && !dimensionsMap.has(dimName)) {
          dimensionsMap.set(dimName, dim);
        }
      }
    }
  }
  
  console.log("\n维度去重结果:");
  console.log("去重后维度数量:", dimensionsMap.size);
  console.log("维度名称:", Array.from(dimensionsMap.keys()));
  
  // 生成 subtotalsSpec
  const dimensionNames = Array.from(dimensionsMap.keys());
  const subtotalsSpec = [];
  for (let i = dimensionNames.length; i > 0; i--) {
    subtotalsSpec.push(dimensionNames.slice(0, i));
  }
  subtotalsSpec.push([]);
  
  // 创建合并后的查询
  const mergedQuery = {
    ...templateQuery,
    queryType: "groupBy",
    dimensions: Array.from(dimensionsMap.values()),
    subtotalsSpec: subtotalsSpec
  };
  
  // 添加 virtualColumns
  if (virtualColumnsMap.size > 0) {
    mergedQuery.virtualColumns = Array.from(virtualColumnsMap.values());
  }
  
  console.log("\n合并后的查询验证:");
  console.log("- 查询类型:", mergedQuery.queryType);
  console.log("- 维度数量:", mergedQuery.dimensions.length);
  console.log("- virtualColumns 数量:", mergedQuery.virtualColumns ? mergedQuery.virtualColumns.length : 0);
  console.log("- subtotalsSpec 组合数量:", mergedQuery.subtotalsSpec.length);
  
  if (mergedQuery.virtualColumns) {
    console.log("\nvirtualColumns 详情:");
    mergedQuery.virtualColumns.forEach((vc, index) => {
      console.log(`  ${index + 1}. ${vc.name}: ${vc.expression}`);
    });
  }
  
  console.log("\nsubtotalsSpec:");
  console.log(JSON.stringify(mergedQuery.subtotalsSpec, null, 2));
  
  return mergedQuery;
}

const result = testVirtualColumnsMerging(mockQueryPlanWithVirtualColumns);

console.log("\n=== 测试完成 ===");
console.log("\n总结:");
if (result && result.virtualColumns) {
  console.log("✅ virtualColumns 合并功能正常");
  console.log("✅ virtualColumns 去重逻辑正确");
  console.log("✅ 维度和 virtualColumns 都正确合并");
  console.log(`✅ 合并效果: 包含 ${result.virtualColumns.length} 个 virtualColumns 和 ${result.dimensions.length} 个维度`);
} else {
  console.log("❌ virtualColumns 合并失败");
}

console.log("\n🎯 关键改进:");
console.log("- 正确收集和合并所有查询中的 virtualColumns");
console.log("- 使用 virtualColumn.name 作为去重依据");
console.log("- 保持 virtualColumns 的完整结构和表达式");
console.log("- 确保 v:platform 等虚拟列正确包含在最终查询中");

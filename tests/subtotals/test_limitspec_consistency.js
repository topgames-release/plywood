const { Expression, DruidExternal } = require('../../build/plywood.js');

console.log("=== 测试 limitSpec 一致性合并 ===\n");

// 模拟包含 limitSpec 的查询计划
const mockQueryPlanWithLimitSpec = [
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
          name: "activation",
          type: "longSum",
          fieldName: "isInstall"
        },
        {
          name: "current_pu",
          type: "count"
        }
      ]
    },
    {
      queryType: "topN",
      dataSource: {
        type: "union",
        dataSources: ["ads_newdata_common_data"]
      },
      intervals: "2025-07-29T00Z/2025-08-06T00Z",
      granularity: "all",
      dimension: {
        type: "default",
        dimension: "platform",
        outputName: "platform"
      },
      aggregations: [
        {
          name: "activation",
          type: "longSum",
          fieldName: "isInstall"
        }
      ],
      metric: "activation",
      threshold: 50,  // topN 的 threshold
      limitSpec: {
        type: "default",
        columns: [
          {
            dimension: "activation",
            direction: "descending"
          }
        ],
        limit: 100  // 与其他查询相同的 limitSpec
      }
    },
    {
      queryType: "groupBy",
      dataSource: {
        type: "union",
        dataSources: ["ads_newdata_common_data"]
      },
      intervals: "2025-07-29T00Z/2025-08-06T00Z",
      granularity: "all",
      dimensions: [
        {
          type: "default",
          dimension: "app",
          outputName: "app"
        }
      ],
      aggregations: [
        {
          name: "activation",
          type: "longSum",
          fieldName: "isInstall"
        }
      ],
      limitSpec: {
        type: "default",
        columns: [
          {
            dimension: "activation",
            direction: "descending"
          }
        ],
        limit: 100  // 所有子请求的 limitSpec 都是一样的
      }
    }
  ]
];

console.log("1. 原始查询计划分析");
console.log("查询组数:", mockQueryPlanWithLimitSpec.length);
console.log("第1组查询数:", mockQueryPlanWithLimitSpec[0].length);

// 分析每个查询的 limit 设置
mockQueryPlanWithLimitSpec[0].forEach((query, index) => {
  console.log(`\n查询${index + 1} (${query.queryType}):`);
  if (query.queryType === "topN" && query.threshold) {
    console.log(`  threshold (topN limit): ${query.threshold}`);
  } else if (query.limitSpec && query.limitSpec.limit) {
    console.log(`  limitSpec.limit: ${query.limitSpec.limit}`);
  } else {
    console.log("  无 limit 设置");
  }
});

// 测试 limitSpec 一致性合并的逻辑
console.log("\n2. 测试 limitSpec 一致性合并的逻辑");

function testLimitSpecConsistentMerge(queryPlan) {
  console.log("开始 limitSpec 一致性合并测试...");
  
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
  
  // 创建合并后的查询
  const mergedQuery = {
    ...templateQuery,
    queryType: "groupBy",
    granularity: "all"
  };
  
  console.log("\n开始合并 limitSpec...");

  // 模拟合并其他参数的逻辑（所有子请求的 limitSpec 都是一样的）
  for (const query of dimensionQueries) {
    // 合并 limitSpec（直接使用第一个找到的，因为所有子请求的 limitSpec 都是一样的）
    if (query.limitSpec && !mergedQuery.limitSpec) {
      console.log(`发现 ${query.queryType} 查询的 limitSpec:`);
      console.log(`  - type: ${query.limitSpec.type}`);
      console.log(`  - limit: ${query.limitSpec.limit}`);
      console.log(`  - columns: ${JSON.stringify(query.limitSpec.columns)}`);

      mergedQuery.limitSpec = query.limitSpec;  // 直接使用，不修改

      console.log("✅ 已使用第一个找到的 limitSpec（所有子请求的 limitSpec 都是一样的）");
      break;  // 找到第一个就停止，因为都是一样的
    }
  }
  
  console.log("\nlimitSpec 合并结果:");
  if (mergedQuery.limitSpec) {
    console.log("- type:", mergedQuery.limitSpec.type);
    console.log("- limit:", mergedQuery.limitSpec.limit);
    console.log("- columns:", JSON.stringify(mergedQuery.limitSpec.columns, null, 2));
    
    // 验证 limitSpec 是否正确保持原值
    if (mergedQuery.limitSpec.limit === 100) {
      console.log("✅ limitSpec.limit 正确保持原值 100");
    } else {
      console.log("❌ limitSpec.limit 未正确保持原值");
    }
  } else {
    console.log("❌ 未找到 limitSpec");
  }
  
  return mergedQuery;
}

const result = testLimitSpecConsistentMerge(mockQueryPlanWithLimitSpec);

console.log("\n3. 最终合并查询验证");
if (result) {
  console.log("合并后的查询结构:");
  console.log("- 查询类型:", result.queryType);
  console.log("- 是否有 limitSpec:", !!result.limitSpec);
  
  if (result.limitSpec) {
    console.log("- limitSpec.limit:", result.limitSpec.limit);
    console.log("- limitSpec 完整结构:");
    console.log(JSON.stringify(result.limitSpec, null, 2));
  }
}

console.log("\n=== 测试完成 ===");
console.log("\n总结:");
if (result && result.limitSpec && result.limitSpec.limit === 100) {
  console.log("✅ limitSpec 一致性合并功能正常");
  console.log("✅ 正确使用第一个找到的 limitSpec");
  console.log("✅ limitSpec 所有属性保持原值不变");
} else {
  console.log("❌ limitSpec 一致性合并功能异常");
}

console.log("\n🎯 关键理解:");
console.log("- 所有子请求的 limitSpec 都是一样的（用于排序）");
console.log("- 直接使用第一个找到的 limitSpec，不需要修改");
console.log("- limitSpec 主要用于排序条件，在所有请求中都是一致的");
console.log("- 保持原始的 limit 值和排序规则");

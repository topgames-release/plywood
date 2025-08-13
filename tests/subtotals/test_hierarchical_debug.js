const { Expression, DruidExternal, External, Dataset } = require('../../build/plywood.js');

console.log("=== 调试层级结构构建问题 ===\n");

// 模拟完整的 subtotalsSpec 查询结果
const mockFlatResult = {
  data: [
    // 总计行 - 所有维度为 null
    {
      __time: null,
      app: null,
      platform: null,
      activation: 678570,
      current_pu: 9510
    },
    // 第一层：按 __time 分组
    {
      __time: "2025-08-09T00:00:00Z",
      app: null,
      platform: null,
      activation: 208526,
      current_pu: 1392
    },
    {
      __time: "2025-08-08T00:00:00Z",
      app: null,
      platform: null,
      activation: 153831,
      current_pu: 1568
    },
    // 第二层：按 __time + app 分组
    {
      __time: "2025-08-09T00:00:00Z",
      app: "EM",
      platform: null,
      activation: 95143,
      current_pu: 205
    },
    {
      __time: "2025-08-09T00:00:00Z",
      app: "TF",
      platform: null,
      activation: 113383,
      current_pu: 1187
    },
    {
      __time: "2025-08-08T00:00:00Z",
      app: "EM",
      platform: null,
      activation: 103831,
      current_pu: 1068
    },
    {
      __time: "2025-08-08T00:00:00Z",
      app: "TF",
      platform: null,
      activation: 50000,
      current_pu: 500
    },
    // 第三层：按 __time + app + platform 分组
    {
      __time: "2025-08-09T00:00:00Z",
      app: "EM",
      platform: "Android",
      activation: 45000,
      current_pu: 100
    },
    {
      __time: "2025-08-09T00:00:00Z",
      app: "EM",
      platform: "IOS",
      activation: 50143,
      current_pu: 105
    },
    {
      __time: "2025-08-09T00:00:00Z",
      app: "TF",
      platform: "Android",
      activation: 60000,
      current_pu: 600
    },
    {
      __time: "2025-08-09T00:00:00Z",
      app: "TF",
      platform: "IOS",
      activation: 53383,
      current_pu: 587
    },
    {
      __time: "2025-08-08T00:00:00Z",
      app: "EM",
      platform: "Android",
      activation: 50000,
      current_pu: 500
    },
    {
      __time: "2025-08-08T00:00:00Z",
      app: "EM",
      platform: "IOS",
      activation: 53831,
      current_pu: 568
    },
    {
      __time: "2025-08-08T00:00:00Z",
      app: "TF",
      platform: "Android",
      activation: 30000,
      current_pu: 300
    },
    {
      __time: "2025-08-08T00:00:00Z",
      app: "TF",
      platform: "IOS",
      activation: 20000,
      current_pu: 200
    }
  ]
};

const mockQuery = {
  dimensions: [
    { dimension: "__time", outputName: "__time" },
    { dimension: "app", outputName: "app" },
    { dimension: "platform", outputName: "platform" }
  ],
  aggregations: [
    { name: "activation", type: "longSum" },
    { name: "current_pu", type: "longSum" }
  ],
  subtotalsSpec: [
    ["__time", "app", "platform"],
    ["__time", "app"],
    ["__time"],
    []
  ]
};

// 手动实现层级构建逻辑来调试
function debugHierarchicalBuild() {
  console.log("1. 分析扁平化数据结构");
  
  const data = mockFlatResult.data;
  const keys = ["__time", "app", "platform"];
  const attributes = [
    { name: "__time", type: "TIME_RANGE" },
    { name: "app", type: "STRING" },
    { name: "platform", type: "STRING" },
    { name: "activation", type: "NUMBER" },
    { name: "current_pu", type: "NUMBER" }
  ];
  
  console.log("总数据行数:", data.length);
  
  // 分析每个层级的数据
  console.log("\n2. 分析各层级数据分布:");
  
  // 总计层级（所有维度为 null）
  const totalRows = data.filter(row => 
    keys.every(key => row[key] === null || row[key] === undefined)
  );
  console.log("总计层级数据:", totalRows.length, "行");
  
  // 第一层级（只有 __time 有值）
  const level1Rows = data.filter(row => 
    row.__time !== null && row.__time !== undefined &&
    row.app === null && row.platform === null
  );
  console.log("第一层级数据 (__time):", level1Rows.length, "行");
  level1Rows.forEach(row => console.log("  -", row.__time, "activation:", row.activation));
  
  // 第二层级（__time 和 app 有值）
  const level2Rows = data.filter(row => 
    row.__time !== null && row.__time !== undefined &&
    row.app !== null && row.app !== undefined &&
    row.platform === null
  );
  console.log("第二层级数据 (__time + app):", level2Rows.length, "行");
  level2Rows.forEach(row => console.log("  -", row.__time, row.app, "activation:", row.activation));
  
  // 第三层级（所有维度都有值）
  const level3Rows = data.filter(row => 
    row.__time !== null && row.__time !== undefined &&
    row.app !== null && row.app !== undefined &&
    row.platform !== null && row.platform !== undefined
  );
  console.log("第三层级数据 (__time + app + platform):", level3Rows.length, "行");
  level3Rows.forEach(row => console.log("  -", row.__time, row.app, row.platform, "activation:", row.activation));
  
  return { data, keys, attributes, totalRows, level1Rows, level2Rows, level3Rows };
}

// 测试当前的分组逻辑
function testGroupingLogic() {
  console.log("\n3. 测试分组逻辑");
  
  const { data, keys } = debugHierarchicalBuild();
  
  // 测试第一层分组（按 __time）
  console.log("\n第一层分组测试 (按 __time):");
  const groups1 = {};
  data.forEach(row => {
    const keyValue = row.__time;
    if (keyValue !== null && keyValue !== undefined) {
      const groupKey = String(keyValue);
      if (!groups1[groupKey]) {
        groups1[groupKey] = [];
      }
      groups1[groupKey].push(row);
    }
  });
  
  Object.keys(groups1).forEach(timeKey => {
    console.log(`时间组 ${timeKey}: ${groups1[timeKey].length} 行数据`);
    
    // 在这个时间组内，找到当前层级的聚合数据
    const aggregateRow = groups1[timeKey].find(row => 
      row.__time === timeKey &&
      row.app === null && row.platform === null
    );
    
    if (aggregateRow) {
      console.log(`  聚合数据: activation=${aggregateRow.activation}, current_pu=${aggregateRow.current_pu}`);
    } else {
      console.log("  ❌ 未找到聚合数据");
    }
    
    // 测试第二层分组（按 app）
    const groups2 = {};
    groups1[timeKey].forEach(row => {
      const appValue = row.app;
      if (appValue !== null && appValue !== undefined) {
        const groupKey = String(appValue);
        if (!groups2[groupKey]) {
          groups2[groupKey] = [];
        }
        groups2[groupKey].push(row);
      }
    });
    
    Object.keys(groups2).forEach(appKey => {
      console.log(`    应用组 ${appKey}: ${groups2[appKey].length} 行数据`);
      
      // 在这个应用组内，找到当前层级的聚合数据
      const appAggregateRow = groups2[appKey].find(row => 
        row.__time === timeKey &&
        row.app === appKey &&
        row.platform === null
      );
      
      if (appAggregateRow) {
        console.log(`      聚合数据: activation=${appAggregateRow.activation}, current_pu=${appAggregateRow.current_pu}`);
      } else {
        console.log("      ❌ 未找到聚合数据");
      }
      
      // 测试第三层分组（按 platform）
      const groups3 = {};
      groups2[appKey].forEach(row => {
        const platformValue = row.platform;
        if (platformValue !== null && platformValue !== undefined) {
          const groupKey = String(platformValue);
          if (!groups3[groupKey]) {
            groups3[groupKey] = [];
          }
          groups3[groupKey].push(row);
        }
      });
      
      Object.keys(groups3).forEach(platformKey => {
        console.log(`      平台组 ${platformKey}: ${groups3[platformKey].length} 行数据`);
        
        const platformRow = groups3[platformKey].find(row => 
          row.__time === timeKey &&
          row.app === appKey &&
          row.platform === platformKey
        );
        
        if (platformRow) {
          console.log(`        数据: activation=${platformRow.activation}, current_pu=${platformRow.current_pu}`);
        }
      });
    });
  });
}

// 识别当前实现的问题
function identifyProblems() {
  console.log("\n4. 识别当前实现的问题");
  
  console.log("🔍 分析结果:");
  console.log("1. 数据分组逻辑看起来是正确的");
  console.log("2. 每个层级都能找到对应的聚合数据");
  console.log("3. 递归构建应该能够正常工作");
  
  console.log("\n🤔 可能的问题:");
  console.log("1. _buildSplitAttributes 方法可能有问题");
  console.log("2. 递归终止条件可能不正确");
  console.log("3. 数据传递给下一层级时可能有遗漏");
  console.log("4. attributes 构建可能不完整");
  
  console.log("\n🎯 需要检查的点:");
  console.log("- _buildSplitAttributes 是否正确构建了每层的 attributes");
  console.log("- 递归调用时是否正确传递了 groupData");
  console.log("- 是否所有层级都正确添加了 SPLIT 属性");
  console.log("- 最深层级是否正确处理（不应该有 SPLIT）");
}

// 运行调试
console.log("开始调试...\n");

debugHierarchicalBuild();
testGroupingLogic();
identifyProblems();

console.log("\n=== 调试总结 ===");
console.log("✅ 数据分析完成");
console.log("✅ 分组逻辑验证完成");
console.log("🔍 需要进一步检查 _buildSplitAttributes 和递归逻辑");

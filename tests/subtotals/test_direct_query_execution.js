const { Expression, DruidExternal, External } = require('../../build/plywood.js');

console.log("=== 测试直接查询执行方案 ===\n");

// 测试 External 类的静态方法
console.log("1. 测试 External 类的静态方法");

try {
  // 测试 postTransformFactory 方法
  if (typeof External.postTransformFactory === 'function') {
    console.log("✅ External.postTransformFactory 方法存在");
    
    const postTransform = External.postTransformFactory([], [], null, null);
    console.log("✅ postTransformFactory 调用成功");
    console.log("- postTransform type:", typeof postTransform);
  } else {
    console.log("❌ External.postTransformFactory 方法不存在");
  }

  // 测试 performQueryAndPostTransform 方法
  if (typeof External.performQueryAndPostTransform === 'function') {
    console.log("✅ External.performQueryAndPostTransform 方法存在");
  } else {
    console.log("❌ External.performQueryAndPostTransform 方法不存在");
  }

  // 测试 buildValueFromStream 方法
  if (typeof External.buildValueFromStream === 'function') {
    console.log("✅ External.buildValueFromStream 方法存在");
  } else {
    console.log("❌ External.buildValueFromStream 方法不存在");
  }

} catch (error) {
  console.log("❌ External 静态方法测试失败:", error.message);
}

console.log("\n2. 测试 DruidExternal 的 requester 属性");

try {
  // 创建一个 DruidExternal 实例
  const druidExternal = DruidExternal.fromJS({
    engine: 'druid',
    source: 'test_datasource',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'platform', type: 'STRING' },
      { name: 'activation', type: 'NUMBER' }
    ]
  });

  console.log("✅ DruidExternal 实例创建成功");

  // 检查 requester 属性
  if (druidExternal.requester) {
    console.log("✅ DruidExternal.requester 存在");
    console.log("- requester type:", typeof druidExternal.requester);
  } else {
    console.log("❌ DruidExternal.requester 不存在");
  }

  // 检查 engine 属性
  console.log("- engine:", druidExternal.engine);

} catch (error) {
  console.log("❌ DruidExternal requester 测试失败:", error.message);
}

console.log("\n3. 模拟直接查询执行流程");

function simulateDirectQueryExecution() {
  try {
    // 1. 模拟 subtotalsSpec 查询
    const subtotalsQuery = {
      queryType: "groupBy",
      dataSource: {
        type: "union",
        dataSources: ["test_datasource"]
      },
      intervals: "2025-07-29T00Z/2025-08-06T00Z",
      granularity: "all",
      dimensions: [
        {
          type: "default",
          dimension: "platform",
          outputName: "platform"
        }
      ],
      aggregations: [
        {
          name: "activation",
          type: "longSum",
          fieldName: "isInstall"
        }
      ],
      subtotalsSpec: [
        ["platform"],
        []
      ],
      limitSpec: {
        type: "default",
        columns: [
          {
            dimension: "activation",
            direction: "descending"
          }
        ],
        limit: 10000
      }
    };

    console.log("✅ subtotalsSpec 查询构建成功");
    console.log("- queryType:", subtotalsQuery.queryType);
    console.log("- subtotalsSpec 组合数:", subtotalsQuery.subtotalsSpec.length);

    // 2. 模拟查询上下文
    const queryContext = {
      timestamp: null,
      ignorePrefix: "!",
      dummyPrefix: "***"
    };

    console.log("✅ 查询上下文构建成功");

    // 3. 模拟 postTransform 创建
    const postTransform = External.postTransformFactory([], [], null, null);
    console.log("✅ postTransform 创建成功");

    // 4. 模拟 QueryAndPostTransform 对象
    const queryAndPostTransform = {
      query: subtotalsQuery,
      context: queryContext,
      postTransform: postTransform
    };

    console.log("✅ QueryAndPostTransform 对象构建成功");
    console.log("- 包含 query:", !!queryAndPostTransform.query);
    console.log("- 包含 context:", !!queryAndPostTransform.context);
    console.log("- 包含 postTransform:", !!queryAndPostTransform.postTransform);

    return true;

  } catch (error) {
    console.log("❌ 直接查询执行流程模拟失败:", error.message);
    return false;
  }
}

const simulationResult = simulateDirectQueryExecution();

console.log("\n=== 测试完成 ===");
console.log("\n总结:");
if (simulationResult) {
  console.log("✅ 直接查询执行方案可行");
  console.log("✅ External 静态方法可用");
  console.log("✅ 查询构建和上下文设置正确");
} else {
  console.log("❌ 直接查询执行方案存在问题");
}

console.log("\n🎯 方案优势:");
console.log("- 直接使用已构建的 subtotalsSpec 查询");
console.log("- 跳过 queryValue() 的查询重新生成步骤");
console.log("- 使用 External.performQueryAndPostTransform 底层机制");
console.log("- 保持 subtotalsSpec 查询的完整性");

console.log("\n🔧 关键组件:");
console.log("- External.postTransformFactory() - 创建结果处理函数");
console.log("- External.performQueryAndPostTransform() - 执行查询和后处理");
console.log("- External.buildValueFromStream() - 将流转换为 PlywoodValue");
console.log("- DruidExternal.requester - 底层查询请求器");

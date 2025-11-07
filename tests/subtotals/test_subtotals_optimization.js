const { Expression, DruidExternal, SplitExpression } = require('../../build/plywood.js');

console.log("=== 测试 subtotalsSpec 优化功能 ===\n");

// 测试 1: 验证 SplitExpression.isMultiSplit() 方法
console.log("1. 测试 SplitExpression.isMultiSplit() 方法");

// 创建单个 split
const singleSplit = Expression._.split({
  platform: '$platform'
}, 'data');

console.log("单个 split isMultiSplit():", singleSplit.isMultiSplit()); // 应该是 false

// 创建多个 split
const multiSplit = Expression._.split({
  platform: '$platform',
  network_name: '$network_name',
  multi_region: '$multi_region',
  lookup_campaign_main_type: '$lookup_campaign_main_type',
  lookup_creative_type: '$lookup_creative_type'
}, 'data');

console.log("多个 split isMultiSplit():", multiSplit.isMultiSplit()); // 应该是 true
console.log("多个 split 维度数量:", multiSplit.numSplits()); // 应该是 5

// 测试 2: 验证 DruidExternal.generateSubtotalsSpec() 方法
console.log("\n2. 测试 DruidExternal.generateSubtotalsSpec() 方法");

// 创建一个 DruidExternal 实例
const druidExternal = new DruidExternal({
  engine: 'druid',
  source: 'ads_data',
  timeAttribute: '__time',
  attributes: [
    { name: '__time', type: 'TIME' },
    { name: 'platform', type: 'STRING' },
    { name: 'network_name', type: 'STRING' },
    { name: 'multi_region', type: 'STRING' },
    { name: 'lookup_campaign_main_type', type: 'STRING' },
    { name: 'lookup_creative_type', type: 'STRING' },
    { name: 'activation', type: 'NUMBER' },
    { name: 'current_pu', type: 'NUMBER' }
  ]
});

// 测试单个 split（应该返回空数组）
const singleSubtotalsSpec = druidExternal.generateSubtotalsSpec(singleSplit);
console.log("单个 split 的 subtotalsSpec:", JSON.stringify(singleSubtotalsSpec, null, 2));

// 测试多个 split
const multiSubtotalsSpec = druidExternal.generateSubtotalsSpec(multiSplit);
console.log("多个 split 的 subtotalsSpec:", JSON.stringify(multiSubtotalsSpec, null, 2));

// 验证 subtotalsSpec 的正确性
console.log("\n3. 验证 subtotalsSpec 的正确性");
console.log("期望的 subtotalsSpec 应该包含以下组合:");
console.log("- 完整组合: [platform, network_name, multi_region, lookup_campaign_main_type, lookup_creative_type]");
console.log("- 4维组合: [platform, network_name, multi_region, lookup_campaign_main_type]");
console.log("- 3维组合: [platform, network_name, multi_region]");
console.log("- 2维组合: [platform, network_name]");
console.log("- 1维组合: [platform]");
console.log("- 总计: []");

const expectedLength = 6; // 5个递减组合 + 1个空数组
console.log(`实际生成的 subtotalsSpec 长度: ${multiSubtotalsSpec.length}, 期望长度: ${expectedLength}`);

if (multiSubtotalsSpec.length === expectedLength) {
  console.log("✅ subtotalsSpec 长度正确");
} else {
  console.log("❌ subtotalsSpec 长度不正确");
}

// 验证最后一个元素是空数组（总计）
const lastElement = multiSubtotalsSpec[multiSubtotalsSpec.length - 1];
if (Array.isArray(lastElement) && lastElement.length === 0) {
  console.log("✅ 最后一个元素是空数组（总计）");
} else {
  console.log("❌ 最后一个元素不是空数组");
}

// 验证第一个元素包含所有维度
const firstElement = multiSubtotalsSpec[0];
if (Array.isArray(firstElement) && firstElement.length === 5) {
  console.log("✅ 第一个元素包含所有5个维度");
} else {
  console.log("❌ 第一个元素不包含所有维度");
}

console.log("\n=== 测试完成 ===");

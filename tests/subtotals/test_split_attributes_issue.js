const { Expression, DruidExternal, External, Dataset } = require('../../build/plywood.js');

console.log("=== 测试 _buildSplitAttributes 问题 ===\n");

// 模拟 _buildSplitAttributes 的当前实现
function currentBuildSplitAttributes(allAttributes, splitKey) {
  const splitAttributes = [];

  // 添加当前分割键的属性
  const keyAttribute = allAttributes.find(attr => attr.name === splitKey);
  if (keyAttribute) {
    splitAttributes.push(keyAttribute);
  }

  // 添加聚合属性
  allAttributes.forEach(attr => {
    if (attr.type === "NUMBER") {
      splitAttributes.push(attr);
    }
  });

  // 问题：总是添加 SPLIT 属性，即使在最深层级
  splitAttributes.push({
    name: "SPLIT",
    type: "DATASET"
  });

  return splitAttributes;
}

// 修复后的 _buildSplitAttributes 实现
function fixedBuildSplitAttributes(allAttributes, splitKey, keys, currentLevel) {
  const splitAttributes = [];

  // 添加当前分割键的属性
  const keyAttribute = allAttributes.find(attr => attr.name === splitKey);
  if (keyAttribute) {
    splitAttributes.push(keyAttribute);
  }

  // 添加聚合属性
  allAttributes.forEach(attr => {
    if (attr.type === "NUMBER") {
      splitAttributes.push(attr);
    }
  });

  // 只有在不是最深层级时才添加 SPLIT 属性
  if (currentLevel + 1 < keys.length) {
    splitAttributes.push({
      name: "SPLIT",
      type: "DATASET"
    });
  }

  return splitAttributes;
}

// 测试当前实现的问题
function testCurrentImplementation() {
  console.log("1. 测试当前实现的问题");
  
  const allAttributes = [
    { name: "__time", type: "TIME_RANGE" },
    { name: "app", type: "STRING" },
    { name: "platform", type: "STRING" },
    { name: "activation", type: "NUMBER" },
    { name: "current_pu", type: "NUMBER" }
  ];
  
  const keys = ["__time", "app", "platform"];
  
  console.log("维度键:", keys);
  console.log("总层级数:", keys.length);
  
  keys.forEach((key, level) => {
    const attrs = currentBuildSplitAttributes(allAttributes, key);
    console.log(`\n第${level + 1}层 (${key}):`);
    console.log("- attributes 数量:", attrs.length);
    console.log("- 包含 SPLIT:", attrs.some(attr => attr.name === "SPLIT"));
    console.log("- attributes:", attrs.map(attr => `${attr.name}:${attr.type}`));
  });
  
  console.log("\n❌ 问题：最深层级 (platform) 也包含 SPLIT 属性");
  console.log("这会导致期望有更深层级的嵌套，但实际上没有数据");
}

// 测试修复后的实现
function testFixedImplementation() {
  console.log("\n2. 测试修复后的实现");
  
  const allAttributes = [
    { name: "__time", type: "TIME_RANGE" },
    { name: "app", type: "STRING" },
    { name: "platform", type: "STRING" },
    { name: "activation", type: "NUMBER" },
    { name: "current_pu", type: "NUMBER" }
  ];
  
  const keys = ["__time", "app", "platform"];
  
  keys.forEach((key, level) => {
    const attrs = fixedBuildSplitAttributes(allAttributes, key, keys, level);
    console.log(`\n第${level + 1}层 (${key}):`);
    console.log("- attributes 数量:", attrs.length);
    console.log("- 包含 SPLIT:", attrs.some(attr => attr.name === "SPLIT"));
    console.log("- attributes:", attrs.map(attr => `${attr.name}:${attr.type}`));
    
    if (level === keys.length - 1) {
      console.log("✅ 最深层级正确：不包含 SPLIT 属性");
    }
  });
}

// 对比期望的结构
function compareExpectedStructure() {
  console.log("\n3. 对比期望的结构");
  
  console.log("期望的层级结构:");
  console.log("顶层:");
  console.log("  - attributes: [activation:NUMBER, current_pu:NUMBER, SPLIT:DATASET]");
  console.log("  - data: [{ activation, current_pu, SPLIT: {...} }]");
  
  console.log("\n第1层 (__time SPLIT):");
  console.log("  - keys: ['__time']");
  console.log("  - attributes: [__time:TIME_RANGE, activation:NUMBER, current_pu:NUMBER, SPLIT:DATASET]");
  console.log("  - data: [{ __time, activation, current_pu, SPLIT: {...} }, ...]");
  
  console.log("\n第2层 (app SPLIT):");
  console.log("  - keys: ['app']");
  console.log("  - attributes: [app:STRING, activation:NUMBER, current_pu:NUMBER, SPLIT:DATASET]");
  console.log("  - data: [{ app, activation, current_pu, SPLIT: {...} }, ...]");
  
  console.log("\n第3层 (platform SPLIT - 最深层级):");
  console.log("  - keys: ['platform']");
  console.log("  - attributes: [platform:STRING, activation:NUMBER, current_pu:NUMBER]");
  console.log("  - data: [{ platform, activation, current_pu }, ...] (无 SPLIT)");
  
  console.log("\n✅ 关键点：最深层级不应该有 SPLIT 属性");
}

// 验证修复的必要性
function verifyFixNecessity() {
  console.log("\n4. 验证修复的必要性");
  
  console.log("🔍 当前问题的影响:");
  console.log("1. 最深层级包含空的 SPLIT 属性");
  console.log("2. 可能导致前端解析时期望更深层级的数据");
  console.log("3. 与 dataset.js 中的正确结构不一致");
  console.log("4. 可能导致递归处理时的无限循环或错误");
  
  console.log("\n🛠️ 修复方案:");
  console.log("1. 修改 _buildSplitAttributes 方法签名");
  console.log("2. 传入 keys 数组和当前层级信息");
  console.log("3. 只在非最深层级时添加 SPLIT 属性");
  console.log("4. 确保最深层级只包含维度和聚合属性");
  
  console.log("\n🎯 修复后的效果:");
  console.log("- 最深层级不包含 SPLIT 属性");
  console.log("- 层级结构更加清晰和正确");
  console.log("- 与普通查询结果完全一致");
  console.log("- 避免前端解析时的混淆");
}

// 运行所有测试
console.log("开始测试...\n");

testCurrentImplementation();
testFixedImplementation();
compareExpectedStructure();
verifyFixNecessity();

console.log("\n=== 测试总结 ===");
console.log("🔍 问题确认：_buildSplitAttributes 在最深层级也添加了 SPLIT 属性");
console.log("✅ 修复方案：根据层级信息决定是否添加 SPLIT 属性");
console.log("🎯 下一步：实施修复并测试完整的层级构建");

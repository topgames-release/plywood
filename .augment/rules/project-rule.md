---
type: "manual"
---

# Plywood 项目开发规则

## 一、通用代码原则

### 1.1 代码简洁性和可读性
- **优先保证代码简洁易懂**：代码首先是给人读的，其次才是给机器执行的
- **避免过度设计**：简单实用就好，不要为了设计模式而设计模式
- **圈复杂度控制**：
  - 函数尽量小，单一职责
  - 避免深层嵌套（最多 3 层）
  - 复杂逻辑应该拆分成多个小函数
  - 使用提前返回（early return）减少嵌套

### 1.2 代码复用和重复代码
- **尽量复用代码**：相同逻辑应该提取成公共函数或工具类
- **避免重复代码**：DRY 原则（Don't Repeat Yourself）
- **建立工具函数库**：在 `src/helper/` 目录中维护通用工具函数

### 1.3 模块设计
- **注意模块设计**：尽量使用设计模式，但不过度
- **单一职责原则**：每个模块只负责一个功能
- **高内聚低耦合**：模块间依赖关系清晰，易于测试和维护

### 1.4 沟通和文档
- **解释代码时说人话**：避免过度使用专业术语，用通俗易懂的语言解释
- **提供可视化图表**：使用 Mermaid 图表辅助说明复杂逻辑
- **实现时给出原理和步骤**：不仅说怎么做，还要说为什么这样做

---

## 二、TypeScript 编码规范

### 2.1 类型安全
- **启用严格类型检查**：项目配置 `noImplicitAny: true`，禁止使用隐式 `any` 类型
- **避免使用 `any` 类型**：
  - ❌ 不好：`function process(data: any): any { ... }`
  - ✅ 好：`function process<T extends Record<string, unknown>>(data: T): ProcessResult<T> { ... }`
- **显式类型注解**：函数参数和返回值必须有类型注解

### 2.2 接口和类型定义
- **使用接口定义数据结构**：
  ```typescript
  interface ComputeOptions {
    customOptions?: Record<string, unknown>;
    useSubtotalsSpec?: boolean;
    timeout?: number;
  }
  ```
- **类型别名用于联合类型**：`type PlywoodValue = Dataset | Set | Range | number | string | boolean | null;`
- **避免过度使用 `any`**：使用泛型或联合类型替代

### 2.3 泛型使用
- **合理使用泛型提高代码复用性**：
  ```typescript
  function transformDataset<T extends Datum>(
    dataset: Dataset,
    transformer: (datum: Datum) => T
  ): T[] {
    return dataset.data.map(transformer);
  }
  ```
- **泛型约束**：使用 `extends` 关键字限制泛型范围
- **避免过度泛型化**：不要为了泛型而泛型，保持代码可读性

### 2.4 null 和 undefined 处理
- **显式处理 null/undefined**：
  ```typescript
  if (value !== null && value !== undefined) {
    processValue(value);
  }
  ```
- **使用可选链和空值合并**：`const result = data?.property ?? defaultValue;`
- **避免隐式 null 检查**：不要依赖 JavaScript 的真值判断

---

## 三、代码组织规范

### 3.1 文件命名约定
- **文件名使用 PascalCase（对应类名）或 camelCase（对应函数）**：
  - ✅ `baseExpression.ts`（导出 `BaseExpression` 类）
  - ✅ `promiseWhile.ts`（导出 `promiseWhile` 函数）
  - ❌ `base-expression.ts`（避免使用 kebab-case）
- **测试文件命名**：`*.mocha.js` 或 `*.test.ts`
- **一个文件一个主要导出**：避免一个文件导出多个不相关的类/函数

### 3.2 目录结构
- **按功能模块组织**：
  ```
  src/
  ├── expressions/        # 表达式系统
  ├── datatypes/         # 数据类型定义
  ├── external/          # 外部数据源适配器
  ├── dialect/           # 查询方言
  ├── executor/          # 查询执行器
  ├── helper/            # 工具函数
  └── index.ts           # 主入口
  ```
- **相关文件放在同一目录**：表达式相关的文件都在 `expressions/` 目录
- **避免循环依赖**：检查模块间的依赖关系

### 3.3 导入导出规范
- **使用 ES6 模块语法**：
  ```typescript
  import { Dataset, Datum } from "../datatypes/index";
  import { BaseExpression } from "./baseExpression";
  ```
- **避免循环导入**：使用依赖注入或重新组织模块结构
- **导出清晰的公共 API**：在 `index.ts` 中集中导出公共接口
- **内部模块使用相对路径**，外部依赖使用绝对路径

### 3.4 类和接口组织
- **类成员顺序**：静态属性 → 实例属性 → 构造函数 → 公共方法 → 受保护方法 → 私有方法
- **使用访问修饰符**：明确标记 `public`、`protected`、`private`
- **避免过大的类**：如果类超过 300 行，考虑拆分

---

## 四、测试规范

### 4.1 测试覆盖率要求
- **单元测试覆盖率目标**：≥ 80%
- **关键路径覆盖率**：≥ 95%
- **边界条件必须测试**：null、undefined、空数组、极限值等
- **异常情况必须测试**：错误处理、异常抛出等

### 4.2 测试文件命名和位置
- **测试文件位置**：与源文件对应的 `test/` 目录下
  - 源文件：`src/expressions/baseExpression.ts`
  - 测试文件：`test/expression/baseExpression.mocha.js`
- **测试文件命名**：`*.mocha.js` 或 `*.test.ts`
- **一个源文件对应一个测试文件**：避免测试文件过大

### 4.3 测试编写规范
- **使用 Mocha + Chai 框架**：
  ```javascript
  const { expect } = require("chai");
  describe("BaseExpression", () => {
    it("should compute simple expression", () => {
      const expression = new LiteralExpression({ value: 42 });
      expect(expression.compute()).to.equal(42);
    });
  });
  ```
- **测试用例结构**：Arrange-Act-Assert（AAA）模式
- **Mock 数据使用规范**：使用工厂函数创建 Mock 数据，避免硬编码

### 4.4 测试执行
- **编译前运行测试**：`npm run pretest` 自动编译
- **运行特定测试**：`mocha test/expression/baseExpression.mocha.js`
- **运行所有测试**：`npm test` 或 `npm run full-test`
- **检查测试覆盖率**：`./run-coverage`

---

## 五、构建和编译规范

### 5.1 编译流程
- **完整编译**：`npm run compile` 或 `./compile`
  - 编译 TypeScript：`./compile-tsc`
  - 编译 PEG.js 语法解析器：`./compile-pegjs`
- **编译前检查**：确保没有 TypeScript 错误、TSLint 检查通过、所有测试通过

### 5.2 编译配置
- **TypeScript 配置** (`tsconfig.json`)：
  - `noImplicitAny: true`：禁止隐式 any
  - `noEmitOnError: true`：有错误时不生成输出
  - `noImplicitReturns: true`：函数必须有返回值
  - `noFallthroughCasesInSwitch: true`：switch 必须有 break
- **TSLint 配置** (`tslint.json`)：遵循推荐规则，最大行长度 200 字符，缩进 2 个空格

### 5.3 构建最佳实践
- **增量编译**：修改后只编译必要的文件
- **清理构建产物**：定期清理 `build/` 目录
- **版本管理**：更新 `src/version.ts` 中的版本号
- **生成类型定义**：确保 `build/index.d.ts` 正确生成

---

## 六、错误处理规范

### 6.1 异常捕获
- **显式捕获异常**：
  ```typescript
  try {
    const result = await executeQuery(expression);
    return result;
  } catch (error) {
    logger.error("Query execution failed", { error });
    throw error;
  }
  ```
- **避免空 catch 块**：必须处理异常或重新抛出
- **使用自定义错误类**：继承 `Error` 类，提供清晰的错误信息

### 6.2 错误日志
- **记录错误上下文**：包括输入参数、执行状态等
- **使用结构化日志**：便于日志分析和调试
- **避免记录敏感信息**：不要记录密码、token 等敏感数据
- **日志级别**：error（严重）、warn（警告）、info（重要）、debug（调试）

### 6.3 边界条件处理
- **检查输入参数**：验证参数有效性
- **处理 null/undefined**：显式检查，不要依赖隐式转换
- **处理空集合**：数组、Set、Map 等集合类型的空值情况
- **处理极限值**：最大值、最小值、溢出等

---

## 七、性能优化规范

### 7.1 查询优化
- **使用 subtotalsSpec 优化**：
  - 在 `compute()` 调用中启用 `customOptions.useSubtotalsSpec: true`
  - 将 N+1 次查询减少到 1 次
  - 性能提升 60-90%
  - 详见 `docs/features/SUBTOTALS_OPTIMIZATION.md`
- **避免不必要的查询**：缓存查询结果、合并多个查询、使用查询计划优化

### 7.2 内存管理
- **避免内存泄漏**：及时释放大对象引用、使用流式处理大数据集
- **使用不可变数据结构**：项目使用 `immutable-class` 库，避免直接修改对象
- **流式处理**：使用 `PassThrough` 流处理大数据集

### 7.3 异步操作最佳实践
- **使用 async/await**：代码更易读，错误处理更清晰
- **避免 Promise 地狱**：使用 async/await 或 Promise 链
- **正确处理 Promise 拒绝**：使用 try-catch 或 .catch()
- **使用流式处理大数据集**：避免一次性加载所有数据

---

## 八、版本控制规范

### 8.1 提交信息格式
- **使用清晰的提交信息**：
  ```
  feat: add subtotalsSpec optimization for Druid queries
  
  - Implement subtotalsSpec in DruidExternal
  - Reduce N+1 queries to single query
  - Performance improvement: 60-90% faster
  ```
- **提交类型前缀**：feat（新功能）、fix（bug 修复）、refactor（重构）、test（测试）、docs（文档）、perf（性能）、chore（构建）

### 8.2 分支管理策略
- **主分支保护**：`main` 分支应该始终可部署
- **功能分支命名**：`feature/description` 或 `fix/description`
- **分支生命周期**：从 main 创建 → 开发测试 → 创建 PR → 代码审查 → 合并 → 删除分支

---

## 九、文档规范

### 9.1 代码注释要求
- **类和公共方法必须有 JSDoc 注释**：包括参数、返回值、异常、示例
- **复杂逻辑需要注释**：解释"为什么"而不是"是什么"
- **避免过度注释**：代码应该自解释，注释补充说明

### 9.2 API 文档维护
- **更新 `docs/` 目录**：expressions、datatypes、design-overview 等
- **新功能必须更新文档**：功能说明、使用示例、性能影响、已知限制
- **保持文档与代码同步**：代码变更时更新对应文档

### 9.3 README 和项目文档
- **维护 `README.md`**：项目概览、快速开始、主要特性
- **维护 `CHANGELOG.md`**：版本历史和变更记录
- **维护 `PROJECT_STRUCTURE.md`**：项目结构说明
- **维护 `CLAUDE.md`**：AI 助手开发指南

---

## 十、代码审查和改动规范

### 10.1 改动前的准备
- **改动或解释前，最好看看所有代码**：不能偷懒
  - 理解相关模块的整体设计
  - 检查是否有类似的实现
  - 评估改动的影响范围
- **查找所有相关的代码**：使用 IDE 的"查找引用"功能、检查测试文件、检查文档

### 10.2 最小化修改原则
- **改动前，要做最小化修改**：尽量不修改到其他模块的代码
- **单一职责改动**：一次改动只做一件事
- **避免不必要的重构**：除非是改动的必要部分
- **保持向后兼容**：如果可能，避免破坏现有 API

### 10.3 改动后的验证
- **改动后，假定 10 条 case 输入，并给出预期结果**：
  1. 空数据集 → 返回空结果
  2. 单条记录 → 正确处理
  3. 大数据集（10000+ 条） → 性能可接受
  4. null 值 → 正确处理
  5. 特殊字符 → 正确转义
  6. 超时场景 → 抛出 TimeoutError
  7. 并发请求 → 正确隔离
  8. 内存压力 → 不泄漏
  9. 无效参数 → 抛出 ValidationError
  10. 边界值 → 正确处理
- **运行完整测试套件**：`npm test` 或 `npm run full-test`
- **检查代码覆盖率**：`./run-coverage`

---

## 十一、可视化和图表规范

### 11.1 Mermaid 图表要求
- **给出的 Mermaid 图，必须自检语法，可以被渲染**：
  - 使用在线 Mermaid 编辑器验证
  - 确保所有语法正确
  - 测试在暗黑主题下的显示效果
- **给出的 Mermaid 图，必须要可以被暗黑主题渲染清晰**：
  - 使用高对比度的颜色
  - 避免使用浅色背景
  - 测试在 VS Code 暗黑主题下的显示

### 11.2 图表类型选择
- **流程图**：表示执行流程、决策树
- **序列图**：表示组件间的交互
- **类图**：表示类的继承和关系

---

## 十二、项目特定规范

### 12.1 表达式系统
- **所有表达式继承 `BaseExpression`**：实现 `compute()` 和 `toJS()` 方法
- **表达式应该是不可变的**：使用 `immutable-class` 库
- **表达式应该支持链式调用**：返回新的表达式对象

### 12.2 数据类型
- **支持的数据类型**：Dataset、Set、Range（TimeRange、NumberRange、StringRange）、基本类型
- **类型转换**：显式转换，避免隐式转换
- **类型检查**：使用 `instanceof` 或类型守卫

### 12.3 外部数据源
- **支持的数据源**：Druid（主要）、MySQL、PostgreSQL
- **实现新数据源**：继承 `BaseExternal`、实现 `getQueryAndPostProcess()` 方法、提供对应的 SQL 方言

### 12.4 查询优化
- **subtotalsSpec 优化**：仅适用于 Druid 数据源，需要启用 `customOptions.useSubtotalsSpec: true`
- **自动检测可优化的查询模式**：详见 `src/external/druidExternal.ts`

---

## 十三、开发工作流

### 13.1 开发步骤
1. 创建功能分支：`git checkout -b feature/description`
2. 编写代码：遵循本规范
3. 编写测试：确保覆盖率 ≥ 80%
4. 编译和检查：`npm run compile`
5. 运行测试：`npm test`
6. 代码审查：提交 Pull Request
7. 合并到主分支：`git merge feature/description`

### 13.2 常用命令
```bash
npm run compile          # 完整编译
./compile-tsc           # 仅编译 TypeScript
./compile-pegjs         # 仅编译 PEG.js
npm test                # 运行标准测试
npm run full-test       # 运行完整测试
./run-coverage          # 检查覆盖率
mocha test/expression/* # 运行特定测试
```

---

## 十四、参考资源

- **TypeScript 官方文档**：https://www.typescriptlang.org/docs/
- **Mocha 测试框架**：https://mochajs.org/
- **Chai 断言库**：https://www.chaijs.com/
- **Mermaid 图表**：https://mermaid.js.org/
- **项目文档**：`docs/` 目录
- **开发指南**：`CLAUDE.md` 文件

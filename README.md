# Plywood

Plywood is a JavaScript library that simplifies building interactive
visualizations and applications for large data sets. Plywood acts as a
middle-layer between data visualizations and data stores.

Plywood is architected around the principles of nested
[Split-Apply-Combine](http://www.jstatsoft.org/article/view/v040i01/v40i01.pdf),
a powerful divide-and-conquer algorithm that can be used to construct all types
of data visualizations. Plywood comes with its own [expression
language](docs/expressions.md) where a single Plywood expression can
translate to multiple database queries, and where results are returned in a
nested data structure so they can be easily consumed by visualization libraries
such as [D3.js](http://d3js.org/). 

You can use Plywood in the browser and/or in node.js to easily create your own
visualizations and applications.

Plywood also acts as a very advanced query planner for Druid, and Plywood will
determine the most optimal way to execute Druid queries.

## Installation

To use Plywood from npm simply run: `npm install plywood`.

Plywood can also be used by the browser.

## Documentation

To learn more, see [http://plywood.imply.io](http://plywood.imply.io/)

### 项目文档结构

- **[docs/](./docs/)** - 项目文档目录
  - **[docs/features/](./docs/features/)** - 功能特性文档
    - [subtotalsSpec 优化详细文档](./docs/features/SUBTOTALS_OPTIMIZATION.md)
    - [subtotalsSpec 优化使用指南](./docs/features/README_SUBTOTALS.md)
  - **[docs/examples/](./docs/examples/)** - 示例代码
- **[tests/](./tests/)** - 测试文件目录
  - **[tests/subtotals/](./tests/subtotals/)** - subtotalsSpec 优化测试

### 新功能特性

#### subtotalsSpec 查询优化
利用 Apache Druid 的 subtotalsSpec 特性，将多次查询合并为一次查询，大幅提升多维度分析的查询性能。

**快速开始**:
```javascript
const result = await expression.compute(context, {
  customOptions: {
    useSubtotalsSpec: true    // 启用 subtotalsSpec 优化
  }
});
```

**性能提升**: 60-90% 的查询时间减少

详细信息请参考：[subtotalsSpec 优化文档](./docs/features/README_SUBTOTALS.md)

## Also see

* [Pivot](http://pivot.imply.io) - a data exploration GUI built using Plywood.
* [PlyQL](https://github.com/implydata/plyql) - a SQL-like interface to plywood
* Vadim Ogievetsky talks about the [inspiration behind Plywood](https://www.youtube.com/watch?v=JNMbLxqzGFA)

## Development

To run all the Plywood unit tests you will need to set up the data sources in the [DataZoo](https://github.com/implydata/datazoo).  

## Questions & Support

For updates about new and upcoming features follow [@implydata](https://twitter.com/implydata) on Twitter.

Please file bugs and feature requests by opening and issue on GitHub and direct all questions to our [user groups](https://groups.google.com/forum/#!forum/imply-user-group).

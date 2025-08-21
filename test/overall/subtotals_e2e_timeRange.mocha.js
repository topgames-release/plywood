const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');
const { $, ply, External } = plywood;

// 端到端模拟：通过 useSubtotalsSpec 路径执行 groupBy + subtotalsSpec
// 使用 mock requester 返回扁平化数据，其中 __time 为字符串；
// 断言最终结果中 __time 被转换为 TimeRange 对象（包含 start/end）。

describe('E2E subtotalsSpec __time -> TimeRange', function() {
  it('should convert __time string to TimeRange via subtotalsSpec pipeline', async function() {
    // 1) 构造 mock requester，返回 objectMode 流，逐行输出 Druid groupBy + subtotalsSpec 扁平化结构
    const requester = ({ query }) => {
      const stream = new PassThrough({ objectMode: true });
      setTimeout(() => {
        // 逐条写入每一行（postTransformFactory 会将每个对象视为一条 datum）
        const rows = [
          // 总计行（所有维度为空）
          { '***__time': null, '***app': null, activation: 22 },
          // 时间分组（字符串 __time）
          { '***__time': '2025-08-03T00:00:00Z', '***app': null, activation: 10 },
          { '***__time': '2025-08-04T00:00:00Z', '***app': null, activation: 12 },
          // 时间 + app 分组
          { '***__time': '2025-08-03T00:00:00Z', '***app': 'gameA', activation: 10 },
          { '***__time': '2025-08-04T00:00:00Z', '***app': 'gameA', activation: 12 }
        ];
        for (const r of rows) stream.write(r);
        stream.end();
      }, 1);
      return stream;
    };

    // 2) 构造 DruidExternal 上下文
    const context = {
      facts: External.fromJS({
        engine: 'druid',
        source: 'facts',
        timeAttribute: 'time',
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'app', type: 'STRING' },
          { name: 'activation', type: 'NUMBER', unsplitable: true }
        ],
        allowSelectQueries: true
      }, requester)
    };

    // 3) 构造表达式：增加一个 totals（产生 timeseries 查询）+ 按天 timeBucket + app split
    const ex = ply()
      .apply('facts', $('facts')
        .filter($('time').overlap(new Date('2025-08-03T00:00:00Z'), new Date('2025-08-05T00:00:00Z')))
      )
      .apply('totalActivation', $('facts').sum('$activation'))
      .apply('result', $('facts')
        .split({ '__time': $('time').timeBucket('P1D', 'Etc/UTC'), 'app': '$app' })
        .apply('activation', $('facts').sum('$activation'))
      );

    // 4) compute 时启用 useSubtotalsSpec，从而走我们在 baseExpression.ts 的合并路径
    const ds = await ex.compute(context, { customOptions: { useSubtotalsSpec: true } });

    // 5) 断言：层级结果中的 __time 为 TimeRange 对象，且起止为期望的 1 天区间
    const js = ds.toJS();
    // 顶层数据即为合并后的结果（__time 作为第一层维度），SPLIT 位于顶层 data[0].SPLIT
    const splitData = js.data[0].SPLIT.data;

    expect(splitData).to.be.an('array').that.is.not.empty;

    // 找到任何一个含有 __time 的条目并验证其为 TimeRange 结构
    const item = splitData.find(r => r.__time && r.__time.start && r.__time.end);
    expect(item).to.be.ok;

    const startMs = Date.parse(item.__time.start);
    const endMs = Date.parse(item.__time.end);
    expect(Number.isNaN(startMs)).to.equal(false);
    expect(Number.isNaN(endMs)).to.equal(false);
    expect(endMs - startMs).to.equal(24 * 60 * 60 * 1000);

    // 确认 app 维度也存在于另一层（至少一个项包含 app）
    const hasApp = splitData.some(r => r.SPLIT && Array.isArray(r.SPLIT.data) && r.SPLIT.data.some(x => x.app === 'gameA'));
    expect(hasApp).to.equal(true);
  });
});


var chai = require('chai');
var expect = chai.expect;
var plywood = require('../plywood');
var Dataset = plywood.Dataset;
var TimeRange = plywood.TimeRange;

// 这个用例针对 subtotalsSpec 合并路径：
// 保证 __time 字段返回为 TimeRange 对象而不是字符串

describe('subtotalsSpec __time TimeRange', function() {
  it('should return __time as TimeRange object', function() {
    // 手工构造层级化后的 JS（模拟 _buildHierarchicalDataset 的输出）：
    const hierarchicalJS = {
      attributes: [
        { name: 'activation', type: 'NUMBER' },
        { name: 'SPLIT', type: 'DATASET' }
      ],
      keys: [],
      data: [
        {
          activation: 22,
          SPLIT: {
            keys: ['__time'],
            attributes: [
              { name: '__time', type: 'TIME_RANGE' },
              { name: 'activation', type: 'NUMBER' }
            ],
            data: [
              {
                __time: { start: '2025-08-03T00:00:00.000Z', end: '2025-08-04T00:00:00.000Z' },
                activation: 10
              },
              {
                __time: { start: '2025-08-04T00:00:00.000Z', end: '2025-08-05T00:00:00.000Z' },
                activation: 12
              }
            ]
          }
        }
      ]
    };

    const ds = Dataset.fromJS(hierarchicalJS);

    // 直接拿原生对象断言（不走 toJS）
    const native = ds.data[0].SPLIT.data[0].__time;
    expect(native instanceof TimeRange).to.equal(true);

    // 同时检查 toJS 后的结构含有 start / end 字段
    const js = ds.toJS();
    const time1 = js.data[0].SPLIT.data[0].__time;
    const time2 = js.data[0].SPLIT.data[1].__time;
    expect(time1).to.have.property('start');
    expect(time1).to.have.property('end');
    expect(time2).to.have.property('start');
    expect(time2).to.have.property('end');
  });
});


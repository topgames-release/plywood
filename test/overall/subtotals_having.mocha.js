const { expect } = require('chai');
const {
  $,
  ply,
  SubtotalsSpecHelper,
  buildMergedQuery,
} = require('../plywood');

describe('subtotals having extraction', () => {
  it('extracts having filter with inclusive upper bound', () => {
    const expression = ply()
      .apply('main', $('main'))
      .apply(
        'SPLIT',
        $('main')
          .split({ app: '$app' }, 'DATA')
          .apply(
            'current_pu',
            $('main')
              .filter($('status').is('ACTIVE'))
              .count()
          )
          .filter($('current_pu').lessThanOrEqual(0))
      );

    const having =
      SubtotalsSpecHelper.extractHavingFiltersFromExpression(expression);
    expect(having).to.deep.equal({
      type: 'lessThan',
      aggregation: 'current_pu',
      value: 1,
    });
  });

  it('supports logical combinations and builder integration', () => {
    const expression = ply()
      .apply('main', $('main'))
      .apply(
        'SPLIT',
        $('main')
          .split({ app: '$app' }, 'DATA')
          .apply(
            'current_pu',
            $('main')
              .filter($('status').is('ACTIVE'))
              .count()
          )
          .apply('rev_total', $('main').sum('$revenue'))
          .filter(
            $('current_pu')
              .greaterThanOrEqual(10)
              .and($('rev_total').lessThan(5))
          )
      );

    const having =
      SubtotalsSpecHelper.extractHavingFiltersFromExpression(expression);

    expect(having).to.deep.equal({
      type: 'and',
      havingSpecs: [
        { type: 'greaterThan', aggregation: 'current_pu', value: 9 },
        { type: 'lessThan', aggregation: 'rev_total', value: 5 },
      ],
    });

    const templateQuery = {
      queryType: 'timeseries',
      dataSource: 'ds',
      intervals: '2025-01-01/2025-01-02',
      granularity: 'all',
      aggregations: [
        { type: 'count', name: 'current_pu' },
        { type: 'doubleSum', name: 'rev_total', fieldName: 'rev_total' },
      ],
    };

    const dimensionQuery = {
      queryType: 'groupBy',
      dataSource: 'ds',
      intervals: '2025-01-01/2025-01-02',
      granularity: 'all',
      dimensions: [
        { type: 'default', dimension: 'app', outputName: 'app' },
      ],
      aggregations: [
        { type: 'count', name: 'current_pu' },
        { type: 'doubleSum', name: 'rev_total', fieldName: 'rev_total' },
      ],
    };

    const mergedQuery = buildMergedQuery(
      null,
      [[templateQuery], [dimensionQuery]],
      500,
      having
    );

    expect(mergedQuery.having).to.deep.equal(having);
  });
});

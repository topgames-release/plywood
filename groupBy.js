var groupBy = {
  queryType: 'groupBy',
  dataSource: {
    type: 'union',
    dataSources: ['ads_newdata_common_data', 'rev_std_means_beta3', 'ads_newdata_ad_cost_lite'],
  },
  intervals: '2025-06-01T00Z/2025-06-22T00Z',
  granularity: 'all',
  context: {
    timeout: 600000,
    useCache: true,
    queryId: '5085fe12-c598-47ff-a18b-e38f9232439e',
  },
  filter: {
    type: 'and',
    fields: [
      {
        type: 'selector',
        dimension: 'app',
        value: 'RG',
        extractionFn: {
          type: 'registeredLookup',
          lookup: 'app_convert',
          retainMissingValue: true,
        },
      },
      {
        type: 'or',
        fields: [
          {
            type: 'expression',
            expression: '(nvl(lookup("raw_platform",\'platform_group\'),"platform")==\'IOS\')',
          },
          {
            type: 'expression',
            expression: '(nvl(lookup("raw_platform",\'platform_group\'),"platform")==\'Android\')',
          },
        ],
      },
      {
        type: 'not',
        field: {
          type: 'in',
          dimension: 'multi_region',
          values: ['US', 'JP'],
        },
      },
      {
        type: 'in',
        dimension: 'campaign_main_type',
        values: ['VO', 'Other', 'AEO', 'MAI'],
        extractionFn: {
          type: 'lookup',
          retainMissingValue: true,
          lookup: {
            type: 'map',
            map: {
              '': 'Other',
            },
          },
        },
      },
    ],
  },
  virtualColumns: [
    {
      type: 'expression',
      name: 'v:platform',
      expression: 'nvl(lookup("raw_platform",\'platform_group\'),"platform")',
      outputType: 'STRING',
    },
  ],
  dimensions: [
    {
      type: 'default',
      dimension: 'v:platform',
      outputName: 'platform',
      outputType: 'STRING',
    },
  ],
  aggregations: [
    {
      type: 'filtered',
      name: '!T_0',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'AD',
          },
        ],
      },
      aggregator: {
        name: '!T_0',
        type: 'doubleSum',
        fieldName: 'ad_revenue',
      },
    },
    {
      type: 'filtered',
      name: '!T_1',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'INSTALL',
          },
          {
            type: 'not',
            field: {
              type: 'in',
              dimension: 'app',
              values: ['LT', 'TF'],
            },
          },
          {
            type: 'or',
            fields: [
              {
                type: 'not',
                field: {
                  type: 'selector',
                  dimension: 'network_name',
                  value: 'Organic',
                },
              },
              {
                type: 'selector',
                dimension: 'raw_platform_text',
                value: 'IOS',
              },
              {
                type: 'not',
                field: {
                  type: 'selector',
                  dimension: 'app',
                  value: 'EM',
                },
              },
            ],
          },
        ],
      },
      aggregator: {
        name: '!T_1',
        type: 'doubleSum',
        fieldName: 'pay7m',
      },
    },
    {
      type: 'filtered',
      name: '!T_2',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'ELTV_5_6',
          },
        ],
      },
      aggregator: {
        name: '!T_2',
        type: 'doubleSum',
        fieldName: 'total_eltv',
      },
    },
    {
      type: 'filtered',
      name: '!T_3',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'AD',
          },
        ],
      },
      aggregator: {
        name: '!T_3',
        type: 'doubleSum',
        fieldName: 'spend',
      },
    },
    {
      type: 'filtered',
      name: '!T_4',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'INSTALL',
          },
        ],
      },
      aggregator: {
        name: '!T_4',
        type: 'longSum',
        fieldName: 'isInstall',
      },
    },
    {
      type: 'filtered',
      name: '!T_5',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'PAY',
          },
          {
            type: 'bound',
            dimension: 'pay_days',
            ordering: 'numeric',
            upper: 0,
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_5',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_6',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'CASH_PAY',
          },
          {
            type: 'bound',
            dimension: 'pay_days',
            ordering: 'numeric',
            upper: 0,
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_6',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_7',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'PAY',
          },
          {
            type: 'bound',
            dimension: 'pay_weeks',
            ordering: 'numeric',
            upper: 0,
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_7',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_8',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'CASH_PAY',
          },
          {
            type: 'bound',
            dimension: 'pay_weeks',
            ordering: 'numeric',
            upper: 0,
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_8',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_9',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'PAY',
          },
          {
            type: 'bound',
            dimension: 'pay_weeks',
            ordering: 'numeric',
            lower: -1,
            lowerStrict: true,
            upper: 0,
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_9',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_10',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'CASH_PAY',
          },
          {
            type: 'bound',
            dimension: 'pay_weeks',
            ordering: 'numeric',
            lower: -1,
            lowerStrict: true,
            upper: 0,
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_10',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_11',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'PAY',
          },
          {
            type: 'bound',
            dimension: 'pay_days',
            ordering: 'numeric',
            lower: -1,
            lowerStrict: true,
            upper: 0,
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_11',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_12',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'CASH_PAY',
          },
          {
            type: 'bound',
            dimension: 'pay_days',
            ordering: 'numeric',
            lower: -1,
            lowerStrict: true,
            upper: 0,
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_12',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_13',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'PAY',
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_13',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_14',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'CASH_PAY',
          },
          {
            type: 'selector',
            dimension: 'order_state',
            value: 1,
          },
        ],
      },
      aggregator: {
        name: '!T_14',
        type: 'doubleSum',
        fieldName: 'pay_money',
      },
    },
    {
      type: 'filtered',
      name: '!T_15',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'INSTALL',
          },
        ],
      },
      aggregator: {
        type: 'javascript',
        fieldNames: ['dpu_daily'],
        fnAggregate: 'function(current, added) { return current+(((added&1)>0)?1:0) }',
        fnCombine: 'function(partialA, partialB) { return partialA + partialB }',
        fnReset: 'function() { return 0; }',
        name: '!T_15',
      },
    },
    {
      type: 'filtered',
      name: '!T_16',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'INSTALL',
          },
        ],
      },
      aggregator: {
        type: 'javascript',
        fieldNames: ['dpu_weekly'],
        fnAggregate: 'function(current, added) { return current+(((added&1)>0)?1:0) }',
        fnCombine: 'function(partialA, partialB) { return partialA + partialB }',
        fnReset: 'function() { return 0; }',
        name: '!T_16',
      },
    },
    {
      type: 'filtered',
      name: '!T_17',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'INSTALL',
          },
        ],
      },
      aggregator: {
        type: 'javascript',
        fieldNames: ['dpu_daily'],
        fnAggregate: 'function(current, added) { return current+(((added&1)>0)?1:0) }',
        fnCombine: 'function(partialA, partialB) { return partialA + partialB }',
        fnReset: 'function() { return 0; }',
        name: '!T_17',
      },
    },
    {
      type: 'filtered',
      name: '!T_18',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'INSTALL',
          },
        ],
      },
      aggregator: {
        type: 'javascript',
        fieldNames: ['dpu_weekly'],
        fnAggregate: 'function(current, added) { return current+(((added&1)>0)?1:0) }',
        fnCombine: 'function(partialA, partialB) { return partialA + partialB }',
        fnReset: 'function() { return 0; }',
        name: '!T_18',
      },
    },
    {
      type: 'filtered',
      name: '!T_19',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'INSTALL',
          },
        ],
      },
      aggregator: {
        type: 'javascript',
        fieldNames: ['drr_daily'],
        fnAggregate: 'function(current, added) { return current+(((added&32)>0)?1:0) }',
        fnCombine: 'function(partialA, partialB) { return partialA + partialB }',
        fnReset: 'function() { return 0; }',
        name: '!T_19',
      },
    },
    {
      type: 'filtered',
      name: '!T_20',
      filter: {
        type: 'and',
        fields: [
          {
            type: 'interval',
            dimension: '__time',
            intervals: ['2025-06-01T00Z/2025-06-22T00Z'],
          },
          {
            type: 'selector',
            dimension: 'action_type',
            value: 'INSTALL',
          },
        ],
      },
      aggregator: {
        name: '!T_20',
        type: 'count',
      },
    },
  ],
  postAggregations: [
    {
      type: 'expression',
      expression: '("!T_0"*0.7)',
      name: 'ad_revenue',
    },
    {
      type: 'expression',
      expression:
        'nvl(if((("!T_1"+"!T_2")-"!T_3")!=0,(if("!T_3"!=0,(cast((("!T_1"+"!T_2")-"!T_3"),\'DOUBLE\')/"!T_3"),9007199254740991)),0),0)',
      name: 'elte_spend',
    },
    {
      type: 'expression',
      expression: '((cast("!T_3",\'DOUBLE\')/1814400000)*86400000)',
      name: 'spend_d',
    },
    {
      type: 'expression',
      expression: '((cast("!T_4",\'DOUBLE\')/1814400000)*86400000)',
      name: 'activation_d',
    },
    {
      type: 'expression',
      expression:
        'if("!T_3"!=0,(if("!T_4"!=0,(cast("!T_3",\'DOUBLE\')/"!T_4"),9007199254740991)),0)',
      name: 'cpi_total',
    },
    {
      type: 'expression',
      expression:
        'nvl(if(("!T_1"+"!T_2")!=0,(if("!T_3"!=0,(cast(("!T_1"+"!T_2"),\'DOUBLE\')/"!T_3"),9007199254740991)),0),0)',
      name: 'eltv_spend',
    },
    {
      type: 'expression',
      expression:
        'nvl(if(("!T_0"*0.7)!=0,(if("!T_3"!=0,(cast(("!T_0"*0.7),\'DOUBLE\')/"!T_3"),9007199254740991)),0),0)',
      name: 'ad_revenue_roas',
    },
    {
      type: 'expression',
      expression:
        'nvl(if((("!T_5"*0.7)+"!T_6")!=0,(if("!T_3"!=0,(cast((("!T_5"*0.7)+"!T_6"),\'DOUBLE\')/"!T_3"),9007199254740991)),0),0)',
      name: '0d_roas',
    },
    {
      type: 'expression',
      expression:
        'nvl(if((("!T_7"*0.7)+"!T_8")!=0,(if("!T_3"!=0,(cast((("!T_7"*0.7)+"!T_8"),\'DOUBLE\')/"!T_3"),9007199254740991)),0),0)',
      name: '0w_roas',
    },
    {
      type: 'expression',
      expression:
        'if((("!T_9"*0.7)+"!T_10")!=0,(if((("!T_11"*0.7)+"!T_12")!=0,(cast((("!T_9"*0.7)+"!T_10"),\'DOUBLE\')/(("!T_11"*0.7)+"!T_12")),9007199254740991)),0)',
      name: '0w_0d_rev_cohort',
    },
    {
      type: 'expression',
      expression:
        'nvl(if((("!T_9"*0.7)+"!T_10")!=0,(if("!T_4"!=0,(cast((("!T_9"*0.7)+"!T_10"),\'DOUBLE\')/"!T_4"),9007199254740991)),0),0)',
      name: '0w_rev_cohort_install',
    },
    {
      type: 'expression',
      expression:
        'nvl(if((("!T_13"*0.7)+"!T_14")!=0,(if("!T_3"!=0,(cast((("!T_13"*0.7)+"!T_14"),\'DOUBLE\')/"!T_3"),9007199254740991)),0),0)',
      name: 'roas_to_date',
    },
    {
      type: 'expression',
      expression:
        'if("!T_3"!=0,(if("!T_15"!=0,(cast("!T_3",\'DOUBLE\')/"!T_15"),9007199254740991)),0)',
      name: '0d_cpp',
    },
    {
      type: 'expression',
      expression:
        'nvl(if("!T_3"!=0,(if("!T_16"!=0,(cast("!T_3",\'DOUBLE\')/"!T_16"),9007199254740991)),0),0)',
      name: '0w_cpp',
    },
    {
      type: 'expression',
      expression:
        'nvl(if("!T_17"!=0,(if("!T_4"!=0,(cast("!T_17",\'DOUBLE\')/"!T_4"),9007199254740991)),0),0)',
      name: '0d_pay_rate',
    },
    {
      type: 'expression',
      expression:
        'nvl(if("!T_18"!=0,(if("!T_4"!=0,(cast("!T_18",\'DOUBLE\')/"!T_4"),9007199254740991)),0),0)',
      name: '0w_pay_rate',
    },
    {
      type: 'expression',
      expression:
        'nvl(if("!T_19"!=0,(if("!T_20"!=0,(cast("!T_19",\'DOUBLE\')/"!T_20"),9007199254740991)),0),0)',
      name: '6d_rr',
    },
    {
      type: 'expression',
      expression: '(("!T_13"*0.7)+"!T_14")',
      name: 'rev_total',
    },
  ],
  limitSpec: {
    type: 'default',
    columns: [
      {
        dimension: 'spend_d',
        direction: 'descending',
      },
    ],
    limit: 100000,
  },
  source: 'TopBI',
  subSource: 'newdata2',
};

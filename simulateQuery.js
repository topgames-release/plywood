var query = [
  [
    {
      queryType: "timeseries",
      dataSource: {
        type: "union",
        dataSources: [
          "ads_newdata_common_data",
        ],
      },
      intervals: "2025-07-28T00Z/2025-08-05T00Z",
      granularity: "all",
      context: {
        timeout: 600000,
        useCache: true,
      },
      filter: {
        type: "selector",
        dimension: "action_type",
        value: "INSTALL",
      },
      aggregations: [
        {
          name: "activation",
          type: "longSum",
          fieldName: "isInstall",
        },
        {
          type: "javascript",
          fieldNames: [
            "dpu_monthly",
          ],
          fnAggregate: "function(current, added) { return current+((added>0)?1:0) }",
          fnCombine: "function(partialA, partialB) { return partialA + partialB }",
          fnReset: "function() { return 0; }",
          name: "current_pu",
        },
      ],
    },
    {
      queryType: "topN",
      dataSource: {
        type: "union",
        dataSources: [
          "ads_newdata_common_data",
        ],
      },
      intervals: "2025-07-28T00Z/2025-08-05T00Z",
      granularity: "all",
      context: {
        timeout: 600000,
        useCache: true,
      },
      dimension: {
        type: "extraction",
        dimension: "__time",
        outputName: "***__time",
        extractionFn: {
          type: "timeFormat",
          granularity: {
            type: "period",
            period: "P1D",
            timeZone: "Etc/UTC",
          },
          format: "yyyy-MM-dd'T'HH:mm:ss'Z",
          timeZone: "Etc/UTC",
        },
      },
      aggregations: [
        {
          type: "filtered",
          name: "activation",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: [
                  "2025-07-28T00Z/2025-08-05T00Z",
                ],
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL",
              },
            ],
          },
          aggregator: {
            name: "activation",
            type: "longSum",
            fieldName: "isInstall",
          },
        },
        {
          type: "filtered",
          name: "current_pu",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: [
                  "2025-07-28T00Z/2025-08-05T00Z",
                ],
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL",
              },
            ],
          },
          aggregator: {
            type: "javascript",
            fieldNames: [
              "dpu_monthly",
            ],
            fnAggregate: "function(current, added) { return current+((added>0)?1:0) }",
            fnCombine: "function(partialA, partialB) { return partialA + partialB }",
            fnReset: "function() { return 0; }",
            name: "current_pu",
          },
        },
      ],
      postAggregations: [
        {
          type: "expression",
          expression: "86400000",
          name: "MillisecondsInInterval",
        },
        {
          type: "expression",
          expression: "86400000",
          name: "activation_MillisecondsInInterval",
        },
        {
          type: "expression",
          expression: "86400000",
          name: "current_pu_MillisecondsInInterval",
        },
      ],
      metric: {
        type: "dimension",
        ordering: "lexicographic",
      },
      threshold: 100,
    },
  ],
  [
    {
      queryType: "groupBy",
      dataSource: {
        type: "union",
        dataSources: [
          "ads_newdata_common_data",
        ],
      },
      intervals: [
      ],
      granularity: "all",
      context: {
        timeout: 600000,
        useCache: true,
      },
      dimensions: [
        {
          type: "extraction",
          dimension: "app",
          outputName: "app",
          extractionFn: {
            type: "registeredLookup",
            lookup: "app_convert",
            retainMissingValue: true,
          },
        },
      ],
      aggregations: [
        {
          type: "filtered",
          name: "activation",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: [
                  "2025-07-28T00Z/2025-08-05T00Z",
                ],
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL",
              },
            ],
          },
          aggregator: {
            name: "activation",
            type: "longSum",
            fieldName: "isInstall",
          },
        },
        {
          type: "filtered",
          name: "current_pu",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: [
                  "2025-07-28T00Z/2025-08-05T00Z",
                ],
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL",
              },
            ],
          },
          aggregator: {
            type: "javascript",
            fieldNames: [
              "dpu_monthly",
            ],
            fnAggregate: "function(current, added) { return current+((added>0)?1:0) }",
            fnCombine: "function(partialA, partialB) { return partialA + partialB }",
            fnReset: "function() { return 0; }",
            name: "current_pu",
          },
        },
      ],
      limitSpec: {
        type: "default",
        columns: [
          {
            dimension: "activation",
            direction: "descending",
          },
        ],
        limit: 100,
      },
    },
  ],
  [
    {
      queryType: "groupBy",
      dataSource: {
        type: "union",
        dataSources: [
          "ads_newdata_common_data",
        ],
      },
      intervals: [
      ],
      granularity: "all",
      context: {
        timeout: 600000,
        useCache: true,
      },
      virtualColumns: [
        {
          type: "expression",
          name: "v:platform",
          expression: "nvl(lookup(\"raw_platform\",'platform_group'),\"platform\")",
          outputType: "STRING",
        },
      ],
      dimensions: [
        {
          type: "default",
          dimension: "v:platform",
          outputName: "platform",
          outputType: "STRING",
        },
      ],
      aggregations: [
        {
          type: "filtered",
          name: "activation",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: [
                  "2025-07-28T00Z/2025-08-05T00Z",
                ],
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL",
              },
            ],
          },
          aggregator: {
            name: "activation",
            type: "longSum",
            fieldName: "isInstall",
          },
        },
        {
          type: "filtered",
          name: "current_pu",
          filter: {
            type: "and",
            fields: [
              {
                type: "interval",
                dimension: "__time",
                intervals: [
                  "2025-07-28T00Z/2025-08-05T00Z",
                ],
              },
              {
                type: "selector",
                dimension: "action_type",
                value: "INSTALL",
              },
            ],
          },
          aggregator: {
            type: "javascript",
            fieldNames: [
              "dpu_monthly",
            ],
            fnAggregate: "function(current, added) { return current+((added>0)?1:0) }",
            fnCombine: "function(partialA, partialB) { return partialA + partialB }",
            fnReset: "function() { return 0; }",
            name: "current_pu",
          },
        },
      ],
      limitSpec: {
        type: "default",
        columns: [
          {
            dimension: "activation",
            direction: "descending",
          },
        ],
        limit: 100,
      },
    },
  ],
]
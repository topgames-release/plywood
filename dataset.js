var dataset = {
  "attributes": [
      {
          "name": "isDST",
          "type": "BOOLEAN"
      },
      {
          "name": "main",
          "type": "DATASET"
      },
      {
          "name": "NOW",
          "type": "NUMBER"
      },
      {
          "name": "MillisecondsInInterval",
          "type": "NUMBER"
      },
      {
          "name": "start_filter_time",
          "type": "NUMBER"
      },
      {
          "name": "end_filter_time",
          "type": "NUMBER"
      },
      {
          "name": "activation_MillisecondsInInterval",
          "type": "NUMBER"
      },
      {
          "name": "current_pu_MillisecondsInInterval",
          "type": "NUMBER"
      },
      {
          "name": "activation",
          "type": "NUMBER"
      },
      {
          "name": "current_pu",
          "type": "NUMBER"
      },
      {
          "name": "SPLIT",
          "type": "DATASET"
      }
  ],
  "data": [
      {
          "isDST": true,
          "NOW": 1754896793093,
          "MillisecondsInInterval": 691200000,
          "start_filter_time": 1754265600000,
          "end_filter_time": 1754956800000,
          "activation_MillisecondsInInterval": 691200000,
          "current_pu_MillisecondsInInterval": 691200000,
          "activation": 677223,
          "current_pu": 9446,
          "SPLIT": {
              "keys": [
                  "__time"
              ],
              "attributes": [
                  {
                      "name": "__time",
                      "type": "TIME_RANGE"
                  },
                  {
                      "name": "activation",
                      "type": "NUMBER"
                  },
                  {
                      "name": "current_pu",
                      "type": "NUMBER"
                  },
                  {
                      "name": "MillisecondsInInterval",
                      "type": "NUMBER"
                  },
                  {
                      "name": "activation_MillisecondsInInterval",
                      "type": "NUMBER"
                  },
                  {
                      "name": "current_pu_MillisecondsInInterval",
                      "type": "NUMBER"
                  },
                  {
                      "name": "SPLIT",
                      "type": "DATASET"
                  }
              ],
              "data": [
                  {
                      "current_pu": 1809,
                      "activation": 94785,
                      "MillisecondsInInterval": 86400000,
                      "activation_MillisecondsInInterval": 86400000,
                      "current_pu_MillisecondsInInterval": 86400000,
                      "__time": {
                          "start": "2025-08-04T00:00:00.000Z",
                          "end": "2025-08-05T00:00:00.000Z"
                      },
                      "SPLIT": {
                          "keys": [
                              "app"
                          ],
                          "attributes": [
                              {
                                  "name": "app",
                                  "type": "STRING"
                              },
                              {
                                  "name": "activation",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "current_pu",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "SPLIT",
                                  "type": "DATASET"
                              }
                          ],
                          "data": [
                              {
                                  "app": "EM",
                                  "activation": 73809,
                                  "current_pu": 209,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 56640,
                                              "current_pu": 114,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 13778,
                                              "current_pu": 83,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 2369,
                                              "current_pu": 8,
                                              "platform": "Windows"
                                          },
                                          {
                                              "activation": 679,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          },
                                          {
                                              "activation": 218,
                                              "current_pu": 1,
                                              "platform": "UWP"
                                          },
                                          {
                                              "activation": 94,
                                              "current_pu": 2,
                                              "platform": "Google Play Games"
                                          },
                                          {
                                              "activation": 31,
                                              "current_pu": 1,
                                              "platform": "MacOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG",
                                  "activation": 13195,
                                  "current_pu": 871,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 7916,
                                              "current_pu": 462,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5279,
                                              "current_pu": 409,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "LT",
                                  "activation": 3482,
                                  "current_pu": 576,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1849,
                                              "current_pu": 485,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 1502,
                                              "current_pu": 90,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 131,
                                              "current_pu": 1,
                                              "platform": "WebGL"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HA",
                                  "activation": 1147,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1062,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 85,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "SOG",
                                  "activation": 1121,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1121,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "TF",
                                  "activation": 953,
                                  "current_pu": 147,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 551,
                                              "current_pu": 69,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 402,
                                              "current_pu": 78,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AC",
                                  "activation": 325,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 296,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 29,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "EP",
                                  "activation": 280,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 267,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 13,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG_CN",
                                  "activation": 125,
                                  "current_pu": 3,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 86,
                                              "current_pu": 3,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 23,
                                              "current_pu": 0,
                                              "platform": "MiniGame"
                                          },
                                          {
                                              "activation": 16,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HC",
                                  "activation": 99,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 81,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 18,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RS",
                                  "activation": 91,
                                  "current_pu": 2,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 91,
                                              "current_pu": 2,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MM",
                                  "activation": 88,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 81,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 7,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MO",
                                  "activation": 21,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 16,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "POZ",
                                  "activation": 20,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 14,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 6,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "IC",
                                  "activation": 18,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 13,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AOG",
                                  "activation": 5,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 3,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 2,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "KL",
                                  "activation": 4,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 4,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "WW",
                                  "activation": 2,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 2,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "CW",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "DJ",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  },
                  {
                      "activation": 92572,
                      "current_pu": 1769,
                      "MillisecondsInInterval": 86400000,
                      "activation_MillisecondsInInterval": 86400000,
                      "current_pu_MillisecondsInInterval": 86400000,
                      "__time": {
                          "start": "2025-08-05T00:00:00.000Z",
                          "end": "2025-08-06T00:00:00.000Z"
                      },
                      "SPLIT": {
                          "keys": [
                              "app"
                          ],
                          "attributes": [
                              {
                                  "name": "app",
                                  "type": "STRING"
                              },
                              {
                                  "name": "activation",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "current_pu",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "SPLIT",
                                  "type": "DATASET"
                              }
                          ],
                          "data": [
                              {
                                  "app": "EM",
                                  "activation": 72269,
                                  "current_pu": 182,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 55748,
                                              "current_pu": 98,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 13054,
                                              "current_pu": 68,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 2466,
                                              "current_pu": 10,
                                              "platform": "Windows"
                                          },
                                          {
                                              "activation": 683,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          },
                                          {
                                              "activation": 227,
                                              "current_pu": 2,
                                              "platform": "UWP"
                                          },
                                          {
                                              "activation": 80,
                                              "current_pu": 3,
                                              "platform": "Google Play Games"
                                          },
                                          {
                                              "activation": 11,
                                              "current_pu": 1,
                                              "platform": "MacOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG",
                                  "activation": 13070,
                                  "current_pu": 863,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 7268,
                                              "current_pu": 477,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5802,
                                              "current_pu": 386,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "LT",
                                  "activation": 3821,
                                  "current_pu": 570,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 2334,
                                              "current_pu": 498,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 1350,
                                              "current_pu": 71,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 137,
                                              "current_pu": 1,
                                              "platform": "WebGL"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HA",
                                  "activation": 1173,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1087,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 86,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "TF",
                                  "activation": 946,
                                  "current_pu": 141,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 574,
                                              "current_pu": 81,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 372,
                                              "current_pu": 60,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "SOG",
                                  "activation": 499,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 499,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "EP",
                                  "activation": 274,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 253,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 21,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG_CN",
                                  "activation": 110,
                                  "current_pu": 6,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 64,
                                              "current_pu": 6,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 29,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 17,
                                              "current_pu": 0,
                                              "platform": "MiniGame"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AC",
                                  "activation": 106,
                                  "current_pu": 2,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 65,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 41,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HC",
                                  "activation": 102,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 83,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 19,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MM",
                                  "activation": 78,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 73,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RS",
                                  "activation": 64,
                                  "current_pu": 3,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 64,
                                              "current_pu": 3,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MO",
                                  "activation": 19,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 14,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "POZ",
                                  "activation": 18,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 12,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 6,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "IC",
                                  "activation": 17,
                                  "current_pu": 2,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 12,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 2,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "KL",
                                  "activation": 3,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 3,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AOG",
                                  "activation": 2,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 2,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "WW",
                                  "activation": 1,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "CW",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "DJ",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  },
                  {
                      "activation": 85057,
                      "current_pu": 1393,
                      "MillisecondsInInterval": 86400000,
                      "activation_MillisecondsInInterval": 86400000,
                      "current_pu_MillisecondsInInterval": 86400000,
                      "__time": {
                          "start": "2025-08-06T00:00:00.000Z",
                          "end": "2025-08-07T00:00:00.000Z"
                      },
                      "SPLIT": {
                          "keys": [
                              "app"
                          ],
                          "attributes": [
                              {
                                  "name": "app",
                                  "type": "STRING"
                              },
                              {
                                  "name": "activation",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "current_pu",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "SPLIT",
                                  "type": "DATASET"
                              }
                          ],
                          "data": [
                              {
                                  "app": "EM",
                                  "activation": 66608,
                                  "current_pu": 170,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 51293,
                                              "current_pu": 96,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 12626,
                                              "current_pu": 64,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 1793,
                                              "current_pu": 8,
                                              "platform": "Windows"
                                          },
                                          {
                                              "activation": 593,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          },
                                          {
                                              "activation": 216,
                                              "current_pu": 2,
                                              "platform": "UWP"
                                          },
                                          {
                                              "activation": 74,
                                              "current_pu": 0,
                                              "platform": "Google Play Games"
                                          },
                                          {
                                              "activation": 13,
                                              "current_pu": 0,
                                              "platform": "MacOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG",
                                  "activation": 12376,
                                  "current_pu": 732,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 6964,
                                              "current_pu": 389,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5412,
                                              "current_pu": 343,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "LT",
                                  "activation": 3076,
                                  "current_pu": 341,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1663,
                                              "current_pu": 86,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 1319,
                                              "current_pu": 252,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 94,
                                              "current_pu": 3,
                                              "platform": "WebGL"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HA",
                                  "activation": 1102,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1016,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 86,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "TF",
                                  "activation": 820,
                                  "current_pu": 144,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 475,
                                              "current_pu": 64,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 345,
                                              "current_pu": 80,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "SOG",
                                  "activation": 319,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 318,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "EP",
                                  "activation": 209,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 195,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 14,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG_CN",
                                  "activation": 120,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 79,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 23,
                                              "current_pu": 0,
                                              "platform": "MiniGame"
                                          },
                                          {
                                              "activation": 18,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AC",
                                  "activation": 109,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 65,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 44,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HC",
                                  "activation": 97,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 82,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 15,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MM",
                                  "activation": 83,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 78,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RS",
                                  "activation": 73,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 73,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "IC",
                                  "activation": 19,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 14,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MO",
                                  "activation": 18,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 16,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 2,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "POZ",
                                  "activation": 17,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 9,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 8,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "KL",
                                  "activation": 6,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 6,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AOG",
                                  "activation": 4,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 2,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 2,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "WW",
                                  "activation": 1,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "DJ",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  },
                  {
                      "activation": 85087,
                      "current_pu": 1235,
                      "MillisecondsInInterval": 86400000,
                      "activation_MillisecondsInInterval": 86400000,
                      "current_pu_MillisecondsInInterval": 86400000,
                      "__time": {
                          "start": "2025-08-07T00:00:00.000Z",
                          "end": "2025-08-08T00:00:00.000Z"
                      },
                      "SPLIT": {
                          "keys": [
                              "app"
                          ],
                          "attributes": [
                              {
                                  "name": "app",
                                  "type": "STRING"
                              },
                              {
                                  "name": "activation",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "current_pu",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "SPLIT",
                                  "type": "DATASET"
                              }
                          ],
                          "data": [
                              {
                                  "app": "EM",
                                  "activation": 67031,
                                  "current_pu": 163,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 51668,
                                              "current_pu": 81,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 12326,
                                              "current_pu": 71,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 2209,
                                              "current_pu": 10,
                                              "platform": "Windows"
                                          },
                                          {
                                              "activation": 529,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          },
                                          {
                                              "activation": 204,
                                              "current_pu": 1,
                                              "platform": "UWP"
                                          },
                                          {
                                              "activation": 79,
                                              "current_pu": 0,
                                              "platform": "Google Play Games"
                                          },
                                          {
                                              "activation": 16,
                                              "current_pu": 0,
                                              "platform": "MacOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG",
                                  "activation": 12433,
                                  "current_pu": 695,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 7224,
                                              "current_pu": 349,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5209,
                                              "current_pu": 346,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "LT",
                                  "activation": 2607,
                                  "current_pu": 253,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1526,
                                              "current_pu": 71,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 946,
                                              "current_pu": 180,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 135,
                                              "current_pu": 2,
                                              "platform": "WebGL"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HA",
                                  "activation": 1277,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1185,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 92,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "TF",
                                  "activation": 757,
                                  "current_pu": 120,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 479,
                                              "current_pu": 68,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 278,
                                              "current_pu": 52,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "SOG",
                                  "activation": 241,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 241,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "EP",
                                  "activation": 239,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 223,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 16,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AC",
                                  "activation": 105,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 71,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 34,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG_CN",
                                  "activation": 100,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 66,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 19,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 15,
                                              "current_pu": 0,
                                              "platform": "MiniGame"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MM",
                                  "activation": 95,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 89,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 6,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HC",
                                  "activation": 90,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 64,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 26,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RS",
                                  "activation": 53,
                                  "current_pu": 2,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 53,
                                              "current_pu": 2,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "IC",
                                  "activation": 24,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 20,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 4,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MO",
                                  "activation": 17,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 12,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "POZ",
                                  "activation": 16,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 10,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 6,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "KL",
                                  "activation": 1,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "WW",
                                  "activation": 1,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AOG",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "DJ",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  },
                  {
                      "current_pu": 1065,
                      "activation": 103831,
                      "MillisecondsInInterval": 86400000,
                      "activation_MillisecondsInInterval": 86400000,
                      "current_pu_MillisecondsInInterval": 86400000,
                      "__time": {
                          "start": "2025-08-08T00:00:00.000Z",
                          "end": "2025-08-09T00:00:00.000Z"
                      },
                      "SPLIT": {
                          "keys": [
                              "app"
                          ],
                          "attributes": [
                              {
                                  "name": "app",
                                  "type": "STRING"
                              },
                              {
                                  "name": "activation",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "current_pu",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "SPLIT",
                                  "type": "DATASET"
                              }
                          ],
                          "data": [
                              {
                                  "app": "EM",
                                  "activation": 86804,
                                  "current_pu": 189,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 70646,
                                              "current_pu": 87,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 12342,
                                              "current_pu": 67,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 2389,
                                              "current_pu": 32,
                                              "platform": "Windows"
                                          },
                                          {
                                              "activation": 729,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          },
                                          {
                                              "activation": 337,
                                              "current_pu": 0,
                                              "platform": "UWP"
                                          },
                                          {
                                              "activation": 305,
                                              "current_pu": 2,
                                              "platform": "Google Play Games"
                                          },
                                          {
                                              "activation": 56,
                                              "current_pu": 1,
                                              "platform": "MacOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG",
                                  "activation": 11990,
                                  "current_pu": 676,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 6247,
                                              "current_pu": 326,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5742,
                                              "current_pu": 350,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "LT",
                                  "activation": 2128,
                                  "current_pu": 129,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1378,
                                              "current_pu": 38,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 637,
                                              "current_pu": 90,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 113,
                                              "current_pu": 1,
                                              "platform": "WebGL"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HA",
                                  "activation": 1416,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1315,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 101,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "TF",
                                  "activation": 483,
                                  "current_pu": 67,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 355,
                                              "current_pu": 51,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 128,
                                              "current_pu": 16,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "EP",
                                  "activation": 282,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 266,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 16,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "SOG",
                                  "activation": 199,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 199,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HC",
                                  "activation": 114,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 91,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 23,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AC",
                                  "activation": 106,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 65,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 41,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MM",
                                  "activation": 100,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 97,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 3,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG_CN",
                                  "activation": 90,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 50,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 24,
                                              "current_pu": 0,
                                              "platform": "MiniGame"
                                          },
                                          {
                                              "activation": 16,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RS",
                                  "activation": 60,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 59,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "IC",
                                  "activation": 20,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 15,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MO",
                                  "activation": 17,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 12,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "POZ",
                                  "activation": 15,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 10,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "KL",
                                  "activation": 4,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 4,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AOG",
                                  "activation": 3,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 3,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "CW",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "DJ",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "WW",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  },
                  {
                      "current_pu": 1182,
                      "activation": 113383,
                      "MillisecondsInInterval": 86400000,
                      "activation_MillisecondsInInterval": 86400000,
                      "current_pu_MillisecondsInInterval": 86400000,
                      "__time": {
                          "start": "2025-08-09T00:00:00.000Z",
                          "end": "2025-08-10T00:00:00.000Z"
                      },
                      "SPLIT": {
                          "keys": [
                              "app"
                          ],
                          "attributes": [
                              {
                                  "name": "app",
                                  "type": "STRING"
                              },
                              {
                                  "name": "activation",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "current_pu",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "SPLIT",
                                  "type": "DATASET"
                              }
                          ],
                          "data": [
                              {
                                  "app": "EM",
                                  "activation": 95143,
                                  "current_pu": 204,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 77939,
                                              "current_pu": 102,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 13473,
                                              "current_pu": 75,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 2394,
                                              "current_pu": 18,
                                              "platform": "Windows"
                                          },
                                          {
                                              "activation": 788,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          },
                                          {
                                              "activation": 269,
                                              "current_pu": 2,
                                              "platform": "UWP"
                                          },
                                          {
                                              "activation": 232,
                                              "current_pu": 7,
                                              "platform": "Google Play Games"
                                          },
                                          {
                                              "activation": 48,
                                              "current_pu": 0,
                                              "platform": "MacOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG",
                                  "activation": 13095,
                                  "current_pu": 735,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 6662,
                                              "current_pu": 355,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 6433,
                                              "current_pu": 380,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "LT",
                                  "activation": 2095,
                                  "current_pu": 149,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1303,
                                              "current_pu": 44,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 650,
                                              "current_pu": 104,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 142,
                                              "current_pu": 1,
                                              "platform": "WebGL"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HA",
                                  "activation": 1487,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1390,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 97,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "TF",
                                  "activation": 528,
                                  "current_pu": 83,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 364,
                                              "current_pu": 49,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 164,
                                              "current_pu": 34,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "EP",
                                  "activation": 270,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 254,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 16,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "SOG",
                                  "activation": 192,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 192,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MM",
                                  "activation": 124,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 119,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG_CN",
                                  "activation": 111,
                                  "current_pu": 5,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 62,
                                              "current_pu": 2,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 30,
                                              "current_pu": 0,
                                              "platform": "MiniGame"
                                          },
                                          {
                                              "activation": 19,
                                              "current_pu": 3,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AC",
                                  "activation": 109,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 71,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 38,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HC",
                                  "activation": 93,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 72,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 21,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RS",
                                  "activation": 57,
                                  "current_pu": 2,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 57,
                                              "current_pu": 2,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "POZ",
                                  "activation": 29,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 16,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 13,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "IC",
                                  "activation": 26,
                                  "current_pu": 1,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 17,
                                              "current_pu": 1,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 9,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MO",
                                  "activation": 21,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 18,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 3,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "KL",
                                  "activation": 2,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 2,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AOG",
                                  "activation": 1,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "CW",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "DJ",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "WW",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  },
                  {
                      "activation": 101539,
                      "current_pu": 993,
                      "MillisecondsInInterval": 86400000,
                      "activation_MillisecondsInInterval": 86400000,
                      "current_pu_MillisecondsInInterval": 86400000,
                      "__time": {
                          "start": "2025-08-10T00:00:00.000Z",
                          "end": "2025-08-11T00:00:00.000Z"
                      },
                      "SPLIT": {
                          "keys": [
                              "app"
                          ],
                          "attributes": [
                              {
                                  "name": "app",
                                  "type": "STRING"
                              },
                              {
                                  "name": "activation",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "current_pu",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "SPLIT",
                                  "type": "DATASET"
                              }
                          ],
                          "data": [
                              {
                                  "app": "EM",
                                  "activation": 84472,
                                  "current_pu": 149,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 66244,
                                              "current_pu": 84,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 14576,
                                              "current_pu": 58,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 2555,
                                              "current_pu": 7,
                                              "platform": "Windows"
                                          },
                                          {
                                              "activation": 689,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          },
                                          {
                                              "activation": 215,
                                              "current_pu": 0,
                                              "platform": "UWP"
                                          },
                                          {
                                              "activation": 168,
                                              "current_pu": 0,
                                              "platform": "Google Play Games"
                                          },
                                          {
                                              "activation": 25,
                                              "current_pu": 0,
                                              "platform": "MacOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG",
                                  "activation": 12841,
                                  "current_pu": 677,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 6934,
                                              "current_pu": 358,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5907,
                                              "current_pu": 319,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "LT",
                                  "activation": 1637,
                                  "current_pu": 112,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 992,
                                              "current_pu": 31,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 535,
                                              "current_pu": 81,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 110,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HA",
                                  "activation": 1124,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1022,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 102,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "TF",
                                  "activation": 463,
                                  "current_pu": 50,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 321,
                                              "current_pu": 34,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 142,
                                              "current_pu": 16,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "EP",
                                  "activation": 253,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 229,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 24,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "SOG",
                                  "activation": 198,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 198,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AC",
                                  "activation": 128,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 71,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 57,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HC",
                                  "activation": 118,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 96,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 22,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG_CN",
                                  "activation": 106,
                                  "current_pu": 5,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 68,
                                              "current_pu": 4,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 21,
                                              "current_pu": 1,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 17,
                                              "current_pu": 0,
                                              "platform": "MiniGame"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MM",
                                  "activation": 91,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 86,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 5,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RS",
                                  "activation": 47,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 47,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "IC",
                                  "activation": 25,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 18,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 7,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MO",
                                  "activation": 18,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 14,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 4,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "POZ",
                                  "activation": 14,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 7,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 7,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AOG",
                                  "activation": 2,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "KL",
                                  "activation": 1,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "WW",
                                  "activation": 1,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  },
                  {
                      "activation": 969,
                      "current_pu": 0,
                      "MillisecondsInInterval": 86400000,
                      "activation_MillisecondsInInterval": 86400000,
                      "current_pu_MillisecondsInInterval": 86400000,
                      "__time": {
                          "start": "2025-08-11T00:00:00.000Z",
                          "end": "2025-08-12T00:00:00.000Z"
                      },
                      "SPLIT": {
                          "keys": [
                              "app"
                          ],
                          "attributes": [
                              {
                                  "name": "app",
                                  "type": "STRING"
                              },
                              {
                                  "name": "activation",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "current_pu",
                                  "type": "NUMBER"
                              },
                              {
                                  "name": "SPLIT",
                                  "type": "DATASET"
                              }
                          ],
                          "data": [
                              {
                                  "app": "EM",
                                  "activation": 842,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 677,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 128,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 26,
                                              "current_pu": 0,
                                              "platform": "Windows"
                                          },
                                          {
                                              "activation": 6,
                                              "current_pu": 0,
                                              "platform": "WebGL"
                                          },
                                          {
                                              "activation": 4,
                                              "current_pu": 0,
                                              "platform": "Google Play Games"
                                          },
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "UWP"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "MacOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG",
                                  "activation": 115,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 67,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 48,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "EP",
                                  "activation": 6,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 6,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MM",
                                  "activation": 3,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 3,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "RG_CN",
                                  "activation": 3,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          },
                                          {
                                              "activation": 1,
                                              "current_pu": 0,
                                              "platform": "MiniGame"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "AC",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "HC",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "IC",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "LT",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          },
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "IOS"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "app": "MO",
                                  "activation": 0,
                                  "current_pu": 0,
                                  "SPLIT": {
                                      "keys": [
                                          "platform"
                                      ],
                                      "attributes": [
                                          {
                                              "name": "platform",
                                              "type": "STRING"
                                          },
                                          {
                                              "name": "activation",
                                              "type": "NUMBER"
                                          },
                                          {
                                              "name": "current_pu",
                                              "type": "NUMBER"
                                          }
                                      ],
                                      "data": [
                                          {
                                              "activation": 0,
                                              "current_pu": 0,
                                              "platform": "Android"
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  }
              ]
          }
      }
  ]
}
import { Duration, Timezone } from '@topgames/chronoshift';
import { PlyType } from '../types';
import { SQLDialect } from './baseDialect';

export class ClickHouseDialect extends SQLDialect {
  static TIME_BUCKETING: Record<string, string> = {
    "PT1S": "%Y-%m-%d %H:%M:%S",
    "PT1M": "%Y-%m-%d %H:%M:%S",
    "PT1H": "%Y-%m-%d %H:%M:%S",
    "P1D":  "%Y-%m-%d %H:%M:%S",
    "P1W":  "%Y-%m-%d %H:%M:%S",
    "P1M":  "%Y-%m-%d %H:%M:%S",
    "P3M":  "%Y-%m-%d %H:%M:%S",
    "P1Y":  "%Y-%m-%d %H:%M:%S"
  };

  static DATE_TIME_FN: Record<string, string> = {
    "PT1S": "toStartOfSecond",
    "PT1M": "toStartOfMinute",
    "PT1H": "toStartOfHour",
    "P1D":  "toDate",
    "P1W":  "toStartOfWeek",
    "P1M":  "toStartOfMonth",
    "P3M":  "toStartOfQuarter",
    "P1Y":  "toStartOfYear"
  };

  static TIME_PART_TO_FUNCTION: Record<string, string> = {
    SECOND_OF_MINUTE: 'SECOND($$)',
    SECOND_OF_HOUR: '(MINUTE($$)*60+SECOND($$))',
    SECOND_OF_DAY: '((HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
    SECOND_OF_WEEK: '((((WEEKDAY($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
    SECOND_OF_MONTH: '((((DAYOFMONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
    SECOND_OF_YEAR: '((((DAYOFYEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',

    MINUTE_OF_HOUR: 'MINUTE($$)',
    MINUTE_OF_DAY: 'HOUR($$)*60+MINUTE($$)',
    MINUTE_OF_WEEK: '((WEEKDAY($$)-1)*24)+HOUR($$)*60+MINUTE($$)',
    MINUTE_OF_MONTH: '((DAYOFMONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$)',
    MINUTE_OF_YEAR: '((DAYOFYEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$)',

    HOUR_OF_DAY: 'HOUR($$)',
    HOUR_OF_WEEK: '((WEEKDAY($$)-1)*24+HOUR($$))',
    HOUR_OF_MONTH: '((DAYOFMONTH($$)-1)*24+HOUR($$))',
    HOUR_OF_YEAR: '((DAYOFYEAR($$)-1)*24+HOUR($$))',

    // DAY_OF_WEEK: '(WEEKDAY($$)+1)',
    DAY_OF_WEEK: 'WEEKDAY($$)',
    DAY_OF_MONTH: 'DAYOFMONTH($$)',
    DAY_OF_YEAR: 'DAYOFYEAR($$)',

    //WEEK_OF_MONTH: ???
    // WEEK_OF_YEAR: 'WEEK($$)', // ToDo: look into mode (https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_week)
    WEEK_OF_YEAR: 'DAYOFWEEK($$)',

    MONTH_OF_YEAR: 'MONTH($$)',
    YEAR: 'YEAR($$)'
  };

  static CAST_TO_FUNCTION: {[outputType: string]: {[inputType: string]: string}} = {
    TIME: {
      NUMBER: 'FROM_UNIXTIME($$ / 1000)'
    },
    NUMBER: {
      TIME: 'toUnixTimestamp($$) * 1000',
      STRING: 'CAST($$ AS SIGNED)'
    },
    STRING: {
      NUMBER: 'CAST($$ AS CHAR)'
    }
  };

  constructor() {
    super();
  }

  public escapeName(name: string): string {
    name = name.replace(/`/g, '``');
    return '`' + name + '`';
  }

  public escapeLiteral(name: string): string {
    if (name === null) return this.nullConstant();
    return JSON.stringify(name).replace(/\"/g, '\'');
  }

  public timeToSQL(date: Date): string {
    if (!date) return this.nullConstant();
    return `toDateTime('${this.dateToSQLDateString(date)}')`;
  }

  public concatExpression(a: string, b: string): string {
    return `CONCAT(${a},${b})`;
  }

  public containsExpression(a: string, b: string): string {
    return `LOCATE(${a},${b})>0`;
  }

  public isNotDistinctFromExpression(a: string, b: string): string {
    return `(${a}=${b})`;
  }

  public castExpression(inputType: PlyType, operand: string, cast: string): string {
    let castFunction = ClickHouseDialect.CAST_TO_FUNCTION[cast][inputType];
    if (!castFunction) throw new Error(`unsupported cast from ${inputType} to ${cast} in MySQL dialect`);
    return castFunction.replace(/\$\$/g, operand);
  }

  public utcToWalltime(operand: string, timezone: Timezone): string {
    if (timezone.isUTC()) return operand;
    return `toDateTime(${operand}), '${timezone}')`;
  }

  public walltimeToUTC(operand: string, timezone: Timezone): string {
    if (timezone.isUTC()) return operand;
    return `toDateTime(${operand}, '${timezone}')`;
  }

  public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
    let timeBucketing = duration.toString()
    let bucketFormat = ClickHouseDialect.TIME_BUCKETING[timeBucketing];
    let dateTimeFn = `${ClickHouseDialect.DATE_TIME_FN[timeBucketing]}(${this.utcToWalltime(operand, timezone)})`
    if (!bucketFormat) throw new Error(`unsupported duration '${duration}'`);
    return this.walltimeToUTC(`toDateTime(formatDateTime(${dateTimeFn},'${bucketFormat}'))`, timezone);
  }

  public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return this.timeFloorExpression(operand, duration, timezone);
  }

  public timePartExpression(operand: string, part: string, timezone: Timezone): string {
    let timePartFunction = ClickHouseDialect.TIME_PART_TO_FUNCTION[part];
    if (!timePartFunction) throw new Error(`unsupported part ${part} in ClickHouse dialect`);
    return timePartFunction.replace(/\$\$/g, this.utcToWalltime(operand, timezone));
  }

  public timeShiftExpression(operand: string, duration: Duration, timezone: Timezone): string {
    let sqlFn = "DATE_ADD("; //warpDirection > 0 ? "DATE_ADD(" : "DATE_SUB(";
    let spans = duration.valueOf();
    if (spans.week) {
      return sqlFn + operand + ", INTERVAL " + String(spans.week) + ' WEEK)';
    }
    if (spans.year || spans.month) {
      let expr = String(spans.year || 0) + "-" + String(spans.month || 0);
      operand = sqlFn + operand + ", INTERVAL '" + expr + "' YEAR_MONTH)";
    }
    if (spans.day || spans.hour || spans.minute || spans.second) {
      let expr = String(spans.day || 0) + " " + [spans.hour || 0, spans.minute || 0, spans.second || 0].join(':');
      operand = sqlFn + operand + ", INTERVAL '" + expr + "' DAY_SECOND)";
    }
    return operand;
  }

  public extractExpression(operand: string, regexp: string): string {
    throw new Error('ClickHouse must implement extractExpression (https://github.com/mysqludf/lib_mysqludf_preg)');
  }

  public indexOfExpression(str: string, substr: string): string {
    return `LOCATE(${substr}, ${str}) - 1`;
  }
}


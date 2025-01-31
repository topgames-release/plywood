/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2019 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

{// starts with function(plywood, chronoshift)
var Timezone = chronoshift.Timezone;
var ply = plywood.ply;
var $ = plywood.$;
var i$ = plywood.i$;
var r = plywood.r;
var Expression = plywood.Expression;
var LiteralExpression = plywood.LiteralExpression;
var MatchExpression = plywood.MatchExpression;
var SortExpression = plywood.SortExpression;
var Set = plywood.Set;

var dataRef = $('data');
var NULL = {}; // A dummy for null

function undummyNull(x) {
  return x === NULL ? null : x;
}

// See here: https://www.drupal.org/node/141051
var reservedWords = {
  ALL: 1, AND: 1, AS: 1, ASC: 1,
  BETWEEN: 1, BY: 1,
  CONTAINS: 1, CREATE: 1, CASE: 1,
  DELETE: 1, DESC: 1, DESCRIBE: 1, DISTINCT: 1, DROP: 1,
  EXISTS: 1, EXPLAIN: 1, ESCAPE: 1, END: 1,
  FALSE: 1, FROM: 1,
  GROUP: 1,
  HAVING: 1,
  IN: 1, INNER: 1, INSERT: 1, INTO: 1, IS: 1,
  JOIN: 1,
  LEFT: 1, LIKE: 1, LIMIT: 1, LOOKUP: 1,
  MATCH: 1,
  NOT: 1, NULL: 1,
  ON: 1, OR: 1, ORDER: 1,
  REPLACE: 1, REGEXP: 1,
  SELECT: 1, SET: 1, SHOW: 1,
  TABLE: 1, TRUE: 1, THEN: 1,
  UNION: 1, UPDATE: 1,
  VALUES: 1,
  WHERE: 1, WHEN: 1
};

var unsupportedVerbs = {
  ALTER: 1,
  CALL: 1,
  CREATE: 1,
  DEALLOCATE: 1,
  DELETE: 1,
  DO: 1,
  DROP: 1,
  EXECUTE: 1,
  HANDLER: 1,
  INSERT: 1,
  KILL: 1,
  LOAD: 1,
  LOCK: 1,
  PREPARE: 1,
  RENAME: 1,
  REPLACE: 1,
  SAVEPOINT: 1,
  START: 1,
  TRUNCATE: 1,
  UNLOCK: 1,
  UPDATE: 1
};

var intervalUnits = {
  MICROSECOND: 1,
  SECOND: 1,
  MINUTE: 1,
  HOUR: 1,
  DAY: 1,
  WEEK: 1,
  MONTH: 1,
  QUARTER: 1,
  YEAR: 1
}

var durationFormats = {
  '%Y-%m-%d %H:%i:%s': 'PT1S',
  '%Y-%m-%d %H:%i:00': 'PT1M',
  '%Y-%m-%d %H:00:00': 'PT1H',
  '%Y-%m-%d': 'P1D',
  '%Y-%m-01': 'P1M',
  '%Y-01-01': 'P1Y'
};

var timePartFormats = {
  '%Y': 'YEAR'
};

function findTimePart(fmt, lookup) {
  var fmts = Object.keys(lookup);
  for (var i=0; i < fmts.length; i++) {
    var possibleFmt = fmts[i];
    if (fmt.indexOf(possibleFmt) !== -1) {
      return {
        pre: fmt.substring(0, fmt.indexOf(possibleFmt)),
        part: lookup[possibleFmt],
        post: fmt.substring(possibleFmt.length)
       }
    }
    return null;
  }
}

var castTypes = {
  CHAR: 'STRING',
  CHARACTER: 'STRING',
  VARCHAR: 'STRING',
  TEXT: 'STRING',
  SIGNED: 'NUMBER',
  FLOAT: 'NUMBER',
  REAL: 'NUMBER',
  INTEGER: 'NUMBER',
  BIGINT: 'NUMBER',
  DECIMAL: 'NUMBER',
  NUMERIC: 'NUMBER'
}

function upgrade(v) {
  if (!(v instanceof Expression)) return r(v);
  return v;
}

function extractString(param) {
  if (param instanceof LiteralExpression && param.type === 'STRING') {
    return param.value;
  }
  error('expecting a string but got ' + param + ' instead');
}

function dateAddSub(op, d) {
  if (!Expression.ZERO.equals(d)) error('only zero interval supported in date add and sub');

  var headOperand = op.getHeadOperand();
  if (headOperand.op === 'ref') {
    var template = headOperand.timePart('YEAR').cast('STRING')
      .concat(r("-"))
      .concat(r(3).multiply(headOperand.timePart('QUARTER').subtract(1)).add(1).cast('STRING'))
      .concat(r("-01 00:00:00"));

    if (template.equals(op)) {
      op = headOperand.timeFloor('P3M');
    }
  }

  return op;
}

var notImplemented = function() { error('not implemented yet'); };
var fns = {
  ABSOLUTE: function(op) { return op.absolute(); },
  OVERLAP: function(op, ex) { return op.overlap(ex); },
  SQRT: function(op) { return op.power(0.5); },
  EXP: function(ex) { return r(Math.E).power(ex); },
  POWER: function(op, ex) { return op.power(ex); },
  LOG: function(op, ex) { return ex ? ex.log(op) : op.log(); },
  LOG2: function(op) { return op.log(2); },
  LOG10: function(op) { return op.log(10); },
  LN: function(op) { return op.log(); },
  NOW: function() { return r(new Date()); },
  CURDATE: function() { return r(chronoshift.day.floor(new Date(), Timezone.UTC)); },
  CUSTOM_TRANSFORM: function(op, fn) { return op.customTransform(fn); },
  ISNULL: function(ex) { return ex.is(null); },
  FALLBACK: function(op, ex) { return op.fallback(ex); },
  IF: function(op, ex1, ex2) { return op.then(ex1).fallback(ex2); },
  NULLIF: function(op, ex1) { return op.isnt(ex1).then(op); },
  MATCH: function(op, reg) { return op.match(reg); },
  EXTRACT: function(op, reg) { return op.extract(reg); },
  CONCAT: function() {
    return Expression.concat(Array.prototype.map.call(arguments, function(e) {
      if (e.type === 'NUMBER') e = e.cast('STRING');
      return e;
    }));
  },
  SUBSTRING: function(op, i, n) { return op.substr(i, n); },
  UPPER: function(op) { return op.transformCase('upperCase'); },
  LOWER: function(op) { return op.transformCase('lowerCase'); },
  LENGTH: function(op) { return op.length(); },
  LOCATE: function(op, ex) { return ex.indexOf(op).add(1); },
  TIME_FLOOR: function(op, d, tz) { return op.timeFloor(d, tz); },
  TIME_SHIFT: function(op, d, s, tz) { return op.timeShift(d, s, tz); },
  TIME_RANGE: function(op, d, s, tz) { return op.timeRange(d, s, tz); },
  TIME_BUCKET: function(op, d, tz) { return op.timeBucket(d, tz); },
  NUMBER_BUCKET: function(op, s, o) { return op.numberBucket(s, o); },
  TIME_PART: function(op, part, tz) { return op.timePart(part, tz); },
  LOOKUP: function(op, name) { return op.lookup(name); },
  PI: function() { return r(Math.PI); },
  STD: notImplemented,
  DATE_FORMAT: function(op, format) {
    format = extractString(format);
    var duration = durationFormats[format.replace(/ 00:00:00$/, '')];
    if (duration) {
      return op.timeFloor(duration);
    } else {
      var timePart = findTimePart(format, timePartFormats);
      if (!timePart) error('unsupported format: ' + format);
      var ex = op.timePart(timePart.part);
      if (timePart.pre || timePart.post) ex = ex.cast("STRING");
      if (timePart.pre) ex = r(timePart.pre).concat(ex);
      if (timePart.post) ex = ex.concat(r(timePart.post));
      return ex;
    }
  },

  YEAR: function(op, tz) { return op.timePart('YEAR', tz); },
  QUARTER: function(op, tz) { return op.timePart('QUARTER', tz) },
  MONTH: function(op, tz) { return op.timePart('MONTH_OF_YEAR', tz); },
  WEEK_OF_YEAR: function(op, tz) { return op.timePart('WEEK_OF_YEAR', tz); },
  DAY_OF_YEAR: function(op, tz) { return op.timePart('DAY_OF_YEAR', tz); },
  DAY_OF_MONTH: function(op, tz) { return op.timePart('DAY_OF_MONTH', tz); },
  DAY_OF_WEEK: function(op, tz) { return op.timePart('DAY_OF_WEEK', tz); },
  WEEKDAY: notImplemented,
  HOUR: function(op, tz) { return op.timePart('HOUR_OF_DAY', tz); },
  MINUTE: function(op, tz) { return op.timePart('MINUTE_OF_HOUR', tz); },
  SECOND: function(op, tz) { return op.timePart('SECOND_OF_MINUTE', tz); },
  DATE: function(op, tz) { return op.timeFloor('P1D', tz); },
  TIMESTAMP: function(op) { return op.upgradeToType('TIME'); },
  TIME: function() { error('time literals are not supported'); },
  DATE_ADD: dateAddSub,
  DATE_SUB: dateAddSub,
  FROM_UNIXTIME: function(op) { return op.multiply(1000).cast('TIME') },
  CAST: function(op, ct) {
    ct = extractString(ct);
    var castTo = castTypes[ct.toUpperCase()];
    if (!castTo) error('can not handle CAST to ' + ct);
    return op.cast(castTo);
  },
  UNIX_TIMESTAMP: function(op) { return op.cast('NUMBER').divide(1000); },

  // Information Functions
  BENCHMARK: function() { return r(0); },
  CHARSET: function() { return r('utf8mb4'); },
  COERCIBILITY: function() { return r(0); },
  COLLATION: function() { return r('utf8mb4_unicode_ci'); },
  CONNECTION_ID: function() { return r(123); }, // ToDo
  DATABASE: function() { return r('plyql1'); },
  FOUND_ROWS: function() { return r(2005); },
  LAST_INSERT_ID: function() { return r(0); },
  ROW_COUNT: function() { return r(0); },
  USER: function() { return r('plyql@localhost'); },
  VERSION: function() { return r('5.7.11'); }
};
fns.ABS = fns.ABSOLUTE;
fns.POW = fns.POWER;
fns.LEN = fns.LENGTH;
fns.CHAR_LENGTH = fns.LENGTH;
fns.IFNULL = fns.FALLBACK;
fns.SUBSTR = fns.SUBSTRING;
fns.CURRENT_TIMESTAMP = fns.NOW;
fns.LCASE = fns.LOWER;
fns.LOCALTIME = fns.NOW;
fns.LOCALTIMESTAMP = fns.NOW;
fns.UTC_TIMESTAMP = fns.NOW;
fns.UCASE = fns.UPPER;
fns.SYSDATE = fns.NOW;
fns.CURRENT_DATE = fns.CURDATE;
fns.UTC_DATE = fns.CURDATE;
fns.DAY_OF_YEAR = fns.DAY_OF_YEAR;
fns.DOY = fns.DAY_OF_YEAR;
fns.DOW = fns.DAY_OF_WEEK;
fns.DAYOFMONTH = fns.DAY_OF_MONTH;
fns.DAY = fns.DAY_OF_MONTH;
fns.WEEKOFYEAR = fns.WEEK_OF_YEAR;
fns.WEEK = fns.WEEK_OF_YEAR;
fns.ADDDATE = fns.DATE_ADD;
fns.SUBDATE = fns.DATE_SUB;
fns.STDDEV = fns.STD;
fns.STDDEV_POP = fns.STD;

// Information Functions
fns.SESSION_USER = fns.USER;
fns.SYSTEM_USER = fns.USER;
fns.CURRENT_USER = fns.USER;
fns.SCHEMA = fns.DATABASE;

function reserved(str) {
  return reservedWords.hasOwnProperty(str.toUpperCase());
}

function makeDate(type, v) {
  try {
    return chronoshift.parseSQLDate(type, v);
  } catch (e) {
    var isoDate = chronoshift.parseISODate(v);
    if (isoDate) {
      if (type === 'd') isoDate = chronoshift.day.floor(isoDate, Timezone.UTC);
      return isoDate;
    }
    error(e.message);
  }
}

function getFromTable(from) {
  if (!from) return null;
  if (from.verb === 'SELECT') return from.table; // From is a sub-query
  return from.name; // From is a ref: `namespace`.`name`
}

function getFromDatabase(from) {
  if (!from) return null;
  if (from.verb === 'SELECT') return from.database; // From is a sub-query
  return from.namespace; // From is a ref: `namespace`.`name`
}

function extractGroupByColumn(columns, groupBy, index) {
  var label = null;
  var otherColumns = [];
  for (var i = 0; i < columns.length; i++) {
    var column = columns[i];
    if (groupBy.equals(column.expression)) {
      if (label) error('already have a label');
      label = column.name;
    } else {
      otherColumns.push(column);
    }
  }
  if (!label) label = 'split' + index;
  return {
    label: label,
    otherColumns: otherColumns
  };
}

function upgradeGroupBys(distinct, columns, groupBys) {
  if (Array.isArray(columns)) { // Not *
    if (!groupBys) {
      // Support for not having a group by clause if there are aggregates in the columns
      // A having an aggregate columns is the same as having "GROUP BY ''"

      var hasAggregate = columns.some(function(column) {
        var columnExpression = column.expression;
        return columnExpression.some(function(ex) { return ex.isAggregate() ? true : null; });
      })
      if (hasAggregate) {
        return [Expression.EMPTY_STRING];
      } else if (distinct) {
        return columns.map(function(column) { return column.expression });
      }

    } else {
      return groupBys.map(function(groupBy) {
        if (groupBy.isOp('literal') && groupBy.type === 'NUMBER') {
          // Support for not having a group by clause refer to a select column by index

          var groupByColumn = columns[groupBy.value - 1];
          if (!groupByColumn) error("Unknown column '" + groupBy.value + "' in group by statement");

          return groupByColumn.expression;
        } else {
          return groupBy;
        }
      });
    }
  }

  return groupBys;
}

function staticColumn(column) {
  return column.expression.getFreeReferences().length === 0;
}

function constructQuery(distinct, columns, from, where, groupBys, having, orderBy, limit) {
  if (!columns) error('Can not have empty column list');
  var originalColumns = Array.isArray(columns) ? columns.map(function(c) { return c.name }) : [];
  var query = null;

  if (!distinct && Array.isArray(columns) && !from && !where && !groupBys && columns.every(staticColumn)) {
    // This is a SELECT 1+1; type query
    query = ply();
    for (var i = 0; i < columns.length; i++) {
      query = query.performAction(columns[i]);
    }

  } else {
    var fromEx = from ? (from.verb === 'SELECT' ? from.expression : $(from.name)) : dataRef;

    if (where) {
      fromEx = fromEx.filter(where);
    }

    groupBys = upgradeGroupBys(distinct, columns, groupBys);


    if (!groupBys) {
      // Select query
      query = fromEx;

      if (Array.isArray(columns)) {
        var attributes = [];
        for (var i = 0; i < columns.length; i++) {
          var column = columns[i];
          query = query.performAction(column);
          attributes.push(column.name);
        }
        query = query.select.apply(query, attributes);
      }

    } else {
      // Group By query
      if (columns === '*') error('can not SELECT * with a GROUP BY');

      if (groupBys.length === 1 && groupBys[0].isOp('literal')) {
        query = ply().apply('data', fromEx);
      } else {
        var splits = {};
        for (var i = 0; i < groupBys.length; i++) {
          var groupBy = groupBys[i];
          var extract = extractGroupByColumn(columns, groupBy, i);
          columns = extract.otherColumns;
          splits[extract.label] = groupBy;
        }
        query = fromEx.split(splits, 'data');
      }

      if (Array.isArray(columns)) {
        for (var i = 0; i < columns.length; i++) {
          query = query.performAction(columns[i]);
        }
      }

      if (originalColumns.length > 1) query = query.select.apply(query, originalColumns);
    }
  }

  if (having) {
    query = query.performAction(having);
  }
  if (orderBy) {
    query = query.performAction(orderBy);
  }
  if (limit) {
    query = query.performAction(limit);
  }

  return query;
}

function makeListMap1(head, tail) {
  if (head == null) return [];
  return [head].concat(tail.map(function(t) { return t[1] }));
}

function naryExpressionFactory(op, head, tail) {
  if (!tail.length) return head;
  return head[op].apply(head, tail.map(function(t) { return t[1]; }));
}

function naryExpressionWithAltFactory(op, head, tail, altToken, altOp) {
  if (!tail.length) return head;
  for (var i = 0; i < tail.length; i++) {
    var t = tail[i];
    var idx = altToken.indexOf(t[0]);
    head = head[idx >= 0 ? altOp[idx] : op].call(head, t[1]);
  }
  return head;
}

}// Start grammar

start
  = _ queryParse:Query QueryTerminator? { return queryParse; }

Query
  = queryParse:(SelectQuery / DescribeQuery / ShowQuery / SetQuery / UseQuery / UnsupportedQuery)
    {
      return queryParse;
    }
  / ex:Expression
    {
      return {
        verb: null,
        expression: ex
      }
    }

ShowQuery
  = ShowToken ex:ShowQueryExpression
    {
      return {
        verb: 'SELECT',
        rewrite: 'SHOW',
        database: 'information_schema',
        expression: ex
      };
    }


ShowQueryExpression
  = (GlobalToken / SessionToken)? what:(VariablesToken / StatusToken) like:LikeRhs? where:WhereClause?
    {
      // https://dev.mysql.com/doc/refman/5.7/en/show-variables.html
      // https://dev.mysql.com/doc/refman/5.7/en/show-status.html
      var ex = i$('GLOBAL_' + what);  // what = 'VARIABLES' | 'STATUS'
      if (like) ex = ex.filter(like(i$('VARIABLE_NAME')));
      if (where) ex = ex.filter(where);
      return ex
        .apply('Variable_name', i$('VARIABLE_NAME'))
        .apply('Value', i$('VARIABLE_VALUE'))
        .select('Variable_name', 'Value');
    }
  / (SchemasToken / DatabasesToken) like:LikeRhs?
    {
      // https://dev.mysql.com/doc/refman/5.0/en/schemata-table.html
      var ex = i$('SCHEMATA')
      if (like) ex = ex.filter(like(i$('SCHEMA_NAME')));
      return ex
        .apply('Database', i$('SCHEMA_NAME'))
        .select('Database');
    }
  / full:FullToken? TablesToken db:(FromOrIn Ref)? like:LikeRhs?
    {
      // https://dev.mysql.com/doc/refman/5.0/en/tables-table.html
      var ex = i$('TABLES')
      if (db) ex= ex.filter(i$('TABLE_SCHEMA').is(r(db[1])));
      if (like) ex = ex.filter(like(i$('TABLE_NAME')));
      ex = ex
        .apply('Tables_in_database', $('TABLE_NAME'));

      if (full) {
        ex = ex
          .apply('Table_type', i$('TABLE_TYPE'))
          .select('Tables_in_database', 'Table_type');
      } else {
        ex = ex.select('Tables_in_database');
      }

      return ex;
    }
  / full:FullToken? ColumnsToken FromOrIn table:RelaxedNamespacedRef db:(FromOrIn Ref)? like:LikeRhs? where:WhereClause?
    {
      // https://dev.mysql.com/doc/refman/5.0/en/columns-table.html
      var ex = $('COLUMNS').filter($('TABLE_NAME').is(r(table.name)));
      db = db ? db[1] : table.namespace;
      if (db) ex = ex.filter($('TABLE_SCHEMA').is(r(db)));
      if (like) ex = ex.filter(like(i$('COLUMN_NAME')));
      if (where) ex = ex.filter(where);
      ex = ex
        .apply('Field', i$('COLUMN_NAME'))
        .apply('Type', i$('COLUMN_TYPE'))
        .apply('Null', i$('IS_NULLABLE'))
        .apply('Key', i$('COLUMN_KEY'))
        .apply('Default', i$('COLUMN_DEFAULT'))
        .apply('Extra', i$('EXTRA'))

      if (full) {
        ex = ex
          .apply('Collation', i$('COLLATION_NAME'))
          .apply('Privileges', i$('PRIVILEGES'))
          .apply('Comment', i$('COLUMN_COMMENT'))
          .select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra', 'Collation', 'Privileges', 'Comment')
      } else {
        ex = ex.select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra')
      }

      return ex;
    }
  / (CharacterToken _ SetToken) like:LikeRhs?
    {
      // https://dev.mysql.com/doc/refman/5.7/en/character-sets-table.html
      var ex = i$('CHARACTER_SETS')
      if (like) ex = ex.filter(like(i$('CHARACTER_SET_NAME')));
      return ex
        .apply('Charset', i$('CHARACTER_SET_NAME'))
        .apply('Default collation', i$('DEFAULT_COLLATE_NAME'))
        .apply('Description', i$('DESCRIPTION'))
        .apply('Maxlen', i$('MAXLEN'))
        .select('Charset', 'Default collation', 'Description', 'Maxlen');
    }
  / CollationToken like:LikeRhs?
    {
      // https://dev.mysql.com/doc/refman/5.7/en/collations-table.html
      var ex = i$('COLLATIONS')
      if (like) ex = ex.filter(like(i$('COLLATION_NAME')));
      return ex
        .apply('Collation', i$('COLLATION_NAME'))
        .apply('Charset', i$('CHARACTER_SET_NAME'))
        .apply('Id', i$('ID'))
        .apply('Default', i$('IS_DEFAULT'))
        .apply('Compiled', i$('IS_COMPILED'))
        .apply('Sortlen', i$('SORTLEN'))
        .select('Collation', 'Charset', 'Id', 'Default', 'Compiled', 'Sortlen');

    }
  // Empty Tables
  / IndexToken FromOrIn table:RelaxedNamespacedRef db:(FromOrIn Ref)? like:LikeRhs? where:WhereClause?
    {
      // http://dev.mysql.com/doc/refman/5.7/en/show-index.html
      return i$('INDEX');
    }
  / WarningsToken LimitClause?
    {
      // http://dev.mysql.com/doc/refman/5.7/en/show-warnings.html
      return i$('WARNINGS');
    }

FromOrIn = FromToken / InToken;


SetQuery
  = verb:SetToken rest:$(.*)
    {
      return {
        verb: verb,
        rest: rest
      };
    }

UseQuery
  = verb:UseToken db:Ref
    {
      return {
        verb: verb,
        database: db
      };
    }

DescribeQuery
  = (DescribeToken / DescToken) table:RelaxedNamespacedRef colRef:Ref? wild:String?
    {
      var ex = $('COLUMNS').filter($('TABLE_NAME').is(r(table.name)));
      if (table.namespace) ex = ex.filter($('TABLE_SCHEMA').is(r(table.namespace)));
      if (colRef) {
        ex = ex.filter(i$('COLUMN_NAME').is(r(colRef)));
      } else if (wild) {
        ex = ex.filter(i$('COLUMN_NAME').match(MatchExpression.likeToRegExp(wild)));
      }

      ex = ex
        .apply('Field', i$('COLUMN_NAME'))
        .apply('Type', i$('COLUMN_TYPE'))
        .apply('Null', i$('IS_NULLABLE'))
        .apply('Key', i$('COLUMN_KEY'))
        .apply('Default', i$('COLUMN_DEFAULT'))
        .apply('Extra', i$('EXTRA'))
        .select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra');

      return {
        verb: 'SELECT',
        rewrite: 'DESCRIBE',
        table: table.name,
        database: 'information_schema',
        expression: ex
      };
    }

UnsupportedQuery
  = verb:Name &{ return unsupportedVerbs[verb.toUpperCase()]; } _ rest:$(.*)
    {
      return {
        verb: verb.toUpperCase(),
        rest: rest
      };
    }

SelectQuery
  = SelectToken distinct:DistinctToken? columns:Columns? from:FromClause? where:WhereClause? groupBys:GroupByClause? having:HavingClause? orderBy:OrderByClause? limit:LimitClause?
    {
      return {
        verb: 'SELECT',
        expression: constructQuery(distinct, columns, from, where, groupBys, having, orderBy, limit),
        table: getFromTable(from),
        database: getFromDatabase(from)
      };
    }

SelectSubQuery
  = SelectToken distinct:DistinctToken? columns:Columns? where:WhereClause? groupBys:GroupByClause having:HavingClause? orderBy:OrderByClause? limit:LimitClause?
    { return constructQuery(distinct, columns, null, where, groupBys, having, orderBy, limit); }

Columns
  = (Ref Dot)? StarToken
    { return '*'; }
  / head:Column tail:(Comma Column)*
    { return makeListMap1(head, tail); }

Column
  = ex:Expression as:AsOptional?
    {
      if (as == null) {
        as = text().trim();
        if (as[0] === '`' && as[as.length - 1] === '`') as = as.substr(1, as.length - 2);
      }
      return Expression._.apply(as, ex);
    }

AsMandatory
  = AsToken? name:(String / Ref) { return name; }

AsOptional
  = AsToken? name:(String / Ref) { return name; }

FromClause
  = FromToken fc:FromContent AsOptional?
    { return fc; }

FromContent
  = RelaxedNamespacedRef
  / OpenParen subQuery:SelectQuery CloseParen
    { return subQuery; }


WhereClause
  = WhereToken filter:Expression
    { return filter; }

GroupByClause
  = GroupToken ByToken head:Expression tail:(Comma Expression)*
    { return makeListMap1(head, tail); }

HavingClause
  = HavingToken having:Expression
    { return Expression._.filter(having); }

OrderByClause
  = OrderToken ByToken orderBy:Expression direction:Direction? tail:(Comma Expression Direction?)*
    {
      if (tail.length) error('plywood does not currently support multi-column ORDER BYs');
      return Expression._.sort(orderBy, direction || 'ascending');
    }

Direction
  = AscToken / DescToken

LimitClause
  = LimitToken a:Number b:(Comma Number)?
    {
      var limit;
      if (b) {
        if (a !== 0) error('can not skip for now');
        limit = b[1];
      } else {
        limit = a;
      }
      return Expression._.limit(limit);
    }

QueryTerminator
  = ";" _

/*
Expressions are defined below in acceding priority order

  Or (OR)
  And (AND)
  Not (NOT)
  Comparison (=, <=>, <, >, <=, >=, <>, !=, IS, LIKE, BETWEEN, IN, CONTAINS, REGEXP)
  Additive (+, -)
  Multiplicative (*), Division (/)
  Unary identity (+), negation (-)
*/

Expression = OrExpression


OrExpression
  = head:AndExpression tail:(OrToken AndExpression)*
    { return naryExpressionFactory('or', head, tail); }


AndExpression
  = head:NotExpression tail:(AndToken NotExpression)*
    { return naryExpressionFactory('and', head, tail); }


NotExpression
  = not:NotToken? ex:ComparisonExpression
    {
      if (not) ex = ex.not();
      return ex;
    }


ComparisonExpression
  = ex:AdditiveExpression rhs:ComparisonExpressionRhs?
    {
      if (rhs) ex = rhs(ex);
      return ex;
    }

ComparisonExpressionRhs
  = not:NotToken? rhs:ComparisonExpressionRhsNotable
    {
      if (!not) return rhs;
      return function(ex) { return rhs(ex).not(); };
    }
  / IsToken not:NotToken? rhs:AdditiveExpression
    {
      return function(ex) {
        ex = ex.is(rhs);
        if (not) ex = ex.not();
        return ex;
      };
    }
  / op:ComparisonOp _ lhs:AdditiveExpression
    {
      return function(ex) { return ex[op](lhs); };
    }

ComparisonExpressionRhsNotable
  = BetweenToken start:(AdditiveExpression) AndToken end:(AdditiveExpression)
    {
      return function(ex) { return ex.greaterThanOrEqual(start).and(ex.lessThanOrEqual(end)); };
    }
  / InToken list:(InSetLiteralExpression / AdditiveExpression)
    {
      return function(ex) { return ex.is(list); };
    }
  / ContainsToken string:String
    {
      return function(ex) { return ex.contains(string, 'ignoreCase'); };
    }
  / LikeRhs
  / RegExpToken string:String
    {
      return function(ex) { return ex.match(string); };
    }

LikeRhs
  = LikeToken string:String escape:(EscapeToken String)?
    {
      var escapeStr = escape ? escape[1] : '\\';
      if (escapeStr.length > 1) error('Invalid escape string: ' + escapeStr);
      var regExp = MatchExpression.likeToRegExp(string, escapeStr);
      return function(ex) { return ex.match(regExp); };
    }

ComparisonOp "Comparison"
  = "="   { return 'is'; }
  / "<=>" { return 'is'; }
  / "<>"  { return 'isnt'; }
  / "!="  { return 'isnt'; }
  / "<="  { return 'lessThanOrEqual'; }
  / ">="  { return 'greaterThanOrEqual'; }
  / "<"   { return 'lessThan'; }
  / ">"   { return 'greaterThan'; }


AdditiveExpression
  = head:MultiplicativeExpression tail:(AdditiveOp MultiplicativeExpression)*
    { return naryExpressionWithAltFactory('add', head, tail, ['-'], ['subtract']); }

AdditiveOp = op:("+" / "-") !"+" _ { return op; }


MultiplicativeExpression
  = head:UnaryExpression tail:(MultiplicativeOp UnaryExpression)*
    { return naryExpressionWithAltFactory('multiply', head, tail, ['/', '%', '&'], ['divide', 'mod', 'bitwiseAnd']); }

MultiplicativeOp = op:("*" / "/" / "%" / '&') _ { return op; }


UnaryExpression
  = op:AdditiveOp !Number ex:BasicExpression
    {
      // !Number is to make sure that -3 parses as literal(-3) and not literal(3).negate()
      var negEx = ex.negate(); // Always negate (even with +) just to make sure it is possible
      return op === '-' ? negEx : ex;
    }
  / BasicExpression


BasicExpression
  = LiteralExpression
  / AggregateExpression
  / FunctionCallExpression
  / CaseExpression
  / OpenParen sub:(Expression / SelectSubQuery) CloseParen { return sub; }
  / RefExpression

AggregateExpression
  = CountToken OpenParen distinct:DistinctToken? exd:ExpressionMaybeFiltered? CloseParen
    {
      if (!exd) {
        if (distinct) error('COUNT DISTINCT must have an expression');
        return dataRef.count();
      } else if (exd.ex === '*') {
        if (distinct) error('COUNT DISTINCT can not be used with *');
        return exd.data.count();
      } else {
        return distinct ? exd.data.countDistinct(exd.ex) : exd.data.filter(exd.ex.isnt(null)).count()
      }
    }
  / fn:AggregateFn OpenParen distinct:DistinctToken? exd:ExpressionMaybeFiltered CloseParen
    {
      if (distinct) error('can not use DISTINCT for ' + fn + ' aggregator');
      if (exd.ex === '*') error('can not use * for ' + fn + ' aggregator');
      return exd.data[fn](exd.ex);
    }
  / QuantileToken OpenParen distinct:DistinctToken? exd:ExpressionMaybeFiltered Comma value:Number tuning:(Comma String)? CloseParen
    {
      if (distinct) error('can not use DISTINCT for quantile aggregator');
      if (exd.ex === '*') error('can not use * for quantile aggregator');
      return exd.data.quantile(exd.ex, value, tuning ? tuning[1] : null);
    }
  / (CustomAggregateToken / CustomToken) OpenParen value:String filter:WhereClause? CloseParen
    {
      var d = dataRef;
      if (filter) d = d.filter(filter);
      return d.customAggregate(value);
    }

AggregateFn
  = SumToken / AvgToken / MinToken / MaxToken / CountDistinctToken

ExpressionMaybeFiltered
  = ex:(StarToken / Expression) filter:WhereClause?
    {
      var data = dataRef;
      if (filter) data = data.filter(filter);
      return { ex: ex, data: data };
    }


FunctionCallExpression
  = fn:Fn OpenParen params:(Expression AsMandatory / Params) CloseParen
    { return fn.apply(null, params.map(upgrade)); }

Fn
  = name:Name &{ return fns[name.toUpperCase()]; }
    { return fns[name.toUpperCase()]; }

Params
  = head:Param? tail:(Comma Param)*
    { return makeListMap1(head, tail); }

Param = Number / String / Interval / Expression;


CaseExpression
  = CaseToken v:Expression? cases:(WhenToken Expression ThenToken Expression)+ els:(ElseToken Expression)? EndToken
    {
      var ex;
      cases.forEach(function(c) {
        var cond = c[1];
        if (v) cond = v.is(cond);
        var myEx = cond.then(c[3]);
        ex = ex ? ex.fallback(myEx) : myEx;
      });
      if (els) ex = ex.fallback(els[1]);
      return ex;
    }

RefExpression
  = ref:NamespacedRef { return i$(ref.name); }

RelaxedNamespacedRef
  = ns:(Ref Dot)? name:RelaxedRef
    {
      return {
        namespace: ns ? ns[0] : null,
        name: name
      };
    }

NamespacedRef
  = ns:(Ref Dot)? name:Ref
    {
      return {
        namespace: ns ? ns[0] : null,
        name: name
      };
    }

RelaxedRef
  = name:RelaxedName !{ return reserved(name); }
    { return name }
  / BacktickRef

Ref
  = name:Name !{ return reserved(name); }
    { return name }
  / BacktickRef

BacktickRef
  = "`" name:$([^`]+) "`" _
    { return name }

NameOrString = Name / String

LiteralExpression
  = OpenCurly type:(DToken / TToken / TsToken) v:String CloseCurly
    { return r(makeDate(type, v)); }
  / type:(DateToken / TimeToken / TimestampToken) v:String
    { return r(makeDate(type, v)); }
  / v:(Number / String / SetLiteral / NullToken / TrueToken / FalseToken)
    { return r(undummyNull(v)); }


SetLiteral
  = OpenCurly head:StringNumberOrNull? tail:(Comma StringNumberOrNull)* CloseCurly
    { return Set.fromJS(makeListMap1(head, tail).map(undummyNull)); }

StringNumberOrNull = String / Number / NullToken


InSetLiteralExpression
  = OpenParen head:StringOrNumber tail:(Comma StringOrNumber)* CloseParen
    { return r(Set.fromJS(makeListMap1(head, tail))); }

StringOrNumber = String / Number

String "String"
  = CharsetIntroducer? "'" chars:NotSQuote "'" _ { return chars; }
  / CharsetIntroducer? "'" chars:NotSQuote { error("Unmatched single quote"); }
  / '"' chars:NotDQuote '"' _ { return chars; }
  / '"' chars:NotDQuote { error("Unmatched double quote"); }


Interval
  = IntervalToken n:Number unit:Name &{ return intervalUnits[unit] }
    {
      if (n !== 0) error('only zero intervals supported for now');
      return 0;
    }


/* Tokens */

NullToken          = "NULL"i           !IdentifierPart _ { return NULL; }
TrueToken          = "TRUE"i           !IdentifierPart _ { return true; }
FalseToken         = "FALSE"i          !IdentifierPart _ { return false; }

SelectToken        = "SELECT"i         !IdentifierPart _ { return 'SELECT'; }
DescribeToken      = ("DESCRIBE"i / "EXPLAIN"i) !IdentifierPart _ { return 'DESCRIBE'; }
ShowToken          = "SHOW"i           !IdentifierPart _ { return 'SHOW'; }
SetToken           = "SET"i            !IdentifierPart _ { return 'SET'; }
UseToken           = "USE"i            !IdentifierPart _ { return 'USE'; }

VariablesToken     = "VARIABLES"i      !IdentifierPart _ { return 'VARIABLES'; }
StatusToken        = "STATUS"i         !IdentifierPart _ { return 'STATUS'; }
DatabasesToken     = "DATABASES"i      !IdentifierPart _
SchemasToken       = "SCHEMAS"i        !IdentifierPart _
ColumnsToken       = "COLUMNS"i        !IdentifierPart _
FullToken          = "FULL"i           !IdentifierPart _
TablesToken        = "TABLES"i         !IdentifierPart _
GlobalToken        = "GLOBAL"i         !IdentifierPart _
SessionToken       = "SESSION"i        !IdentifierPart _
CharacterToken     = "CHARACTER"i      !IdentifierPart _
CollationToken     = "COLLATION"i      !IdentifierPart _
IndexToken         = "INDEX"i          !IdentifierPart _
WarningsToken      = "WARNINGS"i       !IdentifierPart _

FromToken          = "FROM"i           !IdentifierPart _
AsToken            = "AS"i             !IdentifierPart _
OnToken            = "ON"i             !IdentifierPart _
LeftToken          = "LEFT"i           !IdentifierPart _
InnerToken         = "INNER"i          !IdentifierPart _
JoinToken          = "JOIN"i           !IdentifierPart _
UnionToken         = "UNION"i          !IdentifierPart _
WhereToken         = "WHERE"i          !IdentifierPart _
GroupToken         = "GROUP"i          !IdentifierPart _
ByToken            = "BY"i             !IdentifierPart _
OrderToken         = "ORDER"i          !IdentifierPart _
HavingToken        = "HAVING"i         !IdentifierPart _
LimitToken         = "LIMIT"i          !IdentifierPart _

AscToken           = "ASC"i            !IdentifierPart _ { return SortExpression.ASCENDING;  }
DescToken          = "DESC"i           !IdentifierPart _ { return SortExpression.DESCENDING; }

BetweenToken       = "BETWEEN"i        !IdentifierPart _
InToken            = "IN"i             !IdentifierPart _
IsToken            = "IS"i             !IdentifierPart _
LikeToken          = "LIKE"i           !IdentifierPart _
ContainsToken      = "CONTAINS"i       !IdentifierPart _
RegExpToken        = "REGEXP"i         !IdentifierPart _
EscapeToken        = "ESCAPE"i         !IdentifierPart _

NotToken           = "NOT"i            !IdentifierPart _
AndToken           = "AND"i            !IdentifierPart _
OrToken            = "OR"i             !IdentifierPart _

DistinctToken      = "DISTINCT"i       !IdentifierPart _
StarToken          = "*"               !IdentifierPart _ { return '*'; }

CountToken         = "COUNT"i          !IdentifierPart _ { return 'count'; }
CountDistinctToken = "COUNT_DISTINCT"i !IdentifierPart _ { return 'countDistinct'; }
SumToken           = "SUM"i            !IdentifierPart _ { return 'sum'; }
AvgToken           = "AVG"i            !IdentifierPart _ { return 'average'; }
MinToken           = "MIN"i            !IdentifierPart _ { return 'min'; }
MaxToken           = "MAX"i            !IdentifierPart _ { return 'max'; }
QuantileToken      = "QUANTILE"i       !IdentifierPart _ { return 'quantile'; }
CustomToken        = "CUSTOM"i         !IdentifierPart _ { return 'customAggregate'; }
CustomAggregateToken = "CUSTOM_AGGREGATE"i  !IdentifierPart _ { return 'customAggregate'; }

CaseToken          = "CASE"i           !IdentifierPart _
WhenToken          = "WHEN"i           !IdentifierPart _
ThenToken          = "THEN"i           !IdentifierPart _
ElseToken          = "ELSE"i           !IdentifierPart _
EndToken           = "END"i            !IdentifierPart _

DateToken          = "DATE"i           !IdentifierPart _ { return 'd'; }
TimeToken          = "TIME"i           !IdentifierPart _ { return 't'; }
TimestampToken     = "TIMESTAMP"i      !IdentifierPart _ { return 'ts'; }
DToken             = "D"i              !IdentifierPart _ { return 'd'; }
TToken             = "T"i              !IdentifierPart _ { return 't'; }
TsToken            = "TS"i             !IdentifierPart _ { return 'ts'; }

IntervalToken      = "INTERVAL"i       !IdentifierPart _

IdentifierPart = [a-z_]i

/* Numbers */

Number "Number"
  = n:$(Int Fraction? Exp?) _ { return parseFloat(n); }

Int
  = $("-"? [1-9] Digits)
  / $("-"? Digit)

Fraction
  = $("." Digits)

Exp
  = $("e"i [+-]? Digits)

Digits
  = $ Digit+

Digit
  = [0-9]


/* Extra */

OpenParen "("
  = "(" _

CloseParen ")"
  = ")" _

OpenCurly "{"
  = "{" _

CloseCurly "}"
  = "}" _

Comma
  = "," _

Dot
  = "." _

Name "Name"
  = name:$([a-z_]i [a-z0-9_]i*) _ { return name; }

RelaxedName "RelaxedName"
  = name:$([a-z_\-:*/]i [a-z0-9_\-:*/]i*) _ { return name; }

CharsetIntroducer
  = "N"
  / "n"
  / "_"$([a-z0-9]+) // assume all charsets are a combo of lowercase + number

NotSQuote "NotSQuote"
  = $([^']*)

NotDQuote "NotDQuote"
  = $([^"]*)

_ "Whitespace"
  = $ ([ \t\r\n] / SingleLineComment / InlineComment)*

InlineComment
  = "/*" (!CommentTerminator .)* CommentTerminator

SingleLineComment
  = ("-- " / "#") (!LineTerminator .)*

CommentTerminator
  = "*/"

LineTerminator
  = [\n\r]

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

import { Duration, parseISODate, Timezone } from "@topgames/chronoshift";
import * as hasOwnProp from "has-own-prop";
import {
  Instance,
  isImmutableClass,
  generalLookupsEqual,
} from "immutable-class";
import { PassThrough } from "readable-stream";

import {
  failIfIntrospectNeededInDatum,
  getFullTypeFromDatum,
  introspectDatum,
} from "../datatypes/common";
import {
  ComputeFn,
  Dataset,
  DatasetExternalAlterations,
  Datum,
  fillExpressionExternalAlteration,
  sizeOfDatasetExternalAlterations,
  NumberRange,
  PlywoodValue,
  Range,
  Set,
  StringRange,
  TimeRange,
} from "../datatypes/index";
import { iteratorFactory, PlyBit } from "../datatypes/valueStream";

import { SQLDialect } from "../dialect/baseDialect";
import { External, ExternalJS } from "../external/baseExternal";
import { promiseWhile } from "../helper/promiseWhile";
import {
  deduplicateSort,
  formatDateTimeForLog,
  pipeWithError,
  repeat,
  shallowCopy,
} from "../helper/utils";
import {
  DatasetFullType,
  Environment,
  PlyType,
  PlyTypeSimple,
  PlyTypeSingleValue,
} from "../types";

import { AbsoluteExpression } from "./absoluteExpression";
import { AddExpression } from "./addExpression";
import { AndExpression } from "./andExpression";
import { ApplyExpression } from "./applyExpression";
import { AverageExpression } from "./averageExpression";
import { BitwiseAndExpression } from "./bitwiseAndExpression";
import { CardinalityExpression } from "./cardinalityExpression";
import { CastExpression } from "./castExpression";
import { CollectExpression } from "./collectExpression";
import { ConcatExpression } from "./concatExpression";
import { ContainsExpression } from "./containsExpression";
import { CountDistinctExpression } from "./countDistinctExpression";
import { CountExpression } from "./countExpression";
import { CustomAggregateExpression } from "./customAggregateExpression";
import { CustomTransformExpression } from "./customTransformExpression";
import { DivideExpression } from "./divideExpression";
import { ExternalExpression } from "./externalExpression";
import { ExtractExpression } from "./extractExpression";
import { FallbackExpression } from "./fallbackExpression";
import { FilterExpression } from "./filterExpression";
import { GreaterThanExpression } from "./greaterThanExpression";
import { GreaterThanOrEqualExpression } from "./greaterThanOrEqualExpression";
import { IndexOfExpression } from "./indexOfExpression";
import { InExpression } from "./inExpression";
import { IsExpression } from "./isExpression";
import { JoinExpression } from "./joinExpression";
import { LengthExpression } from "./lengthExpression";
import { LessThanExpression } from "./lessThanExpression";
import { LessThanOrEqualExpression } from "./lessThanOrEqualExpression";
import { LimitExpression } from "./limitExpression";
import { LiteralExpression } from "./literalExpression";
import { LookupExpression } from "./lookupExpression";
import { MatchExpression } from "./matchExpression";
import { MaxExpression } from "./maxExpression";
import { MinExpression } from "./minExpression";
import { ModExpression } from "./modExpression";
import { MultiplyExpression } from "./multiplyExpression";
import { NotExpression } from "./notExpression";
import { NumberBucketExpression } from "./numberBucketExpression";
import { OrExpression } from "./orExpression";
import { OverlapExpression } from "./overlapExpression";
import { PowerExpression } from "./powerExpression";
import { LogExpression } from "./logExpression";
import { QuantileExpression } from "./quantileExpression";
import { RefExpression } from "./refExpression";
import { SelectExpression } from "./selectExpression";
import { Direction, SortExpression } from "./sortExpression";
import { SplitExpression } from "./splitExpression";
import { SubstrExpression } from "./substrExpression";
import { SubtractExpression } from "./subtractExpression";
import { SumExpression } from "./sumExpression";
import { ThenExpression } from "./thenExpression";
import { TimeBucketExpression } from "./timeBucketExpression";
import { TimeFloorExpression } from "./timeFloorExpression";
import { TimePartExpression } from "./timePartExpression";
import { TimeRangeExpression } from "./timeRangeExpression";
import { TimeShiftExpression } from "./timeShiftExpression";
import { TransformCaseExpression } from "./transformCaseExpression";

declare const process: any;
export interface ComputeOptions extends Environment {
  customOptions?: any;
  rawQueries?: any[];
  maxQueries?: number;
  maxRows?: number;
  maxComputeCycles?: number;
  concurrentQueryLimit?: number;
  timeout?: number;
  beforePerSplitRequestFn?: (external: External) => void;
  afterSplitRequestFn?: (queriesMade: number) => void;
}

export interface AlterationFillerPromise {
  (external: External, terminal: boolean): Promise<any>;
}

function fillExpressionExternalAlterationAsync(
  alteration: ExpressionExternalAlteration,
  filler: AlterationFillerPromise
): Promise<ExpressionExternalAlteration> {
  let tasks: Promise<any>[] = [];
  fillExpressionExternalAlteration(alteration, (external, terminal) => {
    tasks.push(filler(external, terminal));
    return null;
  });

  return Promise.all(tasks).then((results) => {
    let i = 0;
    fillExpressionExternalAlteration(alteration, () => {
      let res = results[i];
      i++;
      return res;
    });
    return alteration;
  });
}

export interface ExpressionExternalAlterationSimple {
  external: External;
  terminal?: boolean;
  result?: any;
}

export type ExpressionExternalAlteration = Record<
  string,
  ExpressionExternalAlterationSimple | DatasetExternalAlterations
>;

export interface BooleanExpressionIterator {
  (ex: Expression, index: int, depth: int, nestDiff: int): boolean | null;
}

export interface VoidExpressionIterator {
  (ex: Expression, index: int, depth: int, nestDiff: int): void;
}

export interface SubstitutionFn {
  (
    ex: Expression,
    index: int,
    depth: int,
    nestDiff: int,
    typeContext: DatasetFullType
  ): Expression | null;
}

export interface ExpressionMatchFn {
  (ex: Expression): boolean;
}

export interface DatasetBreakdown {
  singleDatasetActions: ApplyExpression[];
  combineExpression: Expression;
}

export interface Indexer {
  index: int;
}

export interface ExpressionTypeContext {
  expression: Expression;
  typeContext: DatasetFullType;
}

export type Alterations = Record<string, Expression>;

export interface SQLParse {
  verb: string;
  rewrite?: string;
  expression?: Expression;
  table?: string;
  database?: string;
  rest?: string;
}

export interface Splits {
  [name: string]: Expression;
}

export interface SplitsJS {
  [name: string]: ExpressionJS;
}

export type CaseType = "upperCase" | "lowerCase";

export interface ExpressionValue {
  op?: string;
  type?: PlyType;
  simple?: boolean;
  options?: Record<string, any>;
  operand?: Expression;
  value?: any;
  name?: string;
  nest?: int;
  external?: External;
  expression?: Expression;
  actions?: any[]; // ToDo remove
  ignoreCase?: boolean;

  dataName?: string;
  splits?: Splits;
  direction?: Direction;
  size?: number;
  offset?: number;
  duration?: Duration;
  timezone?: Timezone;
  part?: string;
  step?: number;
  position?: int;
  len?: int;
  regexp?: string;
  custom?: string;
  compare?: string;
  lookupFn?: string;
  attributes?: string[];
  transformType?: CaseType;
  outputType?: PlyTypeSimple;
  tuning?: string;
}

export interface ExpressionJS {
  op?: string;
  type?: PlyType;
  options?: Record<string, any>;
  value?: any;
  operand?: ExpressionJS;
  name?: string;
  nest?: int;
  external?: ExternalJS;
  expression?: ExpressionJS;
  action?: any;
  actions?: any[]; // ToDo: remove
  ignoreCase?: boolean;

  dataName?: string;
  splits?: SplitsJS;
  direction?: Direction;
  size?: number;
  offset?: number;
  duration?: string;
  timezone?: string;
  part?: string;
  step?: number;
  position?: int;
  len?: int;
  regexp?: string;
  custom?: string;
  compare?: string;
  lookupFn?: string;
  attributes?: string[];
  transformType?: CaseType;
  outputType?: PlyTypeSimple;
  tuning?: string;
}

export interface ExtractAndRest {
  extract: Expression;
  rest: Expression;
}

export type IfNotFound = "throw" | "leave" | "null";

function runtimeAbstract() {
  return new Error("must be implemented");
}

function getDataName(ex: Expression): string {
  if (ex instanceof RefExpression) {
    return ex.name;
  } else if (ex instanceof ChainableExpression) {
    return getDataName(ex.operand);
  } else {
    return null;
  }
}

function getValue(param: any): any {
  if (param instanceof LiteralExpression) return param.value;
  return param;
}

function getString(param: string | Expression): string {
  if (typeof param === "string") return param;
  if (param instanceof LiteralExpression && param.type === "STRING") {
    return param.value;
  }
  if (param instanceof RefExpression && param.nest === 0) {
    return param.name;
  }
  throw new Error("could not extract a string out of " + String(param));
}

function getNumber(param: number | Expression): number {
  if (typeof param === "number") return param;
  if (param instanceof LiteralExpression && param.type === "NUMBER") {
    return param.value;
  }
  throw new Error("could not extract a number out of " + String(param));
}

// -----------------------------

/**
 * The expression starter function. It produces a native dataset with a singleton empty datum inside of it.
 * This is useful to describe the base container
 */
export function ply(dataset?: Dataset): LiteralExpression {
  if (!dataset) {
    dataset = new Dataset({
      keys: [],
      data: [{}],
    });
  }
  return r(dataset);
}

/**
 * $('blah') produces an reference lookup expression on 'blah'
 *
 * @param name The name of the column
 * @param nest (optional) the amount of nesting to add default: 0
 * @param type (optional) force the type of the reference
 */
export function $(name: string, nest?: number, type?: PlyType): RefExpression;
export function $(name: string, type?: PlyType): RefExpression;
export function $(name: string, nest?: any, type?: PlyType): RefExpression {
  if (typeof name !== "string")
    throw new TypeError("$() argument must be a string");
  if (typeof nest === "string") {
    type = nest as PlyType;
    nest = 0;
  }
  return new RefExpression({
    name,
    nest: nest != null ? nest : 0,
    type,
  });
}

export function i$(name: string, nest?: number, type?: PlyType): RefExpression {
  if (typeof name !== "string")
    throw new TypeError("$() argument must be a string");
  if (typeof nest === "string") {
    type = nest as PlyType;
    nest = 0;
  }

  return new RefExpression({
    name,
    nest: nest != null ? nest : 0,
    type,
    ignoreCase: true,
  });
}

export function r(value: any): LiteralExpression {
  if (value instanceof External)
    throw new TypeError("r() can not accept externals");
  if (Array.isArray(value)) value = Set.fromJS(value);
  return LiteralExpression.fromJS({ op: "literal", value: value });
}

export function toJS(thing: any): any {
  return thing && typeof thing.toJS === "function" ? thing.toJS() : thing;
}

function chainVia(
  op: string,
  expressions: Expression[],
  zero: Expression
): Expression {
  let n = expressions.length;
  if (!n) return zero;
  let acc = expressions[0];
  if (!(acc instanceof Expression)) acc = Expression.fromJSLoose(acc);
  for (let i = 1; i < n; i++) acc = (<any>acc)[op](expressions[i]);
  return acc;
}

export interface PEGParserOptions {
  cache?: boolean;
  allowedStartRules?: string;
  output?: string;
  optimize?: string;
  plugins?: any;
  [key: string]: any;
}

export interface PEGParser {
  parse: (str: string, options?: PEGParserOptions) => any;
}

/**
 * Provides a way to express arithmetic operations, aggregations and database operators.
 * This class is the backbone of plywood
 */
export abstract class Expression
  implements Instance<ExpressionValue, ExpressionJS>
{
  static MAX_SAFE_INTEGER: LiteralExpression;
  static NULL: LiteralExpression;
  static ZERO: LiteralExpression;
  static ONE: LiteralExpression;
  static FALSE: LiteralExpression;
  static TRUE: LiteralExpression;
  static EMPTY_STRING: LiteralExpression;
  static EMPTY_SET: LiteralExpression;

  static _: RefExpression;

  static expressionParser: PEGParser;
  static plyqlParser: PEGParser;
  static defaultParserTimezone: Timezone = Timezone.UTC; // The default timezone within which dates in expressions are parsed

  static isExpression(candidate: any): candidate is Expression {
    return candidate instanceof Expression;
  }

  static expressionLookupFromJS(
    expressionJSs: Record<string, ExpressionJS>
  ): Record<string, Expression> {
    let expressions: Record<string, Expression> = Object.create(null);
    for (let name in expressionJSs) {
      if (!hasOwnProp(expressionJSs, name)) continue;
      expressions[name] = Expression.fromJSLoose(expressionJSs[name]);
    }
    return expressions;
  }

  static expressionLookupToJS(
    expressions: Record<string, Expression>
  ): Record<string, ExpressionJS> {
    let expressionsJSs: Record<string, ExpressionJS> = {};
    for (let name in expressions) {
      if (!hasOwnProp(expressions, name)) continue;
      expressionsJSs[name] = expressions[name].toJS();
    }
    return expressionsJSs;
  }

  /**
   * Parses an expression
   * @param str The expression to parse
   * @param timezone The timezone within which to evaluate any untimezoned date strings
   */
  static parse(str: string, timezone?: Timezone): Expression {
    if (str[0] === "{" && str[str.length - 1] === "}") {
      return Expression.fromJS(JSON.parse(str));
    }

    let original = Expression.defaultParserTimezone;
    if (timezone) Expression.defaultParserTimezone = timezone;
    try {
      return Expression.expressionParser.parse(str);
    } catch (e) {
      // Re-throw to add the stacktrace
      throw new Error(`Expression parse error: ${e.message} on '${str}'`);
    } finally {
      Expression.defaultParserTimezone = original;
    }
  }

  /**
   * Parses SQL statements into a plywood expressions
   * @param str The SQL to parse
   * @param timezone The timezone within which to evaluate any untimezoned date strings
   */
  static parseSQL(str: string, timezone?: Timezone): SQLParse {
    let original = Expression.defaultParserTimezone;
    if (timezone) Expression.defaultParserTimezone = timezone;
    try {
      return Expression.plyqlParser.parse(str);
    } catch (e) {
      // Re-throw to add the stacktrace
      throw new Error(`SQL parse error: ${e.message} on '${str}'`);
    } finally {
      Expression.defaultParserTimezone = original;
    }
  }

  /**
   * Deserializes or parses an expression
   * @param param The expression to parse
   */
  static fromJSLoose(param: any): Expression {
    let expressionJS: ExpressionJS;
    // Quick parse simple expressions
    switch (typeof param) {
      case "undefined":
        throw new Error("must have an expression");

      case "object":
        if (param === null) {
          return Expression.NULL;
        } else if (param instanceof Expression) {
          return param;
        } else if (isImmutableClass(param)) {
          if (param.constructor.type) {
            // Must be a datatype
            expressionJS = { op: "literal", value: param };
          } else {
            throw new Error("unknown object"); //ToDo: better error
          }
        } else if (param.op) {
          expressionJS = <ExpressionJS>param;
        } else if (param.toISOString) {
          expressionJS = { op: "literal", value: new Date(param) };
        } else if (Array.isArray(param)) {
          expressionJS = { op: "literal", value: Set.fromJS(param) };
        } else if (hasOwnProp(param, "start") && hasOwnProp(param, "end")) {
          expressionJS = { op: "literal", value: Range.fromJS(param) };
        } else {
          throw new Error("unknown parameter");
        }
        break;

      case "number":
      case "boolean":
        expressionJS = { op: "literal", value: param };
        break;

      case "string":
        return Expression.parse(param);

      default:
        throw new Error("unrecognizable expression");
    }

    return Expression.fromJS(expressionJS);
  }

  static jsNullSafetyUnary(
    inputJS: string,
    ifNotNull: (str: string) => string
  ): string {
    return `(_=${inputJS},(_==null?null:${ifNotNull("_")}))`;
  }

  static jsNullSafetyBinary(
    lhs: string,
    rhs: string,
    combine: (lhs: string, rhs: string) => string,
    lhsCantBeNull?: boolean,
    rhsCantBeNull?: boolean
  ): string {
    if (lhsCantBeNull) {
      if (rhsCantBeNull) {
        return `(${combine(lhs, rhs)})`;
      } else {
        return `(_=${rhs},(_==null)?null:(${combine(lhs, "_")}))`;
      }
    } else {
      if (rhsCantBeNull) {
        return `(_=${lhs},(_==null)?null:(${combine("_", rhs)}))`;
      } else {
        return `(_=${rhs},_2=${lhs},(_==null||_2==null)?null:(${combine(
          "_",
          "_2"
        )})`;
      }
    }
  }

  static parseTuning(tuning: string | null): Record<string, string> {
    if (typeof tuning !== "string") return {};
    const parts = tuning.split(",");
    let parsed: Record<string, string> = {};
    for (let part of parts) {
      let subParts = part.split("=");
      if (subParts.length !== 2)
        throw new Error(`can not parse tuning '${tuning}'`);
      parsed[subParts[0]] = subParts[1];
    }
    return parsed;
  }

  static safeString(str: string): string {
    return /^[a-z]\w+$/i.test(str) ? str : JSON.stringify(str);
  }

  /**
   * Composes the given expressions with an AND
   * @param expressions the array of expressions to compose
   */
  static and(expressions: Expression[]): Expression {
    return chainVia("and", expressions, Expression.TRUE);
  }

  /**
   * Composes the given expressions as E0 or E1 or ... or En
   * @param expressions the array of expressions to compose
   */
  static or(expressions: Expression[]): Expression {
    return chainVia("or", expressions, Expression.FALSE);
  }

  /**
   * Composes the given expressions as E0 + E1+ ... + En
   * @param expressions the array of expressions to compose
   */
  static add(expressions: Expression[]): Expression {
    return chainVia("add", expressions, Expression.ZERO);
  }

  /**
   * Composes the given expressions as E0 - E1- ... - En
   * @param expressions the array of expressions to compose
   */
  static subtract(expressions: Expression[]): Expression {
    return chainVia("subtract", expressions, Expression.ZERO);
  }

  static multiply(expressions: Expression[]): Expression {
    return chainVia("multiply", expressions, Expression.ONE);
  }

  static power(expressions: Expression[]): Expression {
    return chainVia("power", expressions, Expression.ZERO);
  }

  static concat(expressions: Expression[]): Expression {
    return chainVia("concat", expressions, Expression.EMPTY_STRING);
  }

  static classMap: Record<string, typeof Expression> = {};
  static register(ex: typeof Expression): void {
    let op = (<any>ex).op.replace(/^\w/, (s: string) => s.toLowerCase());
    Expression.classMap[op] = ex;
  }

  static getConstructorFor(op: string): typeof Expression {
    const ClassFn = Expression.classMap[op];
    if (!ClassFn) throw new Error(`unsupported expression op '${op}'`);
    return ClassFn;
  }

  static applyMixins(derivedCtor: any, baseCtors: any[]) {
    // From: https://www.typescriptlang.org/docs/handbook/mixins.html
    baseCtors.forEach((baseCtor) => {
      Object.getOwnPropertyNames(baseCtor.prototype).forEach((name) => {
        derivedCtor.prototype[name] = baseCtor.prototype[name];
      });
    });
  }

  static jsToValue(js: ExpressionJS): ExpressionValue {
    return {
      op: js.op,
      type: js.type,
      options: js.options,
    };
  }

  /**
   * Deserializes the expression JSON
   * @param expressionJS
   */
  static fromJS(expressionJS: ExpressionJS): Expression {
    if (!expressionJS) throw new Error("must have expressionJS");
    if (!hasOwnProp(expressionJS, "op")) {
      if (hasOwnProp(expressionJS, "action")) {
        expressionJS = shallowCopy(expressionJS);
        expressionJS.op = expressionJS.action;
        delete expressionJS.action;
        expressionJS.operand = { op: "ref", name: "_" };
      } else {
        throw new Error("op must be defined");
      }
    }

    // Back compat.
    if (expressionJS.op === "custom") {
      expressionJS = shallowCopy(expressionJS);
      expressionJS.op = "customAggregate";
    }

    let op = expressionJS.op;
    if (typeof op !== "string") {
      throw new Error("op must be a string");
    }

    // Back compat.
    if (op === "chain") {
      const actions = expressionJS.actions || [expressionJS.action];
      return Expression.fromJS(expressionJS.expression).performActions(
        actions.map(Expression.fromJS)
      );
    }

    const ClassFn = Expression.getConstructorFor(op);
    return ClassFn.fromJS(expressionJS);
  }

  static fromValue(parameters: ExpressionValue): Expression {
    const { op } = parameters;
    const ClassFn = Expression.getConstructorFor(op) as any;
    return new ClassFn(parameters);
  }

  public op: string;
  public type: PlyType;
  public simple: boolean;
  public options?: Record<string, any>;

  constructor(parameters: ExpressionValue, dummy: any = null) {
    this.op = parameters.op;
    if (dummy !== dummyObject) {
      throw new TypeError(
        "can not call `new Expression` directly use Expression.fromJS instead"
      );
    }
    if (parameters.simple) this.simple = true;
    if (parameters.options) this.options = parameters.options;
  }

  protected _ensureOp(op: string) {
    if (!this.op) {
      this.op = op;
      return;
    }
    if (this.op !== op) {
      throw new TypeError(
        `incorrect expression op '${this.op}' (needs to be: '${op}')`
      );
    }
  }

  public valueOf(): ExpressionValue {
    let value: ExpressionValue = { op: this.op };
    if (this.simple) value.simple = true;
    if (this.options) value.options = this.options;
    return value;
  }

  /**
   * Serializes the expression into a simple JS object that can be passed to JSON.serialize
   */
  public toJS(): ExpressionJS {
    let js: ExpressionJS = { op: this.op };
    if (this.options) js.options = this.options;
    return js;
  }

  /**
   * Makes it safe to call JSON.serialize on expressions
   */
  public toJSON(): ExpressionJS {
    return this.toJS();
  }

  public abstract toString(indent?: int): string;

  /**
   * Validate that two expressions are equal in their meaning
   * @param other
   */
  public equals(other: Expression | undefined): boolean {
    return (
      other instanceof Expression &&
      this.op === other.op &&
      this.type === other.type &&
      generalLookupsEqual(this.options, other.options)
    );
  }

  /**
   * Check that the expression can potentially have the desired type
   * If wanted type is 'SET' then any SET/* type is matched
   * @param wantedType The type that is wanted
   */
  public canHaveType(wantedType: string): boolean {
    let { type } = this;
    if (!type || type === "NULL") return true;
    if (wantedType === "SET") {
      return Set.isSetType(type);
    } else {
      return type === wantedType;
    }
  }

  /**
   * Counts the number of expressions contained within this expression
   */
  public expressionCount(): int {
    return 1;
  }

  /**
   * Check if the expression is of the given operation (op)
   * @param op The operation to test
   */
  public isOp(op: string): boolean {
    return this.op === op;
  }

  public markSimple(): this {
    if (this.simple) return this;
    let value = this.valueOf();
    value.simple = true;
    return Expression.fromValue(value) as any;
  }

  /**
   * Check if the expression contains the given operation (op)
   * @param op The operation to test
   */
  public containsOp(op: string): boolean {
    return this.some((ex: Expression) => ex.isOp(op) || null);
  }

  /**
   * Check if the expression contains externals
   */
  public hasExternal(): boolean {
    return this.some((ex: Expression) => {
      if (ex instanceof ExternalExpression) return true;
      return null; // search further
    });
  }

  public getBaseExternals(): External[] {
    let externals: External[] = [];
    this.forEach((ex: Expression) => {
      if (ex instanceof ExternalExpression)
        externals.push(ex.external.getBase());
    });
    return External.deduplicateExternals(externals);
  }

  public getRawExternals(): External[] {
    let externals: External[] = [];
    this.forEach((ex: Expression) => {
      if (ex instanceof ExternalExpression)
        externals.push(ex.external.getRaw());
    });
    return External.deduplicateExternals(externals);
  }

  public getReadyExternals(limit = Infinity): ExpressionExternalAlteration {
    let indexToSkip: Record<string, boolean> = {};
    let externalsByIndex: ExpressionExternalAlteration = {};

    this.every((ex: Expression, index: int) => {
      if (limit <= 0) return null;

      if (ex instanceof ExternalExpression) {
        if (indexToSkip[index]) return null;
        if (!ex.external.suppress) {
          limit--;
          externalsByIndex[index] = {
            external: ex.external,
            terminal: true,
          };
        }
      } else if (ex instanceof ChainableExpression) {
        let h = ex._headExternal();
        if (h) {
          if (h.allGood) {
            limit--;
            externalsByIndex[index + h.offset] = { external: h.external };
            return true;
          } else {
            indexToSkip[index + h.offset] = true;
            return null;
          }
        }
      } else if (ex instanceof LiteralExpression && ex.type === "DATASET") {
        let datasetExternals = (ex.value as Dataset).getReadyExternals(limit);
        let size = sizeOfDatasetExternalAlterations(datasetExternals);
        if (size) {
          limit -= size;
          externalsByIndex[index] = datasetExternals;
        }
        return null;
      }
      return null;
    });
    return externalsByIndex;
  }

  public applyReadyExternals(
    alterations: ExpressionExternalAlteration
  ): Expression {
    return this.substitute((ex, index) => {
      let alteration = alterations[index];
      if (!alteration) return null;
      if (Array.isArray(alteration)) {
        return r(
          (ex.getLiteralValue() as Dataset).applyReadyExternals(alteration)
        );
      } else {
        return r(alteration.result);
      }
    }).simplify();
  }

  private _headExternal(): any {
    let ex: Expression = this;
    let allGood = true;
    let offset = 0;
    while (ex instanceof ChainableExpression) {
      allGood =
        allGood &&
        (ex.op === "filter"
          ? ex.argumentsResolvedWithoutExternals()
          : ex.argumentsResolved());
      ex = ex.operand;
      offset++;
    }

    if (ex instanceof ExternalExpression) {
      return {
        allGood,
        external: ex.external,
        offset,
      };
    } else {
      return null;
    }
  }

  public getHeadOperand(): Expression {
    return this;
  }

  /**
   * Retrieve all free references by name
   * returns the alphabetically sorted list of the references
   */
  public getFreeReferences(): string[] {
    let freeReferences: string[] = [];
    this.forEach((ex: Expression, index: int, depth: int, nestDiff: int) => {
      if (ex instanceof RefExpression && nestDiff <= ex.nest) {
        freeReferences.push(repeat("^", ex.nest - nestDiff) + ex.name);
      }
    });
    return deduplicateSort(freeReferences);
  }

  /**
   * Retrieve all free references by index in the query
   */
  public getFreeReferenceIndexes(): number[] {
    let freeReferenceIndexes: number[] = [];
    this.forEach((ex: Expression, index: int, depth: int, nestDiff: int) => {
      if (ex instanceof RefExpression && nestDiff <= ex.nest) {
        freeReferenceIndexes.push(index);
      }
    });
    return freeReferenceIndexes;
  }

  /**
   * Increment the ^ nesting on all the free reference variables within this expression
   * @param by The number of generation to increment by (default: 1)
   */
  public incrementNesting(by: int = 1): Expression {
    let freeReferenceIndexes = this.getFreeReferenceIndexes();
    if (freeReferenceIndexes.length === 0) return this;
    return this.substitute((ex: Expression, index: int) => {
      if (
        ex instanceof RefExpression &&
        freeReferenceIndexes.indexOf(index) !== -1
      ) {
        return ex.incrementNesting(by);
      }
      return null;
    });
  }

  /**
   * Returns an expression that is equivalent but no more complex
   * If no simplification can be done will return itself.
   */
  public simplify(): Expression {
    return this;
  }

  /**
   * Runs iter over all the sub expression and return true if iter returns true for everything
   * @param iter The function to run
   * @param thisArg The this for the substitution function
   */
  public every(iter: BooleanExpressionIterator, thisArg?: any): boolean {
    return this._everyHelper(iter, thisArg, { index: 0 }, 0, 0);
  }

  public _everyHelper(
    iter: BooleanExpressionIterator,
    thisArg: any,
    indexer: Indexer,
    depth: int,
    nestDiff: int
  ): boolean {
    let pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
    if (pass != null) {
      return pass;
    } else {
      indexer.index++;
    }
    return true;
  }

  /**
   * Runs iter over all the sub expression and return true if iter returns true for anything
   * @param iter The function to run
   * @param thisArg The this for the substitution function
   */
  public some(iter: BooleanExpressionIterator, thisArg?: any): boolean {
    return !this.every(
      (ex: Expression, index: int, depth: int, nestDiff: int) => {
        let v = iter.call(this, ex, index, depth, nestDiff);
        return v == null ? null : !v;
      },
      thisArg
    );
  }

  /**
   * Runs iter over all the sub expressions
   * @param iter The function to run
   * @param thisArg The this for the substitution function
   */
  public forEach(iter: VoidExpressionIterator, thisArg?: any): void {
    this.every((ex: Expression, index: int, depth: int, nestDiff: int) => {
      iter.call(this, ex, index, depth, nestDiff);
      return null;
    }, thisArg);
  }

  /**
   * Performs a substitution by recursively applying the given substitutionFn to every sub-expression
   * if substitutionFn returns an expression than it is replaced; if null is returned this expression is returned
   * @param substitutionFn The function with which to substitute
   */
  public substitute(
    substitutionFn: SubstitutionFn,
    typeContext: DatasetFullType = null
  ): Expression {
    return this._substituteHelper(
      substitutionFn,
      { index: 0 },
      0,
      0,
      typeContext
    ).expression;
  }

  public _substituteHelper(
    substitutionFn: SubstitutionFn,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
    typeContext: DatasetFullType
  ): ExpressionTypeContext {
    let sub = substitutionFn.call(
      this,
      this,
      indexer.index,
      depth,
      nestDiff,
      typeContext
    );
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext),
      };
    } else {
      indexer.index++;
    }

    return {
      expression: this,
      typeContext: this.updateTypeContextIfNeeded(typeContext),
    };
  }

  public abstract getFn(): ComputeFn;

  public fullyDefined(): boolean {
    return true;
  }

  public abstract calc(datum: Datum): PlywoodValue;

  public abstract getJS(datumVar: string): string;

  public getJSFn(datumVar = "d[]"): string {
    const { type } = this;
    let jsEx = this.getJS(datumVar);
    let body: string;
    if (type === "NUMBER" || type === "NUMBER_RANGE" || type === "TIME") {
      body = `_=${jsEx};return isNaN(_)?null:_`;
    } else {
      body = `return ${jsEx};`;
    }
    return `function(${datumVar.replace("[]", "")}){var _,_2;${body}}`;
  }

  public abstract getSQL(dialect: SQLDialect): string;

  public extractFromAnd(matchFn: ExpressionMatchFn): ExtractAndRest {
    if (this.type !== "BOOLEAN") return null;
    if (matchFn(this)) {
      return {
        extract: this,
        rest: Expression.TRUE,
      };
    } else {
      return {
        extract: Expression.TRUE,
        rest: this,
      };
    }
  }

  public breakdownByDataset(tempNamePrefix = "b"): DatasetBreakdown {
    throw new Error("ToDo");
    // let nameIndex = 0;
    // let singleDatasetActions: ApplyExpression[] = [];
    //
    // let externals = this.getBaseExternals();
    // if (externals.length < 2) {
    //   throw new Error('not a multiple dataset expression');
    // }
    //
    // const combine = this.substitute(ex => {
    //   let externals = ex.getBaseExternals();
    //   if (externals.length !== 1) return null;
    //
    //   let existingApply = SimpleArray.find(singleDatasetActions, (apply) => apply.expression.equals(ex));
    //
    //   let tempName: string;
    //   if (existingApply) {
    //     tempName = existingApply.name;
    //   } else {
    //     tempName = tempNamePrefix + (nameIndex++);
    //     singleDatasetActions.push(Expression._.apply(tempName, ex));
    //   }
    //
    //   return $(tempName);
    // });
    //
    // return {
    //   singleDatasetActions: singleDatasetActions,
    //   combineExpression: combine
    // };
  }

  public getLiteralValue(): any {
    return null;
  }

  public upgradeToType(targetType: PlyType): Expression {
    return this;
  }

  public performAction(action: Expression): Expression {
    return action.substitute((ex) => (ex.equals(Expression._) ? this : null));
  }

  public performActions(actions: Expression[]): Expression {
    let ex: Expression = this;
    for (let action of actions) ex = ex.performAction(action);
    return ex;
  }

  public getOptions(): Record<string, any> {
    return this.options || {};
  }

  public setOptions(options: Record<string, any> | null): this {
    let value = this.valueOf();
    value.options = options;
    return Expression.fromValue(value) as any;
  }

  public setOption(optionKey: string, optionValue: any): this {
    const newOptions = Object.assign({}, this.getOptions());
    newOptions[optionKey] = optionValue;
    return this.setOptions(newOptions);
  }

  // ------------------------------------------------------------------------
  // API behaviour

  private _mkChain<T extends ChainableUnaryExpression>(
    ExpressionClass: any,
    exs: any[]
  ): T {
    let cur: any = this;
    for (let ex of exs) {
      cur = new ExpressionClass({
        operand: cur,
        expression: ex instanceof Expression ? ex : Expression.fromJSLoose(ex),
      });
    }
    return cur;
  }

  // Basic arithmetic

  public add(...exs: any[]) {
    return this._mkChain<AddExpression>(AddExpression, exs);
  }

  public subtract(...exs: any[]) {
    return this._mkChain<SubtractExpression>(SubtractExpression, exs);
  }

  public negate() {
    return Expression.ZERO.subtract(this);
  }

  public multiply(...exs: any[]) {
    return this._mkChain<MultiplyExpression>(MultiplyExpression, exs);
  }

  public divide(...exs: any[]) {
    return this._mkChain<DivideExpression>(DivideExpression, exs);
  }

  public mod(...exs: any[]) {
    return this._mkChain<ModExpression>(ModExpression, exs);
  }

  public bitwiseAnd(...exs: any[]) {
    return this._mkChain<BitwiseAndExpression>(BitwiseAndExpression, exs);
  }

  public reciprocate() {
    return Expression.ONE.divide(this);
  }

  public sqrt() {
    return this.power(0.5);
  }

  public power(...exs: any[]) {
    return this._mkChain<PowerExpression>(PowerExpression, exs);
  }

  public log(ex: any = Math.E) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new LogExpression({ operand: this, expression: ex });
  }

  public ln(): LogExpression {
    return new LogExpression({ operand: this, expression: r(Math.E) });
  }

  // Control flow

  public then(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new ThenExpression({ operand: this, expression: ex });
  }

  public fallback(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new FallbackExpression({ operand: this, expression: ex });
  }

  // Boolean predicates

  public is(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new IsExpression({ operand: this, expression: ex });
  }

  public isnt(ex: any) {
    return this.is(ex).not();
  }

  public lessThan(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new LessThanExpression({ operand: this, expression: ex });
  }

  public lessThanOrEqual(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new LessThanOrEqualExpression({ operand: this, expression: ex });
  }

  public greaterThan(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new GreaterThanExpression({ operand: this, expression: ex });
  }

  public greaterThanOrEqual(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new GreaterThanOrEqualExpression({ operand: this, expression: ex });
  }

  public contains(ex: any, compare?: string) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    if (compare) compare = getString(compare);
    return new ContainsExpression({ operand: this, expression: ex, compare });
  }

  public match(re: string) {
    return new MatchExpression({ operand: this, regexp: getString(re) });
  }

  public in(ex: any): InExpression {
    if (arguments.length === 2) {
      // Back Compat
      return this.overlap(ex, arguments[1]) as any;
    }

    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);

    // Back Compat
    if (Range.isRangeType(ex.type)) {
      return new OverlapExpression({ operand: this, expression: ex }) as any;
    }

    return new InExpression({ operand: this, expression: ex });
  }

  public overlap(ex: any, snd?: Date | number | string) {
    if (arguments.length === 2) {
      ex = getValue(ex);
      snd = getValue(snd);

      if (typeof ex === "string") {
        let parse = parseISODate(ex, Expression.defaultParserTimezone);
        if (parse) ex = parse;
      }

      if (typeof snd === "string") {
        let parse = parseISODate(snd, Expression.defaultParserTimezone);
        if (parse) snd = parse;
      }

      if (typeof ex === "number" && typeof snd === "number") {
        ex = new NumberRange({ start: ex, end: snd });
      } else if (ex.toISOString && (snd as Date).toISOString) {
        ex = new TimeRange({ start: ex, end: snd as Date });
      } else if (typeof ex === "string" && typeof snd === "string") {
        ex = new StringRange({ start: ex, end: snd });
      } else {
        throw new Error("uninterpretable IN parameters");
      }
    }

    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new OverlapExpression({ operand: this, expression: ex });
  }

  public not() {
    return new NotExpression({ operand: this });
  }

  public and(...exs: any[]) {
    return this._mkChain<AndExpression>(AndExpression, exs);
  }

  public or(...exs: any[]) {
    return this._mkChain<OrExpression>(OrExpression, exs);
  }

  // String manipulation

  public substr(position: number, len: number) {
    return new SubstrExpression({
      operand: this,
      position: getNumber(position),
      len: getNumber(len),
    });
  }

  public extract(re: string) {
    return new ExtractExpression({ operand: this, regexp: getString(re) });
  }

  public concat(...exs: any[]) {
    return this._mkChain<ConcatExpression>(ConcatExpression, exs);
  }

  public lookup(lookupFn: string) {
    return new LookupExpression({
      operand: this,
      lookupFn: getString(lookupFn),
    });
  }

  public indexOf(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new IndexOfExpression({ operand: this, expression: ex });
  }

  public transformCase(transformType: CaseType) {
    return new TransformCaseExpression({
      operand: this,
      transformType: getString(transformType) as CaseType,
    });
  }

  public customTransform(custom: string, outputType?: PlyTypeSingleValue) {
    if (!custom)
      throw new Error(
        "Must provide an extraction function name for custom transform"
      );
    outputType =
      outputType !== undefined
        ? (getString(outputType) as PlyTypeSingleValue)
        : null;
    return new CustomTransformExpression({
      operand: this,
      custom: getString(custom),
      outputType,
    });
  }

  // Number manipulation

  public numberBucket(size: number, offset = 0) {
    return new NumberBucketExpression({
      operand: this,
      size: getNumber(size),
      offset: getNumber(offset),
    });
  }

  public absolute() {
    return new AbsoluteExpression({ operand: this });
  }

  public length() {
    return new LengthExpression({ operand: this });
  }

  // Time manipulation

  public timeBucket(duration: any, timezone?: any) {
    if (!(duration instanceof Duration))
      duration = Duration.fromJS(getString(duration));
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimeBucketExpression({ operand: this, duration, timezone });
  }

  public timeFloor(duration: any, timezone?: any) {
    if (!(duration instanceof Duration))
      duration = Duration.fromJS(getString(duration));
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimeFloorExpression({ operand: this, duration, timezone });
  }

  public timeShift(duration: any, step?: number, timezone?: any) {
    if (!(duration instanceof Duration))
      duration = Duration.fromJS(getString(duration));
    step = typeof step !== "undefined" ? getNumber(step) : null;
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimeShiftExpression({ operand: this, duration, step, timezone });
  }

  public timeRange(duration: any, step?: number, timezone?: any) {
    if (!(duration instanceof Duration))
      duration = Duration.fromJS(getString(duration));
    step = typeof step !== "undefined" ? getNumber(step) : null;
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimeRangeExpression({ operand: this, duration, step, timezone });
  }

  public timePart(part: string, timezone?: any) {
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimePartExpression({
      operand: this,
      part: getString(part),
      timezone,
    });
  }

  public cast(outputType: PlyType) {
    return new CastExpression({
      operand: this,
      outputType: getString(outputType) as PlyTypeSimple,
    });
  }

  // Set operations

  public cardinality() {
    return new CardinalityExpression({ operand: this });
  }

  // Split Apply Combine based transformations

  /**
   * Filter the dataset with a boolean expression
   * Only works on expressions that return DATASET
   * @param ex A boolean expression to filter on
   */
  public filter(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new FilterExpression({ operand: this, expression: ex });
  }

  public split(splits: any, dataName?: string): SplitExpression;
  public split(ex: any, name: string, dataName?: string): SplitExpression;
  public split(splits: any, name?: string, dataName?: string): SplitExpression {
    // Determine if use case #2 (ex + name)
    if (
      arguments.length === 3 ||
      ((arguments.length === 2 || arguments.length === 1) &&
        (typeof splits === "string" || typeof splits.op === "string"))
    ) {
      name = arguments.length === 1 ? "split" : getString(name);
      let realSplits = Object.create(null);
      realSplits[name] = splits;
      splits = realSplits;
    } else {
      dataName = name;
    }

    let parsedSplits: Splits = Object.create(null);
    for (let k in splits) {
      if (!hasOwnProp(splits, k)) continue;
      let ex = splits[k];
      parsedSplits[k] =
        ex instanceof Expression ? ex : Expression.fromJSLoose(ex);
    }

    dataName = dataName ? getString(dataName) : getDataName(this);
    if (!dataName)
      throw new Error(
        "could not guess data name in `split`, please provide one explicitly"
      );
    return new SplitExpression({
      operand: this,
      splits: parsedSplits,
      dataName: dataName,
    });
  }

  /**
   * Evaluate some expression on every datum in the dataset. Record the result as `name`
   * @param name The name of where to store the results
   * @param ex The expression to evaluate
   */
  public apply(name: string, ex: any) {
    if (arguments.length < 2)
      throw new Error(
        "invalid arguments to .apply, did you forget to specify a name?"
      );
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new ApplyExpression({
      operand: this,
      name: getString(name),
      expression: ex,
    });
  }

  public sort(ex: any, direction?: Direction) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new SortExpression({
      operand: this,
      expression: ex,
      direction: direction ? (getString(direction) as Direction) : null,
    });
  }

  public limit(value: number) {
    return new LimitExpression({ operand: this, value: getNumber(value) });
  }

  public select(attributes: string[]): SelectExpression;
  public select(...attributes: any[]): SelectExpression {
    attributes =
      attributes.length === 1 && Array.isArray(attributes[0])
        ? attributes[0]
        : attributes.map(getString);
    return new SelectExpression({ operand: this, attributes });
  }

  // Aggregate expressions

  public count() {
    if (arguments.length)
      throw new Error(
        ".count() should not have arguments, did you want to .filter().count() ?"
      );
    return new CountExpression({ operand: this });
  }

  public sum(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new SumExpression({ operand: this, expression: ex });
  }

  public min(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new MinExpression({ operand: this, expression: ex });
  }

  public max(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new MaxExpression({ operand: this, expression: ex });
  }

  public average(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new AverageExpression({ operand: this, expression: ex });
  }

  public countDistinct(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new CountDistinctExpression({ operand: this, expression: ex });
  }

  public quantile(ex: any, value: number, tuning?: string) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new QuantileExpression({
      operand: this,
      expression: ex,
      value: getNumber(value),
      tuning: tuning ? getString(tuning) : null,
    });
  }

  public collect(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new CollectExpression({ operand: this, expression: ex });
  }

  public customAggregate(custom: string) {
    return new CustomAggregateExpression({
      operand: this,
      custom: getString(custom),
    });
  }

  // Undocumented (for now)

  public join(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new JoinExpression({ operand: this, expression: ex });
  }

  public needsEnvironment(): boolean {
    return false;
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param environment The environment that will be defined
   */
  public defineEnvironment(environment: Environment): Expression {
    if (!environment.timezone) environment = { timezone: Timezone.UTC };

    // Allow strings as well
    if (typeof environment.timezone === "string")
      environment = { timezone: Timezone.fromJS(environment.timezone as any) };

    return this.substitute((ex) => {
      if (ex.needsEnvironment()) {
        return ex.defineEnvironment(environment);
      }
      return null;
    });
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param context The datum within which the check is happening
   */
  public referenceCheck(context: Datum): Expression {
    return this.changeInTypeContext(getFullTypeFromDatum(context));
  }

  /**
   * Check if the expression is defined in the given type context
   * @param typeContext The FullType context within which to resolve
   */
  public definedInTypeContext(typeContext: DatasetFullType): boolean {
    try {
      this.changeInTypeContext(typeContext);
    } catch (e) {
      return false;
    }
    return true;
  }

  /**
   * Check if the expression is defined in the given type context
   * @param typeContext The FullType context within which to resolve
   * @deprecated
   */
  public referenceCheckInTypeContext(typeContext: DatasetFullType): Expression {
    console.warn(
      `referenceCheckInTypeContext is deprecated, use changeInTypeContext instead`
    );
    return this.changeInTypeContext(typeContext);
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param typeContext The FullType context within which to resolve
   */
  public changeInTypeContext(typeContext: DatasetFullType): Expression {
    return this.substitute(
      (
        ex: Expression,
        index: int,
        depth: int,
        nestDiff: int,
        typeContext: DatasetFullType
      ) => {
        if (ex instanceof RefExpression) {
          return ex.changeInTypeContext(typeContext);
        }
        return null;
      },
      typeContext
    );
  }

  public updateTypeContext(
    typeContext: DatasetFullType,
    extra?: any
  ): DatasetFullType {
    return typeContext;
  }

  public updateTypeContextIfNeeded(
    typeContext: DatasetFullType | null,
    extra?: any
  ): DatasetFullType | null {
    return typeContext ? this.updateTypeContext(typeContext, extra) : null;
  }

  /**
   * Resolves one level of dependencies that refer outside of this expression.
   * @param context The context containing the values to resolve to
   * @param ifNotFound If the reference is not in the context what to do? "throw", "leave", "null"
   * @return The resolved expression
   */
  public resolve(context: Datum, ifNotFound: IfNotFound = "throw"): Expression {
    let expressions: Record<string, Expression> = Object.create(null);
    for (let k in context) {
      if (!hasOwnProp(context, k)) continue;
      let value = context[k];
      if (value instanceof External) {
        expressions[k] = new ExternalExpression({ external: <External>value });
      } else if (value instanceof Expression) {
        expressions[k] = value;
      } else {
        expressions[k] = new LiteralExpression({ value });
      }
    }

    return this.resolveWithExpressions(expressions, ifNotFound);
  }

  public resolveWithExpressions(
    expressions: Record<string, Expression>,
    ifNotFound: IfNotFound = "throw"
  ): Expression {
    return this.substitute(
      (ex: Expression, index: int, depth: int, nestDiff: int) => {
        if (ex instanceof RefExpression) {
          const { nest, ignoreCase, name } = ex;
          if (nestDiff === nest) {
            let foundExpression: Expression = null;
            let valueFound = false;
            let property = ignoreCase
              ? RefExpression.findPropertyCI(expressions, name)
              : RefExpression.findProperty(expressions, name);
            if (property != null) {
              foundExpression = expressions[property];
              valueFound = true;
            }

            if (foundExpression instanceof ExternalExpression) {
              let mode = foundExpression.external.mode;

              // Never substitute split externals at all
              if (mode === "split") {
                return ex;
              }

              // Never substitute non-raw externals from an outside nesting
              if (nest > 0 && mode !== "raw") {
                return ex;
              }
            }

            if (valueFound) {
              return foundExpression;
            } else if (ifNotFound === "throw") {
              throw new Error(
                `could not resolve ${ex} because is was not in the context`
              );
            } else if (ifNotFound === "null") {
              return Expression.NULL;
            } else if (ifNotFound === "leave") {
              return ex;
            }
          } else if (nestDiff < nest) {
            throw new Error(`went too deep during resolve on: ${ex}`);
          }
        }
        return null;
      }
    );
  }

  public resolved(): boolean {
    return this.every((ex, index, depth, nestDiff) => {
      return ex instanceof RefExpression ? ex.nest <= nestDiff : null; // Search within
    });
  }

  public resolvedWithoutExternals(): boolean {
    return this.every((ex, index, depth, nestDiff) => {
      if (ex instanceof ExternalExpression) return false;
      return ex instanceof RefExpression ? ex.nest <= nestDiff : null; // Search within
    });
  }

  public noRefs(): boolean {
    return this.every((ex) => {
      if (ex instanceof RefExpression) return false;
      return null;
    });
  }

  public isAggregate(): boolean {
    return false;
  }

  /**
   * Decompose instances of $data.average($x) into $data.sum($x) / $data.count()
   * @param countEx and optional expression to use in a sum instead of a count
   */
  public decomposeAverage(countEx?: Expression): Expression {
    return this.substitute((ex) => {
      if (ex instanceof AverageExpression) {
        return ex.decomposeAverage(countEx);
      }
      return null;
    });
  }

  /**
   * Apply the distributive law wherever possible to aggregates
   * Turns $data.sum($x - 2 * $y) into $data.sum($x) - 2 * $data.sum($y)
   */
  public distribute(): Expression {
    return this.substitute((ex: Expression, index: int) => {
      if (index === 0) return null;
      let distributedEx = ex.distribute();
      if (distributedEx === ex) return null;
      return distributedEx;
    }).simplify();
  }

  /**
   * Returns the maximum number of possible values this expression can return in a split context
   */
  public maxPossibleSplitValues(): number {
    return this.type === "BOOLEAN" ? 3 : Infinity;
  }

  // ---------------------------------------------------------
  // Evaluation

  private _initialPrepare(
    context: Datum,
    environment: Environment
  ): Expression {
    return this.defineEnvironment(environment)
      .referenceCheck(context)
      .resolve(context)
      .simplify();
  }

  /**
   * Simulates computing the expression and returns the results with the right shape (but fake data)
   * @param context The context within which to compute the expression
   * @param options The options within which to evaluate
   */
  public simulate(
    context: Datum = {},
    options: ComputeOptions = {}
  ): PlywoodValue {
    failIfIntrospectNeededInDatum(context);

    let readyExpression = this._initialPrepare(context, options);
    if (readyExpression instanceof ExternalExpression) {
      // Top level externals need to be unsuppressed
      readyExpression = readyExpression.unsuppress();
    }

    return readyExpression._computeResolvedSimulate(options, []);
  }

  /**
   * Simulates computing the expression and returns the queries that would have been made
   * @param context The context within which to compute the expression
   * @param options The options within which to evaluate
   */
  public simulateQueryPlan(
    context: Datum = {},
    options: ComputeOptions = {}
  ): any[][] {
    failIfIntrospectNeededInDatum(context);

    let readyExpression = this._initialPrepare(context, options);
    if (readyExpression instanceof ExternalExpression) {
      // Top level externals need to be unsuppressed
      readyExpression = readyExpression.unsuppress();
    }

    let simulatedQueryGroups: any[] = [];
    readyExpression._computeResolvedSimulate(options, simulatedQueryGroups);
    return simulatedQueryGroups;
  }

  private _computeResolvedSimulate(
    options: ComputeOptions,
    simulatedQueryGroups: any[][]
  ): PlywoodValue {
    const {
      maxComputeCycles = 5,
      maxQueries = 500,
      maxRows,
      concurrentQueryLimit = Infinity,
    } = options;

    let ex: Expression = this;
    let readyExternals = ex.getReadyExternals(concurrentQueryLimit);

    let computeCycles = 0;
    let queries = 0;

    while (
      Object.keys(readyExternals).length > 0 &&
      computeCycles < maxComputeCycles &&
      queries < maxQueries
    ) {
      let simulatedQueryGroup: any[] = [];
      fillExpressionExternalAlteration(readyExternals, (external, terminal) => {
        if (queries < maxQueries) {
          queries++;
          return external.simulateValue(terminal, simulatedQueryGroup);
        } else {
          queries++;
          return null; // Query limit reached, don't do any more queries.
        }
      });

      simulatedQueryGroups.push(simulatedQueryGroup);
      ex = ex.applyReadyExternals(readyExternals);
      const literalValue = ex.getLiteralValue();
      if (maxRows && literalValue instanceof Dataset) {
        ex = r(literalValue.depthFirstTrimTo(maxRows));
      }
      readyExternals = ex.getReadyExternals(concurrentQueryLimit);
      computeCycles++;
    }
    return ex.getLiteralValue();
  }

  /**
   * Computes a general asynchronous expression
   * @param context The context within which to compute the expression
   * @param options The options determining computation
   */
  public compute(
    context: Datum = {},
    options: ComputeOptions = {}
  ): Promise<PlywoodValue> {
    return Promise.resolve(null)
      .then(() => {
        return introspectDatum(context);
      })
      .then((introspectedContext: Datum) => {
        const { customOptions } = options;

        // 检查是否启用 subtotalsSpec 优化
        if (customOptions && customOptions.useSubtotalsSpec) {
          // 关键修复：为 subtotalsSpec 优化创建两个独立的表达式副本
          // 这样可以确保 splitExpressionsMap 和 queryPlan 内部的 ex 都是完整的

          // 副本 1：用于提取 split expressions
          let readyExpression1 = this._initialPrepare(
            introspectedContext,
            options
          );
          if (readyExpression1 instanceof ExternalExpression) {
            readyExpression1 = readyExpression1.unsuppress();
          }

          // 副本 2：用于生成查询计划
          let readyExpression2 = this._initialPrepare(
            introspectedContext,
            options
          );
          if (readyExpression2 instanceof ExternalExpression) {
            readyExpression2 = readyExpression2.unsuppress();
          }

          return readyExpression1._computeWithSubtotalsSpec(
            introspectedContext,
            options,
            readyExpression2 // 传入第二个副本用于生成 queryPlan
          );
        }

        // 正常计算流程
        let readyExpression = this._initialPrepare(
          introspectedContext,
          options
        );
        if (readyExpression instanceof ExternalExpression) {
          readyExpression = readyExpression.unsuppress();
        }
        return readyExpression._computeResolved(options);
      });
  }

  /**
   * Computes a general asynchronous expression and streams the results
   * @param context The context within which to compute the expression
   * @param options The options determining computation
   */
  public computeStream(
    context: Datum = {},
    options: ComputeOptions = {}
  ): ReadableStream {
    const pt = new PassThrough({ objectMode: true });

    let rawQueries = options.rawQueries;

    introspectDatum(context)
      .then((introspectedContext: Datum) => {
        let readyExpression = this._initialPrepare(
          introspectedContext,
          options
        );
        if (readyExpression instanceof ExternalExpression) {
          // Top level externals need to be unsuppressed
          //readyExpression = readyExpression.unsuppress();
          pipeWithError(
            readyExpression.external.queryValueStream(true, rawQueries),
            pt
          );
          return;
        }

        readyExpression._computeResolved(options).then((v) => {
          const i = iteratorFactory(v as Dataset);
          let bit: PlyBit;
          while ((bit = i())) {
            pt.write(bit);
          }
          pt.end();
        });
      })
      .catch((e) => {
        pt.emit("error", e);
      });

    return pt as any;
  }

  private _computeResolved(options: ComputeOptions): Promise<PlywoodValue> {
    const {
      customOptions = null,
      rawQueries,
      maxComputeCycles = 5,
      maxQueries = 500,
      maxRows,
      timeout,
      concurrentQueryLimit = Infinity,
      beforePerSplitRequestFn = function () {},
      afterSplitRequestFn = function () {},
    } = options;

    // 记录开始时间（毫秒）
    const startTime = Date.now();

    let ex: Expression = this;
    let readyExternals = ex.getReadyExternals(concurrentQueryLimit);

    let computeCycles = 0;
    let queriesMade = 0;
    return promiseWhile(
      () => {
        // 在每次循环前检查是否超时
        if (typeof timeout === "number" && Date.now() - startTime > timeout) {
          return false;
        }
        return (
          Object.keys(readyExternals).length > 0 &&
          computeCycles < maxComputeCycles &&
          queriesMade < maxQueries
        );
      },
      async () => {
        const readyExternalsFilled =
          await fillExpressionExternalAlterationAsync(
            readyExternals,
            (external, terminal) => {
              if (queriesMade < maxQueries) {
                if (
                  typeof timeout === "number" &&
                  Date.now() - startTime > timeout
                ) {
                  console.error(
                    `${formatDateTimeForLog(new Date())} 进程ID:${
                      process.env.pm_id
                    } Plywood Operation timed out, exceeded ${
                      timeout / 1000
                    } seconds customOptions->${JSON.stringify(customOptions)}`
                  );
                  return Promise.reject(
                    new Error(
                      `Plywood Operation timed out, exceeded ${
                        timeout / 1000
                      } seconds`
                    )
                  );
                }
                queriesMade++;
                beforePerSplitRequestFn(external);
                // todo: 3
                return external.queryValue(terminal, rawQueries, customOptions);
              } else {
                queriesMade++;
                return Promise.reject(new Error("Query limit exceeded."));
                // return Promise.resolve(null); // Query limit reached, don't do any more queries.
              }
            }
          );

        afterSplitRequestFn(queriesMade);
        ex = ex.applyReadyExternals(readyExternalsFilled);
        const literalValue = ex.getLiteralValue();
        if (maxRows && literalValue instanceof Dataset) {
          ex = r(literalValue.depthFirstTrimTo(maxRows));
        }
        readyExternals = ex.getReadyExternals(concurrentQueryLimit);
        computeCycles++;
      }
    ).then(() => {
      // 最后再检查一次是否超时
      if (typeof timeout === "number" && Date.now() - startTime > timeout) {
        console.error(
          `${formatDateTimeForLog(new Date())} 进程ID:${
            process.env.pm_id
          } Plywood Operation timed out, exceeded ${
            timeout / 1000
          } seconds customOptions->${JSON.stringify(customOptions)}`
        );
        return Promise.reject(
          new Error(
            `Plywood Operation timed out, exceeded ${timeout / 1000} seconds`
          )
        );
      }
      if (!ex.isOp("literal"))
        throw new Error(`something went wrong, did not get literal: ${ex}`);
      return ex.getLiteralValue();
    });
  }

  /**
   * 从表达式中递归提取所有层级的 split 的 expression 信息
   * 参考 _computeResolvedSimulate 的循环逻辑，逐层处理嵌套的 split
   * 注意：此方法不应改变原始表达式的状态，仅用于提取信息
   *
   * 关键点：
   * 1. 通过 Expression.fromJS(expression.toJS()) 创建表达式的深拷贝
   * 2. 在副本上调用 getReadyExternals 和 applyReadyExternals，避免状态污染
   * 3. 原始表达式保持不变，可以继续用于后续的查询执行
   */
  private _extractSplitExpressionsFromExternals(
    expression: Expression,
    concurrentQueryLimit: number = Infinity
  ): Map<string, Expression> {
    const splitExpressionsMap = new Map<string, Expression>();

    // 创建表达式的深拷贝，避免影响原始表达式
    // 这是关键：simulateQueryPlan 已经消耗了表达式的状态
    // 我们需要一个全新的副本来重新提取 split expressions
    let ex: Expression = expression;
    let readyExternals = ex.getReadyExternals(concurrentQueryLimit);
    let computeCycles = 0;
    const maxComputeCycles = 5; // 防止无限循环

    // 递归提取每一层的 split 信息
    while (
      Object.keys(readyExternals).length > 0 &&
      computeCycles < maxComputeCycles
    ) {
      // 从当前层的 readyExternals 提取 split 信息
      this._extractSplitExpressionsFromAlterations(
        readyExternals,
        splitExpressionsMap
      );

      // 模拟执行以推进到下一层（不保存模拟查询）
      fillExpressionExternalAlteration(readyExternals, (external, terminal) => {
        return external.simulateValue(terminal, []);
      });

      // 应用结果并获取下一层的 readyExternals
      ex = ex.applyReadyExternals(readyExternals);
      readyExternals = ex.getReadyExternals(concurrentQueryLimit);
      computeCycles++;
    }

    return splitExpressionsMap;
  }

  /**
   * 从单层 alterations 中提取 split expressions
   * 根据真实数据结构处理：
   * 1. DatasetExternalAlterations 数组（item.jsonl 中的主要结构）
   * 2. ExpressionExternalAlterationSimple 对象（包含 external）
   * 3. 嵌套的 expressionAlterations 和 datasetAlterations
   * 注意：某些对象可能同时包含 external 和 expressionAlterations（混合类型）
   */
  private _extractSplitExpressionsFromAlterations(
    readyExternals: ExpressionExternalAlteration,
    splitExpressionsMap: Map<string, Expression>
  ): void {
    for (const key in readyExternals) {
      const alteration = readyExternals[key];

      if (Array.isArray(alteration)) {
        // 处理 DatasetExternalAlterations 数组
        for (const item of alteration) {
          // 处理当前项的 external
          if (
            item.external &&
            item.external.mode === "split" &&
            item.external.split
          ) {
            item.external.split.mapSplits(
              (label: string, expression: Expression) => {
                if (!splitExpressionsMap.has(label)) {
                  splitExpressionsMap.set(label, expression);
                }
              }
            );
          }

          // 递归处理 expressionAlterations（嵌套的表达式级别的 alterations）
          if (item.expressionAlterations) {
            this._extractSplitExpressionsFromAlterations(
              item.expressionAlterations,
              splitExpressionsMap
            );
          }

          // 递归处理 datasetAlterations（嵌套的数据集级别的 alterations）
          if (
            item.datasetAlterations &&
            Array.isArray(item.datasetAlterations)
          ) {
            for (const nestedItem of item.datasetAlterations) {
              // 处理嵌套项的 external
              if (
                nestedItem.external &&
                nestedItem.external.mode === "split" &&
                nestedItem.external.split
              ) {
                nestedItem.external.split.mapSplits(
                  (label: string, expression: Expression) => {
                    if (!splitExpressionsMap.has(label)) {
                      splitExpressionsMap.set(label, expression);
                    }
                  }
                );
              }

              // 继续递归处理嵌套的 expressionAlterations
              if (nestedItem.expressionAlterations) {
                this._extractSplitExpressionsFromAlterations(
                  nestedItem.expressionAlterations,
                  splitExpressionsMap
                );
              }
            }
          }
        }
      } else {
        // 处理非数组类型（可能是 ExpressionExternalAlterationSimple 或混合类型）
        // 先处理 external（如果存在）
        if (alteration.external) {
          const external = alteration.external;
          if (external.mode === "split" && external.split) {
            external.split.mapSplits(
              (label: string, expression: Expression) => {
                if (!splitExpressionsMap.has(label)) {
                  splitExpressionsMap.set(label, expression);
                }
              }
            );
          }
        }

        // 递归处理 expressionAlterations（如果存在）
        // 某些对象可能同时包含 external 和 expressionAlterations
        if ((alteration as any).expressionAlterations) {
          this._extractSplitExpressionsFromAlterations(
            (alteration as any).expressionAlterations,
            splitExpressionsMap
          );
        }

        // 递归处理 datasetAlterations（如果存在）
        if (
          (alteration as any).datasetAlterations &&
          Array.isArray((alteration as any).datasetAlterations)
        ) {
          for (const nestedItem of (alteration as any).datasetAlterations) {
            // 处理嵌套项的 external
            if (
              nestedItem.external &&
              nestedItem.external.mode === "split" &&
              nestedItem.external.split
            ) {
              nestedItem.external.split.mapSplits(
                (label: string, expression: Expression) => {
                  if (!splitExpressionsMap.has(label)) {
                    splitExpressionsMap.set(label, expression);
                  }
                }
              );
            }

            // 继续递归处理嵌套的 expressionAlterations
            if (nestedItem.expressionAlterations) {
              this._extractSplitExpressionsFromAlterations(
                nestedItem.expressionAlterations,
                splitExpressionsMap
              );
            }
          }
        }
      }
    }
  }

  /**
   * 使用 subtotalsSpec 优化计算表达式
   * 通过 simulateQueryPlan 获取所有查询，然后合并为一个带有 subtotalsSpec 的 groupBy 查询
   *
   * 关键修复：
   * - simulateQueryPlan 和 _extractSplitExpressionsFromExternals 都会消耗表达式状态
   * - 在 compute() 中为两个操作分别创建独立的表达式副本
   * - 通过参数传入第二个副本，确保 ex 都是完整的
   * - 并行执行 timeseries 总计查询和 subtotalsSpec 查询，合并结果
   *
   * @param context 计算上下文
   * @param options 计算选项
   * @param expressionForQueryPlan 用于生成查询计划的独立表达式副本
   */
  private _computeWithSubtotalsSpec(
    context: Datum,
    options: ComputeOptions,
    expressionForQueryPlan: Expression
  ): Promise<PlywoodValue> {
    try {
      // 1. 使用 this 提取 split expressions（第一个副本）
      const splitExpressionsMap = this._extractSplitExpressionsFromExternals(
        this,
        options.concurrentQueryLimit
      );

      // 2. 使用传入的独立副本生成查询计划（第二个副本）
      const queryPlan = expressionForQueryPlan.simulateQueryPlan(
        context,
        options
      );

      if (queryPlan.length === 0) {
        throw new Error("没有生成查询计划");
      }

      // 3. 提取 timeseries 总计查询
      const timeseriesQuery = this._extractTimeseriesQuery(queryPlan);

      // 4. 合并查询为一个带有 subtotalsSpec 的 groupBy 查询
      const mergedQuery = this._mergeQueriesWithSubtotalsSpec(queryPlan);

      // 5. 并行执行两个查询并合并结果
      return this._executeQueriesInParallel(
        timeseriesQuery,
        mergedQuery,
        context,
        options,
        splitExpressionsMap
      );
    } catch (error) {
      console.error("subtotalsSpec 优化失败，回退到正常计算:", error.message);
      // 回退到正常计算
      return this._computeResolved(options);
    }
  }

  /**
   * 从查询计划中提取 timeseries 查询（用于获取总计行）
   */
  private _extractTimeseriesQuery(queryPlan: any[][]): any | null {
    // 优先选择带有明确聚合名称（且不为 __VALUE__）的 timeseries，避免“value”模式导致的度量名坍缩
    let fallback: any = null;
    let preferred: any = null;
    for (const group of queryPlan) {
      for (const query of group) {
        if (query.queryType !== "timeseries") continue;
        if (!fallback) fallback = { ...query };
        const aggs = Array.isArray(query.aggregations)
          ? query.aggregations
          : [];
        const hasNamedAggs =
          aggs.length > 0 &&
          aggs.some((a: any) => a && a.name && a.name !== "__VALUE__");
        if (hasNamedAggs) {
          preferred = { ...query };
          break;
        }
      }
      if (preferred) break;
    }
    return preferred || fallback;
  }

  /**
   * 并行执行 timeseries 总计查询和 subtotalsSpec 查询，然后合并结果
   */
  private _executeQueriesInParallel(
    timeseriesQuery: any | null,
    subtotalsQuery: any,
    context: Datum,
    options: ComputeOptions,
    splitExpressions: Map<string, Expression>
  ): Promise<PlywoodValue> {
    // 找到 DruidExternal
    const druidExternal = this._findDruidExternal(context);
    if (!druidExternal) {
      throw new Error("未找到 DruidExternal 数据源");
    }

    // 提取 attributes 和 keys 信息
    const extractedInfo = this._extractAttributesFromSubtotalsQuery(
      subtotalsQuery,
      splitExpressions
    );

    // 创建并行执行的 Promise 数组
    const promises: Promise<any>[] = [];

    // 1. 执行 subtotalsSpec 查询（保持原有逻辑）
    const subtotalsPromise = this._executeSubtotalsQuery(
      subtotalsQuery,
      context,
      options,
      splitExpressions
    );
    promises.push(subtotalsPromise);

    // 2. 如果有 timeseries 查询，并行执行
    let totalRowPromise: Promise<any> | null = null;
    if (timeseriesQuery) {
      totalRowPromise = this._executeTotalRowQuery(
        timeseriesQuery,
        druidExternal,
        options,
        extractedInfo
      );
      promises.push(totalRowPromise);
    }

    // 3. 等待两个查询都完成
    return Promise.all(promises).then((results) => {
      const subtotalsResult = results[0]; // subtotalsSpec 查询结果（已经是扁平化的对象）
      const totalRowData = results[1]; // timeseries 查询结果（可能为 undefined）

      // 4. 合并结果（将总计行插入到 subtotals 结果中）
      const mergedResult = this._mergeTotalRowIntoSubtotalsResult(
        subtotalsResult,
        totalRowData,
        extractedInfo.keys
      );

      // 5. 构建层级结构
      const hierarchicalResult = this._buildHierarchicalDataset(
        mergedResult,
        subtotalsQuery,
        extractedInfo
      );

      // 6. 转换为 Dataset
      return Dataset.fromJS(hierarchicalResult);
    });
  }

  /**
   * 执行 subtotalsSpec 查询（保持原有逻辑不变）
   */
  private _executeSubtotalsQuery(
    query: any,
    context: Datum,
    options: ComputeOptions,
    splitExpressions: Map<string, Expression>
  ): Promise<any> {
    // 找到 DruidExternal
    const druidExternal = this._findDruidExternal(context);
    if (!druidExternal) {
      throw new Error("未找到 DruidExternal 数据源");
    }

    const { rawQueries, customOptions } = options;
    const { engine } = druidExternal;
    const requester = druidExternal.requester;

    if (!requester) {
      throw new Error(
        "DruidExternal 缺少 requester，请确保在创建 External 时传入了 requester"
      );
    }

    // 提取 attributes 和 keys 信息
    const extractedInfo = this._extractAttributesFromSubtotalsQuery(
      query,
      splitExpressions
    );

    // 生成 inflaters
    const inflaters = this._generateInflaters(query, splitExpressions);

    // 构建查询上下文
    const queryContext: any = {
      timestamp: null,
      ignorePrefix: "!",
      dummyPrefix: "***",
    };

    // 创建 postTransform 函数
    const postTransform = External.postTransformFactory(
      inflaters,
      extractedInfo.attributes,
      extractedInfo.keys,
      null // zeroTotalApplies
    );

    // 构建 QueryAndPostTransform 对象
    const queryAndPostTransform = {
      query,
      context: queryContext,
      postTransform,
    };

    // 执行查询
    const resultStream = External.performQueryAndPostTransform(
      queryAndPostTransform,
      requester,
      engine,
      rawQueries,
      customOptions || {}
    );

    // 将流转换为结果对象（返回扁平化的数据）
    return External.buildValueFromStream(resultStream);
  }

  /**
   * 执行 timeseries 总计查询，获取真实的总计行数据
   */
  private _executeTotalRowQuery(
    query: any,
    druidExternal: any,
    options: ComputeOptions,
    extractedInfo: { attributes: any[]; keys: string[] }
  ): Promise<any> {
    const { rawQueries, customOptions } = options;
    const { engine } = druidExternal;
    const requester = druidExternal.requester;

    if (!requester) {
      throw new Error(
        "DruidExternal 缺少 requester，请确保在创建 External 时传入了 requester"
      );
    }

    // 构建查询上下文
    const queryContext: any = {
      timestamp: null,
      ignorePrefix: "!",
      dummyPrefix: "***",
    };

    // timeseries 查询不需要 inflaters，因为没有维度
    const postTransform = External.postTransformFactory(
      [], // 空的 inflaters
      extractedInfo.attributes,
      [], // timeseries 没有 keys
      null // zeroTotalApplies
    );

    // 构建 QueryAndPostTransform 对象
    const queryAndPostTransform = {
      query,
      context: queryContext,
      postTransform,
    };

    // 执行查询
    const resultStream = External.performQueryAndPostTransform(
      queryAndPostTransform,
      requester,
      engine,
      rawQueries,
      customOptions || {}
    );

    // 将流转换为结果对象
    return External.buildValueFromStream(resultStream).then((result: any) => {
      // timeseries 查询返回的数据格式：{ data: [ {...} ] }
      // 提取第一行数据作为总计行
      if (
        result &&
        result.data &&
        Array.isArray(result.data) &&
        result.data.length > 0
      ) {
        return result.data[0];
      }
      return null;
    });
  }

  /**
   * 将 timeseries 查询得到的总计行数据合并到 subtotalsSpec 查询结果中
   * 注意：需要去重，如果 subtotals 结果中已有总计行，则替换；否则插入
   */
  private _mergeTotalRowIntoSubtotalsResult(
    subtotalsResult: any,
    totalRowData: any,
    keys: string[]
  ): any {
    if (
      !subtotalsResult ||
      !subtotalsResult.data ||
      !Array.isArray(subtotalsResult.data)
    ) {
      return subtotalsResult;
    }

    // 如果没有总计行数据，直接返回原结果
    if (!totalRowData) {
      return subtotalsResult;
    }

    const data = subtotalsResult.data;

    // 查找 subtotals 结果中是否已存在总计行（所有维度字段为 null）
    const existingTotalRowIndex = data.findIndex((row: any) =>
      keys.every((k) => {
        const actualKey = this._resolveActualKeyName(k, [row]);
        return row[actualKey] === null || row[actualKey] === undefined;
      })
    );

    // 构建完整的总计行（包含所有维度字段设为 null）
    const enhancedTotalRow = { ...totalRowData };
    keys.forEach((k) => {
      const actualKey = this._resolveActualKeyName(k, data);
      if (!(actualKey in enhancedTotalRow)) {
        enhancedTotalRow[actualKey] = null;
      }
    });

    if (existingTotalRowIndex >= 0) {
      // 如果已存在总计行，替换它（使用 timeseries 查询的准确数据）
      data[existingTotalRowIndex] = enhancedTotalRow;
    } else {
      // 如果不存在总计行（可能因为 limit 被截断），插入到数组开头
      data.unshift(enhancedTotalRow);
    }

    return {
      ...subtotalsResult,
      data,
    };
  }

  /**
   * 合并多个查询为一个带有 subtotalsSpec 的 groupBy 查询
   */
  private _mergeQueriesWithSubtotalsSpec(queryPlan: any[][]): any {
    // 找到第一个 timeseries 查询作为模板
    let templateQuery: any = null;
    const dimensionQueries: any[] = [];

    for (const group of queryPlan) {
      for (const query of group) {
        if (query.queryType === "timeseries" && !templateQuery) {
          templateQuery = { ...query };
        } else if (
          query.queryType === "topN" ||
          query.queryType === "groupBy"
        ) {
          dimensionQueries.push(query);
        }
      }
    }

    if (!templateQuery) throw new Error("未找到 timeseries 查询作为模板");
    if (dimensionQueries.length === 0) throw new Error("未找到维度查询");

    // 以 timeseries 查询为模板，转换为 groupBy 查询
    const mergedQuery: any = {
      ...templateQuery,
      queryType: "groupBy",
      granularity: "all",
    };

    // 收集所有 virtualColumns 并去重
    const virtualColumnsMap = new Map<string, any>();
    if (Array.isArray(templateQuery.virtualColumns)) {
      for (const vc of templateQuery.virtualColumns) {
        if (vc.name) virtualColumnsMap.set(vc.name, vc);
      }
    }

    // 收集所有维度并去重，同时合并 virtualColumns
    const dimensionsMap = new Map<string, any>();
    for (const dq of dimensionQueries) {
      if (Array.isArray(dq.virtualColumns)) {
        for (const vc of dq.virtualColumns) {
          if (vc.name && !virtualColumnsMap.has(vc.name))
            virtualColumnsMap.set(vc.name, vc);
        }
      }

      if (dq.dimension) {
        const dimName = this._extractDimensionName(dq.dimension);
        if (dimName && !dimensionsMap.has(dimName))
          dimensionsMap.set(dimName, dq.dimension);
      } else if (Array.isArray(dq.dimensions)) {
        for (const dim of dq.dimensions) {
          const dimName = this._extractDimensionName(dim);
          if (dimName && !dimensionsMap.has(dimName))
            dimensionsMap.set(dimName, dim);
        }
      }
    }

    // 生成 subtotalsSpec
    if (dimensionsMap.size > 0) {
      const dimensions = Array.from(dimensionsMap.values());
      mergedQuery.dimensions = dimensions;

      const dimensionOutputNames = dimensions
        .map((dim: any) => this._extractDimensionOutputName(dim))
        .filter((name) => name !== null) as string[];

      const subtotalsSpec: string[][] = [];
      for (let i = dimensionOutputNames.length; i > 0; i--) {
        subtotalsSpec.push(dimensionOutputNames.slice(0, i));
      }
      // 添加空数组表示总计
      subtotalsSpec.push([]);
      mergedQuery.subtotalsSpec = subtotalsSpec;
    }

    // 添加 virtualColumns 到合并查询中
    if (virtualColumnsMap.size > 0)
      mergedQuery.virtualColumns = Array.from(virtualColumnsMap.values());

    // 合并 aggregations / postAggregations（以维度查询中的定义为准）
    const aggMap = new Map<string, any>();
    const postAggMap = new Map<string, any>();

    for (const dq of dimensionQueries) {
      if (Array.isArray(dq.aggregations)) {
        for (const agg of dq.aggregations) {
          if (!agg || !agg.name) continue;
          if (agg.name === "__VALUE__") continue; // 跳过 value 模式的占位名
          if (!aggMap.has(agg.name)) aggMap.set(agg.name, agg);
        }
      }
      if (Array.isArray(dq.postAggregations)) {
        for (const pa of dq.postAggregations) {
          if (!pa || !pa.name) continue;
          if (!postAggMap.has(pa.name)) postAggMap.set(pa.name, pa);
        }
      }
    }

    // 若维度查询没有提供，则回退使用模板 timeseries 的聚合定义
    const templateAggs = Array.isArray(templateQuery.aggregations)
      ? templateQuery.aggregations
      : [];
    const templatePostAggs = Array.isArray(templateQuery.postAggregations)
      ? templateQuery.postAggregations
      : [];

    if (aggMap.size === 0 && templateAggs.length > 0) {
      for (const agg of templateAggs) {
        if (!agg || !agg.name) continue;
        // 如果模板里也有 __VALUE__，且存在其他具名度量，则忽略 __VALUE__
        if (agg.name === "__VALUE__" && templateAggs.length > 1) continue;
        if (!aggMap.has(agg.name)) aggMap.set(agg.name, agg);
      }
    }

    if (postAggMap.size === 0 && templatePostAggs.length > 0) {
      for (const pa of templatePostAggs) {
        if (!pa || !pa.name) continue;
        if (!postAggMap.has(pa.name)) postAggMap.set(pa.name, pa);
      }
    }

    if (aggMap.size > 0) mergedQuery.aggregations = Array.from(aggMap.values());
    if (postAggMap.size > 0)
      mergedQuery.postAggregations = Array.from(postAggMap.values());

    // 合并其他参数：having
    for (const dq of dimensionQueries) {
      if (dq.having && !mergedQuery.having) mergedQuery.having = dq.having;
    }

    // 合并所有排序规则（维度排序 + 指标排序）
    const sortColumns = this._mergeSortColumns(dimensionQueries);
    if (sortColumns.length > 0) {
      mergedQuery.limitSpec = {
        type: "default",
        columns: sortColumns,
        limit: 10000,
      };
    }

    return mergedQuery;
  }

  /**
   * 提取维度名称，用于去重（使用 dimension 字段）
   */
  private _extractDimensionName(dim: any): string | null {
    if (typeof dim === "string") {
      return dim;
    } else if (dim.dimension) {
      return dim.dimension;
    } else if (dim.outputName) {
      return dim.outputName;
    }
    return null;
  }

  /**
   * 提取维度的输出名称，用于 subtotalsSpec（使用 outputName 字段）
   */
  private _extractDimensionOutputName(dim: any): string | null {
    if (typeof dim === "string") {
      return dim;
    } else if (dim.outputName) {
      return dim.outputName;
    } else if (dim.dimension) {
      return dim.dimension;
    }
    return null;
  }

  /**
   * 从 topN 查询的 metric 字段中提取排序规则
   */
  private _extractSortFromTopNMetric(query: any): any | null {
    if (query.queryType !== "topN" || !query.metric) return null;

    const metric = query.metric;
    let direction = "ascending";
    let dimensionOrder = "lexicographic";

    // 处理 inverted 类型（表示倒序）
    if (metric.type === "inverted") {
      direction = "descending";
      if (metric.metric && metric.metric.type === "dimension") {
        dimensionOrder = metric.metric.ordering || "lexicographic";
      }
    } else if (metric.type === "dimension") {
      dimensionOrder = metric.ordering || "lexicographic";
    } else {
      return null; // 不是维度排序
    }

    // 提取维度输出名称
    const dimension = this._extractDimensionOutputName(query.dimension);
    if (!dimension) return null;

    return { dimension, direction, dimensionOrder };
  }

  /**
   * 合并所有排序规则（维度排序优先，指标排序次之）
   */
  private _mergeSortColumns(dimensionQueries: any[]): any[] {
    const sortMap = new Map<string, any>(); // key: dimension 名称, value: OrderByColumnSpec

    // 1. 先收集维度排序（优先级高）
    for (const query of dimensionQueries) {
      if (query.queryType === "topN") {
        const sortSpec = this._extractSortFromTopNMetric(query);
        if (sortSpec && !sortMap.has(sortSpec.dimension)) {
          sortMap.set(sortSpec.dimension, sortSpec);
        }
      }
    }

    // 2. 再收集指标排序（优先级低）
    for (const query of dimensionQueries) {
      if (query.limitSpec && query.limitSpec.columns) {
        for (const column of query.limitSpec.columns) {
          if (!sortMap.has(column.dimension)) {
            sortMap.set(column.dimension, column);
          }
        }
      }
    }

    return Array.from(sortMap.values());
  }

  /**
   * 去除 Druid 生成的 dummy 前缀（例如 '***'）
   */
  private _stripDummyPrefix(name: string): string {
    const dummyPrefix = "***";
    if (typeof name === "string" && name.indexOf(dummyPrefix) === 0) {
      return name.slice(dummyPrefix.length);
    }
    return name;
  }

  /**
   * 在实际数据行中解析出某个维度键真正使用的字段名（可能带有 dummy 前缀）
   */
  private _resolveActualKeyName(requestedKey: string, data: any[]): string {
    if (!Array.isArray(data) || data.length === 0) return requestedKey;
    const prefixed = "***" + requestedKey;
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      if (!row) continue;
      if (Object.prototype.hasOwnProperty.call(row, requestedKey))
        return requestedKey;
      if (Object.prototype.hasOwnProperty.call(row, prefixed)) return prefixed;
    }
    return requestedKey;
  }

  /**
   * 从 subtotalsSpec 查询中提取 attributes 信息
   * 注意：需要移除 dimensions.outputName 上可能存在的 dummy 前缀 (***),
   * 以确保与实际数据中的字段名匹配
   */
  private _extractAttributesFromSubtotalsQuery(
    query: any,
    splitExpressions: Map<string, Expression>
  ): {
    attributes: any[];
    keys: string[];
  } {
    const attributes: any[] = [];
    const keys: string[] = [];

    // 1. 处理维度信息
    if (query.dimensions && Array.isArray(query.dimensions)) {
      query.dimensions.forEach((dimension: any) => {
        const rawName = dimension.outputName || dimension.dimension;
        const outputName = this._stripDummyPrefix(rawName);
        keys.push(outputName);

        // 根据 splitExpression 确定属性类型
        let attributeType = "STRING";
        const expression = splitExpressions.get(outputName);

        if (expression) {
          if (expression instanceof NumberBucketExpression) {
            attributeType = "NUMBER_RANGE";
          } else if (expression instanceof TimeBucketExpression) {
            attributeType = "TIME_RANGE";
          } else if (expression.type) {
            attributeType = expression.type;
          }
        } else if (
          dimension.outputType === "LONG" ||
          dimension.outputType === "FLOAT"
        ) {
          attributeType = "NUMBER";
        } else if (
          dimension.dimension === "__time" ||
          outputName === "__time"
        ) {
          // __time 作为维度通常应视为时间（按桶聚合时可视为 TIME / TIME_RANGE）
          attributeType = "TIME_RANGE";
        }

        attributes.push({
          name: outputName,
          type: attributeType,
        });
      });
    }

    // 2. 处理聚合信息
    if (query.aggregations && Array.isArray(query.aggregations)) {
      query.aggregations.forEach((aggregation: any) => {
        attributes.push({
          name: aggregation.name,
          type: "NUMBER",
        });
      });
    }

    // 3. 处理后聚合信息
    if (query.postAggregations && Array.isArray(query.postAggregations)) {
      query.postAggregations.forEach((postAggregation: any) => {
        attributes.push({
          name: postAggregation.name,
          type: "NUMBER",
        });
      });
    }

    return { attributes, keys };
  }

  /**
   * 提取时间维度信息（period 与 timeZone），用于将字符串 __time 转换为 TimeRange
   */
  private _getTimeDimensionInfo(
    query: any,
    keyName: string
  ): { duration: Duration; timezone: Timezone } | null {
    if (!query || !Array.isArray(query.dimensions)) return null;

    for (const dim of query.dimensions) {
      const rawName = (dim.outputName || dim.dimension) as string;
      const outputName = this._stripDummyPrefix(rawName);
      if (outputName !== keyName) continue;

      // 仅处理 __time 维度的 timeFormat 提取函数
      if (
        (dim.dimension === "__time" || outputName === "__time") &&
        dim.extractionFn &&
        dim.extractionFn.type === "timeFormat" &&
        dim.extractionFn.granularity &&
        (dim.extractionFn.granularity.type === "period" ||
          typeof dim.extractionFn.granularity.period === "string")
      ) {
        const period = dim.extractionFn.granularity.period;
        const tz = dim.extractionFn.granularity.timeZone || "Etc/UTC";
        try {
          const duration = Duration.fromJS(period);
          const timezone = Timezone.fromJS(tz);
          return { duration, timezone };
        } catch (e) {
          return null;
        }
      }
    }

    return null;
  }

  /**
   * 如果当前分组键是 __time，则尝试将字符串键值转换为 TimeRange
   */
  private _maybeConvertTimeKey(
    query: any,
    keyName: string,
    keyValue: any
  ): any {
    if (keyName !== "__time") return keyValue;
    if (keyValue == null) return keyValue;

    const info = this._getTimeDimensionInfo(query, keyName);
    if (!info) return keyValue;

    let start: Date;
    if (keyValue instanceof Date) {
      start = keyValue as Date;
    } else if (typeof keyValue === "string" || typeof keyValue === "number") {
      let parsed = keyValue as any;
      if (typeof parsed === "string") {
        let s = parsed;
        if (/^\d{4}-\d{2}-\d{2}T\d{2}Z$/.test(s))
          s = s.replace(/Z$/, ":00:00Z");
        else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}Z$/.test(s))
          s = s.replace(/Z$/, ":00Z");
        else if (/^\d{4}-\d{2}-\d{2}T\d{2}$/.test(s)) s = s + ":00:00Z";
        else if (/^\d{4}-\d{2}-\d{2}$/.test(s)) s = s + "T00:00:00Z";
        parsed = s;
      }
      const d = new Date(parsed as any);
      if (isNaN(d.getTime())) return keyValue;
      start = d;
    } else {
      return keyValue;
    }

    try {
      const end = info.duration.shift(start, info.timezone, 1);
      return new TimeRange({ start, end });
    } catch {
      return keyValue;
    }
  }

  /**
   * 将扁平化的 subtotalsSpec 结果转换为层级结构
   */
  private _buildHierarchicalDataset(
    flatResult: any,
    query: any,
    extractedInfo: { attributes: any[]; keys: string[] }
  ): any {
    if (!flatResult || !flatResult.data || !Array.isArray(flatResult.data)) {
      return flatResult;
    }

    const { attributes, keys } = extractedInfo;
    const { subtotalsSpec } = query;

    if (
      !subtotalsSpec ||
      !Array.isArray(subtotalsSpec) ||
      subtotalsSpec.length === 0
    ) {
      return flatResult;
    }

    // 简化的层级构建策略：
    // 1. 找到总计行（所有维度都为 null）
    // 2. 按第一个维度分组构建第一层 SPLIT
    // 3. 递归构建更深层级

    const data = flatResult.data;

    // 计算每个维度键在数据中的实际字段名（可能带有 '***' 前缀）
    const actualKeys = keys.map((k) => this._resolveActualKeyName(k, data));

    // 找到总计行
    const totalRow = data.find((row: any) =>
      actualKeys.every(
        (ak: string) => row[ak] === null || row[ak] === undefined
      )
    );

    if (!totalRow) {
      console.warn("未找到总计行，使用原始结果");
      return flatResult;
    }

    // 构建顶层数据对象
    const topLevelData: any = {};

    // 添加聚合字段
    attributes.forEach((attr: any) => {
      if (attr.type === "NUMBER" && totalRow[attr.name] !== undefined) {
        topLevelData[attr.name] = totalRow[attr.name];
      }
    });

    // 如果有维度，构建 SPLIT
    if (keys.length > 0) {
      const splitData = this._buildSimpleSplit(
        data,
        keys,
        attributes,
        0,
        query
      );
      if (splitData && splitData.data.length > 0) {
        topLevelData.SPLIT = splitData;
      }
    }

    const result: any = {
      attributes: this._buildTopLevelAttributes(attributes, keys),
      keys: [],
      data: [topLevelData],
    };
    return result;
  }

  /**
   * 构建简单的 SPLIT 数据集
   */
  private _buildSimpleSplit(
    data: any[],
    keys: string[],
    attributes: any[],
    level: number,
    query: any
  ): any {
    if (level >= keys.length) {
      return null;
    }

    const currentKey = keys[level];
    const actualKey = this._resolveActualKeyName(currentKey, data);
    const groups: Map<string, { rows: any[]; actualValue: any }> = new Map();

    // 将复杂键（如 TimeRange/NumberRange/Date）稳定为字符串用于分组键
    const keyToStableString = (v: any): string => {
      if (v == null) return "__NULL__";
      if (typeof v === "number") return String(v);
      if (v instanceof Date) return String(v.valueOf());
      if (typeof v === "object") {
        const maybeStart: any = (v as any).start;
        if (maybeStart instanceof Date) return String(maybeStart.valueOf());
        if (typeof maybeStart === "number") return String(maybeStart);
      }
      return String(v);
    };

    // 按当前键（实际字段名）分组数据
    data.forEach((row: any) => {
      const keyValue = row[actualKey];
      if (keyValue !== null && keyValue !== undefined) {
        const groupKey = keyToStableString(keyValue);
        if (!groups.has(groupKey)) {
          groups.set(groupKey, { rows: [], actualValue: keyValue });
        }
        groups.get(groupKey)!.rows.push(row);
      }
    });

    const splitData: any[] = [];

    // 为每个分组构建数据项
    groups.forEach(({ rows: groupData, actualValue }, groupKey) => {
      const splitItem: any = {};

      // 设置当前维度值（使用实际的值，可能是 NumberRange 或 TimeRange 对象），并在是 __time 时进行 TimeRange 转换
      const maybeValue = this._maybeConvertTimeKey(
        query,
        currentKey,
        actualValue
      );
      splitItem[currentKey] = maybeValue;

      // 找到当前分组的聚合数据
      const aggregateRow = groupData.find((row: any) => {
        // 当前维度有值，后续维度为 null 的行就是当前层级的聚合
        const curMatches = keyToStableString(row[actualKey]) === groupKey;
        if (!curMatches) return false;
        if (level + 1 >= keys.length) return true;
        return keys.slice(level + 1).every((k) => {
          const ak = this._resolveActualKeyName(k, [row]);
          return row[ak] === null || row[ak] === undefined;
        });
      });

      if (aggregateRow) {
        // 添加聚合字段
        attributes.forEach((attr: any) => {
          if (attr.type === "NUMBER" && aggregateRow[attr.name] !== undefined) {
            splitItem[attr.name] = aggregateRow[attr.name];
          }
        });
      }

      // 如果还有更深层级，递归构建
      if (level + 1 < keys.length) {
        const nestedSplit = this._buildSimpleSplit(
          groupData,
          keys,
          attributes,
          level + 1,
          query
        );
        if (nestedSplit && nestedSplit.data.length > 0) {
          splitItem.SPLIT = nestedSplit;
        }
      }

      splitData.push(splitItem);
    });

    // 应用 limitSpec 排序
    this._applySortingToSplitData(splitData, query);

    const splitResult = {
      keys: [currentKey],
      attributes: this._buildSplitAttributes(
        attributes,
        currentKey,
        keys,
        level
      ),
      data: splitData,
    };
    return splitResult;
  }

  /**
   * 根据 limitSpec 对 splitData 进行排序
   */
  private _applySortingToSplitData(splitData: any[], query: any): void {
    if (
      !query ||
      !query.limitSpec ||
      !query.limitSpec.columns ||
      !Array.isArray(query.limitSpec.columns)
    ) {
      return;
    }

    const sortColumns = query.limitSpec.columns;
    if (sortColumns.length === 0) {
      return;
    }

    // 支持多列排序，按照 columns 数组的顺序进行排序
    // 注意：避免对 TimeRange 等复杂对象调用 String()，否则会触发 toString() 并可能抛错
    const coerceForCompare = (v: any): number | string | null => {
      if (v == null) return null;
      if (typeof v === "number") return v;
      if (v instanceof Date) return v.valueOf();
      if (typeof v === "object") {
        // TimeRange / NumberRange：优先用 start 值进行比较
        const maybeStart: any = (v as any).start;
        if (maybeStart instanceof Date) return maybeStart.valueOf();
        if (typeof maybeStart === "number") return maybeStart;
      }
      // 其他类型按字符串比较
      return String(v);
    };

    splitData.sort((a: any, b: any) => {
      for (const sortColumn of sortColumns) {
        const dimension = sortColumn.dimension;
        const direction = sortColumn.direction || "ascending";

        const aRaw = a[dimension];
        const bRaw = b[dimension];

        const aValue = coerceForCompare(aRaw);
        const bValue = coerceForCompare(bRaw);

        let comparison = 0;

        // 处理 null/undefined 值
        if (aValue == null && bValue == null) {
          comparison = 0;
        } else if (aValue == null) {
          comparison = 1; // null 值排在后面
        } else if (bValue == null) {
          comparison = -1; // null 值排在后面
        } else {
          // 根据排序方向进行比较
          if (direction === "descending") {
            // 降序：大的值排在前面
            if (typeof aValue === "number" && typeof bValue === "number") {
              comparison = (bValue as number) - (aValue as number);
            } else {
              // 字符串降序比较
              const aStr = String(aValue);
              const bStr = String(bValue);
              comparison = bStr < aStr ? -1 : bStr > aStr ? 1 : 0;
            }
          } else {
            // 升序：小的值排在前面
            if (typeof aValue === "number" && typeof bValue === "number") {
              comparison = (aValue as number) - (bValue as number);
            } else {
              // 字符串升序比较
              const aStr = String(aValue);
              const bStr = String(bValue);
              comparison = aStr < bStr ? -1 : aStr > bStr ? 1 : 0;
            }
          }
        }

        // 如果当前列的比较结果不为 0，则返回结果
        if (comparison !== 0) {
          return comparison;
        }

        // 如果当前列相等，继续比较下一列
      }

      return 0;
    });
  }

  /**
   * 构建顶层 attributes
   */
  private _buildTopLevelAttributes(
    allAttributes: any[],
    keys: string[]
  ): any[] {
    const topLevelAttributes: any[] = [];

    // 添加非键的聚合属性
    allAttributes.forEach((attr) => {
      if (!keys.includes(attr.name)) {
        topLevelAttributes.push(attr);
      }
    });

    // 添加 SPLIT 属性
    topLevelAttributes.push({
      name: "SPLIT",
      type: "DATASET",
    });

    return topLevelAttributes;
  }

  /**
   * 构建 SPLIT 层级的 attributes
   */
  private _buildSplitAttributes(
    allAttributes: any[],
    splitKey: string,
    keys: string[],
    currentLevel: number
  ): any[] {
    const splitAttributes: any[] = [];

    // 添加当前分割键的属性
    const keyAttribute = allAttributes.find((attr) => attr.name === splitKey);
    if (keyAttribute) {
      splitAttributes.push(keyAttribute);
    }

    // 添加聚合属性
    allAttributes.forEach((attr) => {
      if (attr.type === "NUMBER") {
        splitAttributes.push(attr);
      }
    });

    // 只有在不是最深层级时才添加 SPLIT 属性
    if (currentLevel + 1 < keys.length) {
      splitAttributes.push({
        name: "SPLIT",
        type: "DATASET",
      });
    }

    return splitAttributes;
  }

  /**
   * 从 splitExpressions 生成 inflaters 数组
   * 参考 _computeResolved 流程中 External.getInteligentInflater 的实现方式
   * 根据 expression 类型（TimeBucket, NumberBucket, BOOLEAN, NUMBER, TIME 等）
   * 使用相应的 inflater 工厂函数生成正确的 inflater
   */
  private _generateInflaters(
    query: any,
    splitExpressions: Map<string, Expression>
  ): any[] {
    const inflaters: any[] = [];

    if (!query.dimensions || !Array.isArray(query.dimensions)) {
      return inflaters;
    }

    query.dimensions.forEach((dimension: any) => {
      // 提取维度的输出名称（移除 dummy 前缀）
      const rawName = dimension.outputName || dimension.dimension; // 这里可能包含 '***' 前缀
      const labelForMap = this._stripDummyPrefix(rawName); // 用于在 splitExpressions 中查找表达式
      const labelForInflater = rawName; // 保持与查询返回的数据字段名一致，避免读取不到值

      // 从保存的 splitExpressions 中找到对应的 expression
      const expression = splitExpressions.get(labelForMap);

      if (expression) {
        // 使用 External.getInteligentInflater 生成 inflater
        // 这个方法会根据 expression 类型自动选择：
        // - NumberBucketExpression -> numberRangeInflaterFactory
        // - TimeBucketExpression -> timeRangeInflaterFactory
        // - BOOLEAN/NUMBER/TIME 等 -> 对应的 simpleInflater
        const inflater = External.getInteligentInflater(
          expression,
          labelForInflater
        );
        if (inflater) {
          inflaters.push(inflater);
        }
      }
    });

    return inflaters;
  }

  /**
   * 从上下文中找到 DruidExternal 数据源
   */
  private _findDruidExternal(context: Datum): any {
    for (const key in context) {
      const value = context[key];
      if (
        value &&
        value.constructor &&
        value.constructor.name === "DruidExternal"
      ) {
        return value;
      }
    }
    return null;
  }
}

export abstract class ChainableExpression extends Expression {
  static jsToValue(js: ExpressionJS): ExpressionValue {
    let value = Expression.jsToValue(js);
    value.operand = js.operand ? Expression.fromJS(js.operand) : Expression._;
    return value;
  }

  public operand: Expression;

  constructor(value: ExpressionValue, dummy: any = null) {
    super(value, dummy);
    this.operand = value.operand || Expression._;
  }

  protected _checkTypeAgainstTypes(
    name: string,
    type: string,
    neededTypes: string[]
  ) {
    if (type && type !== "NULL" && neededTypes.indexOf(type) === -1) {
      if (neededTypes.length === 1) {
        throw new Error(
          `${this.op} must have ${name} of type ${neededTypes[0]} (is ${type})`
        );
      } else {
        throw new Error(
          `${this.op} must have ${name} of type ${neededTypes.join(
            " or "
          )} (is ${type})`
        );
      }
    }
  }

  protected _checkOperandTypes(...neededTypes: string[]) {
    this._checkTypeAgainstTypes(
      "operand",
      Set.unwrapSetType(this.operand.type),
      neededTypes
    );
  }

  protected _checkOperandTypesStrict(...neededTypes: string[]) {
    this._checkTypeAgainstTypes("operand", this.operand.type, neededTypes);
  }

  protected _bumpOperandToTime() {
    if (this.operand.type === "STRING") {
      this.operand = this.operand.upgradeToType("TIME");
    }
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.operand = this.operand;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    if (!this.operand.equals(Expression._)) {
      js.operand = this.operand.toJS();
    }
    return js;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [];
  }

  public toString(indent?: int): string {
    return `${this.operand.toString(indent)}.${
      this.op
    }(${this._toStringParameters(indent).join(",")})`;
  }

  public equals(other: ChainableExpression | undefined): boolean {
    return super.equals(other) && this.operand.equals(other.operand);
  }

  public changeOperand(operand: Expression): this {
    if (this.operand === operand || this.operand.equals(operand)) return this;

    let value = this.valueOf();
    value.operand = operand;
    delete value.simple;
    return Expression.fromValue(value) as any;
  }

  public swapWithOperand(): ChainableExpression {
    const { operand } = this;
    if (operand instanceof ChainableExpression) {
      return operand.changeOperand(this.changeOperand(operand.operand));
    } else {
      throw new Error("operand must be chainable");
    }
  }

  public getAction(): Expression {
    return this.changeOperand(Expression._);
  }

  public getHeadOperand(): Expression {
    let iter = this.operand;
    while (iter instanceof ChainableExpression) iter = iter.operand;
    return iter;
  }

  public getArgumentExpressions(): Expression[] {
    return [];
  }

  public expressionCount(): int {
    let sum = super.expressionCount() + this.operand.expressionCount();
    this.getArgumentExpressions().forEach(
      (ex) => (sum += ex.expressionCount())
    );
    return sum;
  }

  public argumentsResolved(): boolean {
    return this.getArgumentExpressions().every((ex) => ex.resolved());
  }

  public argumentsResolvedWithoutExternals(): boolean {
    return this.getArgumentExpressions().every((ex) =>
      ex.resolvedWithoutExternals()
    );
  }

  public getFn(): ComputeFn {
    // ToDo: this should be moved into Expression
    return (d: Datum) => this.calc(d);
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    throw runtimeAbstract();
  }

  public fullyDefined(): boolean {
    return this.operand.isOp("literal");
  }

  public calc(datum: Datum): PlywoodValue {
    return this._calcChainableHelper(this.operand.calc(datum));
  }

  protected _getJSChainableHelper(operandJS: string): string {
    throw runtimeAbstract();
  }

  public getJS(datumVar: string): string {
    return this._getJSChainableHelper(this.operand.getJS(datumVar));
  }

  protected _getSQLChainableHelper(
    dialect: SQLDialect,
    operandSQL: string
  ): string {
    throw runtimeAbstract();
  }

  public getSQL(dialect: SQLDialect): string {
    return this._getSQLChainableHelper(dialect, this.operand.getSQL(dialect));
  }

  public pushIntoExternal(): ExternalExpression | null {
    const { operand } = this;
    if (operand instanceof ExternalExpression) {
      return operand.addExpression(this.getAction());
    }
    return null;
  }

  protected specialSimplify(): Expression {
    return this;
  }

  public simplify(): Expression {
    if (this.simple) return this;

    let simpler: Expression = this.changeOperand(this.operand.simplify());

    if (simpler.fullyDefined()) {
      return r(simpler.calc({}));
    }

    const specialSimpler = (
      simpler as ChainableUnaryExpression
    ).specialSimplify();
    if (specialSimpler === simpler) {
      simpler = specialSimpler.markSimple();
    } else {
      simpler = specialSimpler.simplify();
    }

    if (simpler instanceof ChainableExpression) {
      const pushedInExternal = simpler.pushIntoExternal();
      if (pushedInExternal) return pushedInExternal;
    }

    return simpler;
  }

  public isNester(): boolean {
    return false;
  }

  public _everyHelper(
    iter: BooleanExpressionIterator,
    thisArg: any,
    indexer: Indexer,
    depth: int,
    nestDiff: int
  ): boolean {
    let pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
    if (pass != null) {
      return pass;
    } else {
      indexer.index++;
    }
    depth++;

    let operand = this.operand;
    if (!operand._everyHelper(iter, thisArg, indexer, depth, nestDiff))
      return false;

    let nestDiffNext = nestDiff + Number(this.isNester());
    return this.getArgumentExpressions().every((ex) =>
      ex._everyHelper(iter, thisArg, indexer, depth, nestDiffNext)
    );
  }

  public _substituteHelper(
    substitutionFn: SubstitutionFn,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
    typeContext: DatasetFullType
  ): ExpressionTypeContext {
    let sub = substitutionFn.call(
      this,
      this,
      indexer.index,
      depth,
      nestDiff,
      typeContext
    );
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext),
      };
    } else {
      indexer.index++;
    }
    depth++;

    const operandSubs = this.operand._substituteHelper(
      substitutionFn,
      indexer,
      depth,
      nestDiff,
      typeContext
    );
    const updatedThis = this.changeOperand(operandSubs.expression);

    return {
      expression: updatedThis,
      typeContext: updatedThis.updateTypeContextIfNeeded(
        operandSubs.typeContext
      ),
    };
  }
}

export abstract class ChainableUnaryExpression extends ChainableExpression {
  static jsToValue(js: ExpressionJS): ExpressionValue {
    let value = ChainableExpression.jsToValue(js);
    value.expression = Expression.fromJS(js.expression);
    return value;
  }

  public expression: Expression;

  constructor(value: ExpressionValue, dummy: any = null) {
    super(value, dummy);
    if (!value.expression) throw new Error(`must have an expression`);
    this.expression = value.expression;
  }

  protected _checkExpressionTypes(...neededTypes: string[]) {
    this._checkTypeAgainstTypes(
      "expression",
      Set.unwrapSetType(this.expression.type),
      neededTypes
    );
  }

  protected _checkExpressionTypesStrict(...neededTypes: string[]) {
    this._checkTypeAgainstTypes(
      "expression",
      this.expression.type,
      neededTypes
    );
  }

  protected _checkOperandExpressionTypesAlign() {
    let operandType = Set.unwrapSetType(this.operand.type);
    let expressionType = Set.unwrapSetType(this.expression.type);
    if (
      !operandType ||
      operandType === "NULL" ||
      !expressionType ||
      expressionType === "NULL" ||
      operandType === expressionType
    )
      return;
    throw new Error(
      `${this.op} must have matching types (are ${this.operand.type}, ${this.expression.type})`
    );
  }

  protected _bumpOperandExpressionToTime() {
    if (this.expression.type === "TIME" && this.operand.type === "STRING") {
      this.operand = this.operand.upgradeToType("TIME");
    }

    if (this.operand.type === "TIME" && this.expression.type === "STRING") {
      this.expression = this.expression.upgradeToType("TIME");
    }
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.expression = this.expression;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.expression = this.expression.toJS();
    return js;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.expression.toString(indent)];
  }

  public toString(indent?: int): string {
    // ToDo: handle indent
    return `${this.operand.toString(indent)}.${
      this.op
    }(${this._toStringParameters(indent).join(",")})`;
  }

  public equals(other: ChainableUnaryExpression | undefined): boolean {
    return super.equals(other) && this.expression.equals(other.expression);
  }

  public changeExpression(expression: Expression): this {
    if (this.expression === expression || this.expression.equals(expression))
      return this;

    let value = this.valueOf();
    value.expression = expression;
    delete value.simple;
    return Expression.fromValue(value) as any;
  }

  protected _calcChainableUnaryHelper(
    operandValue: any,
    expressionValue: any
  ): PlywoodValue {
    throw runtimeAbstract();
  }

  public fullyDefined(): boolean {
    return this.operand.isOp("literal") && this.expression.isOp("literal");
  }

  public calc(datum: Datum): PlywoodValue {
    return this._calcChainableUnaryHelper(
      this.operand.calc(datum),
      this.isNester() ? null : this.expression.calc(datum)
    );
  }

  protected _getJSChainableUnaryHelper(
    operandJS: string,
    expressionJS: string
  ): string {
    throw runtimeAbstract();
  }

  public getJS(datumVar: string): string {
    return this._getJSChainableUnaryHelper(
      this.operand.getJS(datumVar),
      this.expression.getJS(datumVar)
    );
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string
  ): string {
    throw runtimeAbstract();
  }

  public getSQL(dialect: SQLDialect): string {
    return this._getSQLChainableUnaryHelper(
      dialect,
      this.operand.getSQL(dialect),
      this.expression.getSQL(dialect)
    );
  }

  public getExpressionList(): Expression[] {
    const { op, operand, expression } = this;
    let expressionList = [expression];
    let iter = operand;
    while (iter.op === op) {
      expressionList.unshift((iter as ChainableUnaryExpression).expression);
      iter = (iter as ChainableUnaryExpression).operand;
    }
    expressionList.unshift(iter);
    return expressionList;
  }

  public isCommutative(): boolean {
    return false;
  }

  public isAssociative(): boolean {
    return false;
  }

  public associateLeft(): this | null {
    if (!this.isAssociative()) return null;
    const { op, operand, expression } = this;
    if (op !== expression.op) return null;
    const MyClass: any = this.constructor;

    return new MyClass({
      operand: new MyClass({
        operand: operand,
        expression: (expression as ChainableUnaryExpression).operand,
      }),
      expression: (expression as ChainableUnaryExpression).expression,
    });
  }

  public associateRightIfSimpler(): this | null {
    if (!this.isAssociative()) return null;
    const { op, operand, expression } = this;
    if (op !== operand.op) return null;
    const MyClass: any = this.constructor;

    const simpleExpression = new MyClass({
      operand: (operand as ChainableUnaryExpression).expression,
      expression: expression,
    }).simplify();

    if (simpleExpression instanceof LiteralExpression) {
      return new MyClass({
        operand: (operand as ChainableUnaryExpression).operand,
        expression: simpleExpression,
      }).simplify();
    } else {
      return null;
    }
  }

  public pushIntoExternal(): ExternalExpression | null {
    const { operand, expression } = this;
    if (operand instanceof ExternalExpression) {
      return operand.addExpression(this.getAction());
    }
    if (expression instanceof ExternalExpression) {
      return expression.prePush(this.changeExpression(Expression._));
    }
    return null;
  }

  public simplify(): Expression {
    if (this.simple) return this;

    let simpleOperand = this.operand.simplify();
    let simpleExpression = this.expression.simplify();
    let simpler: Expression =
      this.changeOperand(simpleOperand).changeExpression(simpleExpression);
    if (simpler.fullyDefined()) return r(simpler.calc({}));

    if (this.isCommutative() && simpleOperand instanceof LiteralExpression) {
      // Swap!
      const MyClass: any = this.constructor;
      const myValue = this.valueOf();
      myValue.operand = simpleExpression;
      myValue.expression = simpleOperand;
      return new MyClass(myValue).simplify();
    }

    // Auto associate left if possible
    let assLeft = (simpler as ChainableUnaryExpression).associateLeft();
    if (assLeft) return assLeft.simplify();

    if (simpler instanceof ChainableUnaryExpression) {
      let specialSimpler = simpler.specialSimplify();
      if (specialSimpler !== simpler) {
        return specialSimpler.simplify();
      } else {
        simpler = specialSimpler;
      }

      if (simpler instanceof ChainableUnaryExpression) {
        // Try to associate right and pick that if that is simpler
        let assRight = simpler.associateRightIfSimpler();
        if (assRight) return assRight;
      }
    }

    simpler = simpler.markSimple();

    if (simpler instanceof ChainableExpression) {
      const pushedInExternal = simpler.pushIntoExternal();
      if (pushedInExternal) return pushedInExternal;
    }

    return simpler;
  }

  public getArgumentExpressions(): Expression[] {
    return [this.expression];
  }

  public _substituteHelper(
    substitutionFn: SubstitutionFn,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
    typeContext: DatasetFullType
  ): ExpressionTypeContext {
    let sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff);
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext),
      };
    } else {
      indexer.index++;
    }
    depth++;

    const operandSubs = this.operand._substituteHelper(
      substitutionFn,
      indexer,
      depth,
      nestDiff,
      typeContext
    );
    const nestDiffNext = nestDiff + Number(this.isNester());
    const expressionSubs = this.expression._substituteHelper(
      substitutionFn,
      indexer,
      depth,
      nestDiffNext,
      this.isNester() ? operandSubs.typeContext : typeContext
    );
    const updatedThis = this.changeOperand(
      operandSubs.expression
    ).changeExpression(expressionSubs.expression);

    return {
      expression: updatedThis,
      typeContext: updatedThis.updateTypeContextIfNeeded(
        operandSubs.typeContext,
        expressionSubs.typeContext
      ),
    };
  }
}

/*
 * Copyright 2016-2019 Imply Data, Inc.
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

import { PlywoodValue, Set } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class DivideExpression extends ChainableUnaryExpression {
  static op = "Divide";
  static fromJS(parameters: ExpressionJS): DivideExpression {
    return new DivideExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("divide");
    this._checkOperandTypes('NUMBER');
    this._checkExpressionTypes('NUMBER');
    this.type = 'NUMBER';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (operandValue === null || expressionValue === null) return null;
    return Set.crossBinary(operandValue, expressionValue, (a, b) => a === 0 ? 0 : b === 0 ? Number.MAX_SAFE_INTEGER : a / b);
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    return `(_o=${operandJS},_e=${expressionJS},((_o===0||isNaN(_o)?0:(_e===0||isNaN(_e)?${Number.MAX_SAFE_INTEGER}:${operandJS}/${expressionJS})))`;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return `(CASE WHEN ${operandSQL} = '0' THEN 0 CASE WHEN ${expressionSQL} = '0' THEN ${Number.MAX_SAFE_INTEGER} ELSE (${operandSQL}/${expressionSQL}) END)`;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;

    // 0 / X
    if (operand.equals(Expression.ZERO)) return Expression.ZERO;

    // X / 0
    if (expression.equals(Expression.ZERO)) return Expression.MAX_SAFE_INTEGER;

    // X / 1
    if (expression.equals(Expression.ONE)) return operand;

    return this;
  }
}

Expression.register(DivideExpression);

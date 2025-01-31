/*
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

import * as hasOwnProp from "has-own-prop";
import { ReadableStream, WritableStream } from "readable-stream";

export function repeat(str: string, times: int): string {
  return new Array(times + 1).join(str);
}

export function indentBy(str: string, indent: int): string {
  const spaces = repeat(" ", indent);
  return str
    .split("\n")
    .map((x) => spaces + x)
    .join("\n");
}

export function dictEqual(
  dictA: Record<string, any>,
  dictB: Record<string, any>
): boolean {
  if (dictA === dictB) return true;
  if (!dictA !== !dictB) return false;
  let keys = Object.keys(dictA);
  if (keys.length !== Object.keys(dictB).length) return false;
  for (let key of keys) {
    if (dictA[key] !== dictB[key]) return false;
  }
  return true;
}

export function shallowCopy<T>(thing: T): T {
  let newThing: any = {};
  for (let k in thing) {
    if (hasOwnProp(thing, k)) newThing[k] = (thing as any)[k];
  }
  return newThing;
}

export function deduplicateSort(a: string[]): string[] {
  a = a.sort();
  let newA: string[] = [];
  let last: string = null;
  for (let v of a) {
    if (v !== last) newA.push(v);
    last = v;
  }
  return newA;
}

export function mapLookup<T, U>(
  thing: Record<string, T>,
  fn: (x: T) => U
): Record<string, U> {
  let newThing: Record<string, U> = Object.create(null);
  for (let k in thing) {
    if (hasOwnProp(thing, k)) newThing[k] = fn(thing[k]);
  }
  return newThing;
}

export function emptyLookup(lookup: Record<string, any>): boolean {
  for (let k in lookup) {
    if (hasOwnProp(lookup, k)) return false;
  }
  return true;
}

export function nonEmptyLookup(lookup: Record<string, any>): boolean {
  return !emptyLookup(lookup);
}

export function clip(x: number): number {
  const rx = Math.round(x);
  return Math.abs(x - rx) < 1e-5 ? rx : x;
}

export function safeAdd(num: number, delta: number): number {
  let stringDelta = String(delta);
  let dotIndex = stringDelta.indexOf(".");
  if (dotIndex === -1 || stringDelta.length === 18) {
    return num + delta;
  } else {
    let scale = Math.pow(10, stringDelta.length - dotIndex - 1);
    return (num * scale + delta * scale) / scale;
  }
}

export function safeRange(
  num: number,
  delta: number
): { start: number; end: number } {
  let stringDelta = String(delta);
  let dotIndex = stringDelta.indexOf(".");
  if (dotIndex === -1 || stringDelta.length === 18) {
    return {
      start: num,
      end: num + delta,
    };
  } else {
    let scale = Math.pow(10, stringDelta.length - dotIndex - 1);
    num = clip(num * scale) / scale;
    return {
      start: num,
      end: (num * scale + delta * scale) / scale,
    };
  }
}

export function continuousFloorExpression(
  variable: string,
  floorFn: string,
  size: number,
  offset: number
): string {
  let expr = variable;
  if (offset !== 0) {
    expr = expr + " - " + offset;
  }
  if (offset !== 0 && size !== 1) {
    expr = "(" + expr + ")";
  }
  if (size !== 1) {
    expr = expr + " / " + size;
  }
  expr = floorFn + "(" + expr + ")";
  if (size !== 1) {
    expr = expr + " * " + size;
  }
  if (offset !== 0) {
    expr = expr + " + " + offset;
  }
  return expr;
}

// Taken from: https://stackoverflow.com/questions/31089801/extending-error-in-javascript-with-es6-syntax
export class ExtendableError extends Error {
  public stack: string;

  constructor(message: string) {
    super(message);
    this.name = (this.constructor as any).name;
    this.message = message;
    if (typeof (Error as any).captureStackTrace === "function") {
      (Error as any).captureStackTrace(this, this.constructor);
    } else {
      this.stack = (new Error(message) as any).stack;
    }
  }
}

export function pluralIfNeeded(n: number, thing: string): string {
  return `${n} ${thing}${n === 1 ? "" : "s"}`;
}

export function pipeWithError(src: ReadableStream, dest: WritableStream): any {
  src.pipe(dest);
  src.on("error", (e: Error) => dest.emit("error", e));
  return dest;
}

export function formatDateTimeForLog(date: Date) {
  // 获取年、月、日、时、分、秒
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0"); // 月份从0开始，需要加1
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");

  // 拼接成格式化的字符串
  const formattedDateTime = `${year}/${month}/${day}-${hours}:${minutes}:${seconds}`;

  return formattedDateTime;
}

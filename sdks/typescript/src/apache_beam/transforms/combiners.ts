/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { CombineFn } from "./group_and_combine";
import { Coder } from "../coders/coders";
import { VarIntCoder } from "../coders/standard_coders";
import { requireForSerialization } from "../serialization";
import { packageName } from "../utils/packageJson";

// TODO(cleanup): These reductions only work on Arrays, not Iterables.

export const count: CombineFn<any, number, number> = {
  createAccumulator: () => 0,
  addInput: (acc, i) => acc + 1,
  mergeAccumulators: (accumulators: number[]) =>
    accumulators.reduce((prev, current) => prev + current),
  extractOutput: (acc) => acc,
  accumulatorCoder: () => new VarIntCoder(),
};

export const sum: CombineFn<number, number, number> = {
  createAccumulator: () => 0,
  addInput: (acc: number, i: number) => acc + i,
  mergeAccumulators: (accumulators: number[]) =>
    accumulators.reduce((prev, current) => prev + current),
  extractOutput: (acc: number) => acc,
  accumulatorCoder: (inputCoder: Coder<number>) => inputCoder,
};

export const max: CombineFn<any, any, any> = {
  createAccumulator: () => undefined,
  addInput: (acc: any, i: any) => (acc === undefined || acc < i ? i : acc),
  mergeAccumulators: (accumulators: any[]) =>
    accumulators
      .filter((x) => x !== undefined)
      .reduce((a, b) => (a > b ? a : b), undefined),
  extractOutput: (acc: any) => acc,
};

export const min: CombineFn<any, any, any> = {
  createAccumulator: () => undefined,
  addInput: (acc: any, i: any) => (acc === undefined || acc > i ? i : acc),
  mergeAccumulators: (accumulators: any[]) =>
    accumulators
      .filter((x) => x !== undefined)
      .reduce((a, b) => (a < b ? a : b), undefined),
  extractOutput: (acc: any) => acc,
};

export const mean: CombineFn<number, [number, number], number> = {
  createAccumulator: () => [0, 0],
  addInput: ([sum, count]: [number, number], i: number) => [sum + i, count + 1],
  mergeAccumulators: (accumulators: [number, number][]) =>
    accumulators.reduce(([sum0, count0], [sum1, count1]) => [
      sum0 + sum1,
      count0 + count1,
    ]),
  extractOutput: ([sum, count]: [number, number]) => sum / count,
};

requireForSerialization(`${packageName}/transforms/combiners`, exports);

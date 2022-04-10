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

export class CountFn implements CombineFn<any, number, number> {
  createAccumulator() {
    return 0;
  }
  addInput(acc: number, i: any) {
    return acc + 1;
  }
  mergeAccumulators(accumulators: number[]) {
    return accumulators.reduce((prev, current) => prev + current);
  }
  extractOutput(acc: number) {
    return acc;
  }
}

export class SumFn implements CombineFn<number, number, number> {
  createAccumulator() {
    return 0;
  }
  addInput(acc: number, i: number) {
    return acc + i;
  }
  mergeAccumulators(accumulators: number[]) {
    return accumulators.reduce((prev, current) => prev + current);
  }
  extractOutput(acc: number) {
    return acc;
  }
}

export class MaxFn implements CombineFn<any, any, any> {
  createAccumulator() {
    return null;
  }
  addInput(acc: any, i: any) {
    if (acc == null || acc < i) {
      return i;
    } else {
      return acc;
    }
  }
  mergeAccumulators(accumulators: any[]) {
    return accumulators.reduce((a, b) => (a > b ? a : b));
  }
  extractOutput(acc: any) {
    return acc;
  }
}

export class MeanFn implements CombineFn<number, [number, number], number> {
  createAccumulator() {
    return [0, 0] as [number, number];
  }
  addInput(acc: [number, number], i: number) {
    return [acc[0] + i, acc[1] + 1] as [number, number];
  }
  mergeAccumulators(accumulators: [number, number][]) {
    return accumulators.reduce(([sum0, count0], [sum1, count1]) => [
      sum0 + sum1,
      count0 + count1,
    ]);
  }
  extractOutput(acc: number) {
    return acc[0] / acc[1];
  }
}

import { requireForSerialization } from "../serialization";
requireForSerialization("apache_beam.transforms.combiners", exports);

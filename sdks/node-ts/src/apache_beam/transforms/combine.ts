// XXX rename to combiners or combineFns.

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

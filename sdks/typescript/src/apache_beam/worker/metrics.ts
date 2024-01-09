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

import * as protobufjs from "protobufjs";

import { MonitoringInfo } from "../proto/metrics";
import { Coder, Context } from "../coders/coders";
import { IterableCoder } from "../coders/required_coders";
import { VarIntCoder } from "../coders/standard_coders";

export interface MetricSpec {
  metricType: string;
  name: string;
}

const metricTypes = new Map<
  string,
  (spec: MetricSpec) => MetricCell<unknown>
>();

/**
 * A MetricCell holds the concrete value of a metric at runtime.
 *
 * Different types of metrics will have different types of MetricCells,
 * with the relationship being stored in the metricTypes map.
 */
interface MetricCell<T> {
  // This is the publicly available function at execution time.
  update: (value: T) => void;

  // Rather than clear and re-create counters every time, we reset them
  // between bundles.
  reset: () => void;

  // These are used to pass counters back from the worker.
  toEmptyMonitoringInfo: () => MonitoringInfo;
  payload: () => Uint8Array;

  // These are used for reading counters.
  loadFromPayload: (paylaod: Uint8Array) => void;
  merge(other: MetricCell<T>);
  extract(): any;
}

class Counter implements MetricCell<number> {
  private value = 0;

  constructor(spec: MetricSpec) {}

  update(value: number) {
    this.value += value;
  }

  reset(): void {
    this.value = 0;
  }

  toEmptyMonitoringInfo(): MonitoringInfo {
    return MonitoringInfo.create({
      urn: "beam:metric:user:sum_int64:v1",
      type: "beam:metrics:sum_int64:v1",
    });
  }

  payload(): Uint8Array {
    return encode(this.value, new VarIntCoder());
  }

  loadFromPayload(payload: Uint8Array) {
    this.value = decode(payload, new VarIntCoder());
  }

  merge(other) {
    this.value += other.value;
  }

  extract() {
    return this.value;
  }
}

metricTypes.set("beam:metric:user:sum_int64:v1", (spec) => new Counter(spec));

class Distribution implements MetricCell<number> {
  private count = 0;
  private sum = 0;
  private min = 0;
  private max = 0;

  private coder = new IterableCoder(new VarIntCoder());

  constructor(spec: MetricSpec) {}

  update(value: number) {
    if (this.count == 0) {
      this.count = 1;
      this.sum = this.min = this.max = value;
    } else {
      this.count += 1;
      this.sum += value;
      this.min = Math.min(value, this.min);
      this.max = Math.max(value, this.max);
    }
  }

  reset(): void {
    this.count = 0;
  }

  toEmptyMonitoringInfo(): MonitoringInfo {
    return MonitoringInfo.create({
      urn: "beam:metric:user:distribution_int64:v1",
      type: "beam:metrics:distribution_int64:v1",
    });
  }

  payload(): Uint8Array {
    return encode([this.count, this.sum, this.min, this.max], this.coder);
  }

  loadFromPayload(payload: Uint8Array) {
    [this.count, this.sum, this.min, this.max] = decode(payload, this.coder);
  }

  merge(other) {
    if (other.count == 0) {
      return;
    } else if (this.count == 0) {
      this.count = other.count;
      this.sum = other.sum;
      this.min = other.min;
      this.max = other.max;
    } else {
      this.count += other.count;
      this.sum += other.sum;
      this.min = Math.min(this.min, other.min);
      this.max = Math.max(this.max, other.max);
    }
  }

  extract() {
    if (this.count == 0) {
      return {
        count: 0,
        sum: 0,
        min: NaN,
        max: NaN,
      };
    } else {
      return {
        count: this.count,
        sum: this.sum,
        min: this.min,
        max: this.max,
      };
    }
  }
}

metricTypes.set(
  "beam:metric:user:distribution_int64:v1",
  (spec) => new Distribution(spec),
);

class ElementCount extends Counter {
  toEmptyMonitoringInfo(): MonitoringInfo {
    return MonitoringInfo.create({
      urn: "beam:metric:element_count:v1",
      type: "beam:metrics:sum_int64:v1",
    });
  }
}

metricTypes.set(
  "beam:metric:user:element_count:v1",
  (spec) => new ElementCount(spec),
);

/**
 * A ScopedMetricCell is a MetricCell together with its identifier(s).
 */
class ScopedMetricCell<T> {
  is_set: boolean;

  constructor(
    public key: string,
    public value: MetricCell<unknown>,
    public name?: string,
    public transformId?: string,
    public pcollectionId?: string,
  ) {
    this.is_set = false;
  }

  toEmptyMonitoringInfo(): MonitoringInfo {
    const info = this.value.toEmptyMonitoringInfo();
    if (this.name) {
      info.labels["NAME"] = this.name;
      info.labels["NAMESPACE"] = "";
    }
    if (this.transformId) {
      info.labels["TRANSFORM"] = this.transformId;
    }
    if (this.pcollectionId) {
      info.labels["PCOLLECTION"] = this.pcollectionId;
    }
    return info;
  }

  update(value: T) {
    this.is_set = true;
    this.value.update(value);
  }

  reset() {
    this.is_set = false;
  }
}

/**
 * A MetricsContainer holds a set of metrics for a particular bundle.
 *
 * It can be re-used, but must be reset() between bundles.
 */
export class MetricsContainer {
  metrics = new Map<string, ScopedMetricCell<unknown>>();

  getMetric(
    transformId: string | undefined,
    pcollectionId: string | undefined,
    spec: MetricSpec,
  ): ScopedMetricCell<unknown> {
    const key =
      spec.metricType +
      (transformId ? transformId.length + transformId : "_") +
      (pcollectionId ? pcollectionId.length + pcollectionId : "_") +
      spec.name;
    var cell = this.metrics.get(key);
    if (cell === undefined) {
      cell = new ScopedMetricCell(
        key,
        createMetric(spec),
        spec.name,
        transformId,
        pcollectionId,
      );
      this.metrics.set(key, cell);
    }
    return cell;
  }

  elementCountMetric(pcollectionId: string) {
    return this.getMetric(undefined, pcollectionId, {
      name: undefined!,
      metricType: "beam:metric:user:element_count:v1",
    });
  }

  monitoringData(shortIdCache: MetricsShortIdCache): Map<string, Uint8Array> {
    return new Map(
      Array.from(this.metrics.values())
        .filter((metric) => metric.is_set)
        .map((metric) => [
          shortIdCache.getShortId(metric),
          metric.value.payload(),
        ]),
    );
  }

  reset(): void {
    for (const [_, cell] of this.metrics) {
      cell.reset();
    }
  }
}

function createMetric(spec: MetricSpec): MetricCell<unknown> {
  const constructor = metricTypes.get(spec.metricType);
  if (constructor === undefined) {
    throw new Error("Unknown metric type: " + spec.metricType);
  }
  return constructor(spec);
}

export class MetricsShortIdCache {
  private idToInfo = new Map<string, MonitoringInfo>();
  private keyToId = new Map<string, string>();

  getShortId(metric: ScopedMetricCell<unknown>): string {
    var shortId = this.keyToId.get(metric.key);
    if (shortId === undefined) {
      shortId = "m" + this.keyToId.size;
      this.idToInfo.set(shortId, metric.toEmptyMonitoringInfo());
      this.keyToId.set(metric.key, shortId);
    }
    return shortId;
  }

  asMonitoringInfo(
    id: string,
    payload: Uint8Array | undefined = undefined,
  ): MonitoringInfo {
    const result = this.idToInfo.get(id);
    if (payload !== undefined) {
      return { ...result!, payload };
    } else {
      return result!;
    }
  }
}

export function aggregateMetrics(
  infos: MonitoringInfo[],
  urn: string,
): Map<string, any> {
  const cells = new Map<string, MetricCell<any>>();
  for (const info of infos) {
    if (info.urn == urn) {
      const name = info.labels.NAME;
      const cell = createMetric({ metricType: info.urn, name });
      cell.loadFromPayload(info.payload);
      if (cells.has(name)) {
        cells.get(name)!.merge(cell);
      } else {
        cells.set(name, cell);
      }
    }
  }
  return new Map<string, any>(
    Array.from(cells.entries()).map(([name, cell]) => [name, cell.extract()]),
  );
}

function encode<T>(value: T, coder: Coder<T>) {
  const buffer = new protobufjs.Writer();
  coder.encode(value, buffer, Context.needsDelimiters);
  return buffer.finish();
}

function decode<T>(encoded: Uint8Array, coder: Coder<T>): T {
  return coder.decode(new protobufjs.Reader(encoded), Context.needsDelimiters);
}

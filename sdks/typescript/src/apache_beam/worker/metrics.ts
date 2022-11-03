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

/**
 * A ScopedMetricCell is a MetricCell together with its identifier(s).
 */
class ScopedMetricCell {
  constructor(
    public key: string,
    public transformId: string,
    public name: string,
    public value: MetricCell<unknown>
  ) {}

  toEmptyMonitoringInfo(): MonitoringInfo {
    const info = this.value.toEmptyMonitoringInfo();
    info.labels["NAME"] = this.name;
    info.labels["NAMESPACE"] = "";
    if (this.transformId) {
      info.labels["TRANSFORM"] = this.transformId;
    }
    return info;
  }
}

/**
 * A MetricsContainer holds a set of metrics for a particular bundle.
 *
 * It can be re-used, but must be reset() between bundles.
 */
export class MetricsContainer {
  metrics = new Map<string, ScopedMetricCell>();

  getMetric(transformId: string, spec: MetricSpec): MetricCell<unknown> {
    const key = spec.metricType + transformId.length + transformId + spec.name;
    var cell = this.metrics.get(key);
    if (cell === undefined) {
      cell = new ScopedMetricCell(
        key,
        transformId,
        spec.name,
        createMetric(spec)
      );
      this.metrics.set(key, cell);
    }
    return cell.value;
  }

  monitoringData(shortIdCache: MetricsShortIdCache): Map<string, Uint8Array> {
    return new Map(
      Array.from(this.metrics.values()).map((metric) => [
        shortIdCache.getShortId(metric),
        metric.value.payload(),
      ])
    );
  }

  reset(): void {
    for (const [_, cell] of this.metrics) {
      cell.value.reset();
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

  getShortId(metric: ScopedMetricCell): string {
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
    payload: Uint8Array | undefined = undefined
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
  urn: string
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
    Array.from(cells.entries()).map(([name, cell]) => [name, cell.extract()])
  );
}

function encode<T>(value: T, coder: Coder<T>) {
  const buffer = new protobufjs.Writer();
  coder.encode(value, buffer, Context.wholeStream);
  return buffer.finish();
}

function decode<T>(encoded: Uint8Array, coder: Coder<T>): T {
  return coder.decode(new protobufjs.Reader(encoded), Context.wholeStream);
}

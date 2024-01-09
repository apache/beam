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

import Long from "long";
import { requireForSerialization } from "./serialization";
import { packageName } from "./utils/packageJson";

export type KV<K, V> = {
  key: K;
  value: V;
};

export type Instant = Long;

export interface Window {
  maxTimestamp(): Instant;
}

export class GlobalWindow implements Window {
  maxTimestamp(): Instant {
    return Long.fromValue("9223371950454775"); // TODO: (Cleanup) Pull constant out of proto, or at least as a constant elsewhere.
  }
}

export class IntervalWindow implements Window {
  constructor(
    public start: Instant,
    public end: Instant,
  ) {}

  maxTimestamp() {
    return this.end.sub(1);
  }
}

export interface WindowedValue<T> {
  value: T;
  windows: Array<Window>;
  pane: PaneInfo;
  timestamp: Instant;
}

export interface PaneInfo {
  timing: Timing;
  index: number; // TODO: (Cleanup) should be a long, is overflow plausible?
  onTimeIndex: number; // TODO: (Cleanup) should be a long, is overflow plausible?
  isFirst: boolean;
  isLast: boolean;
}

export enum Timing {
  EARLY = "EARLY",
  ON_TIME = "ON_TIME",
  LATE = "LATE",
  UNKNOWN = "UNKNOWN",
}

requireForSerialization(`${packageName}/values`, exports);

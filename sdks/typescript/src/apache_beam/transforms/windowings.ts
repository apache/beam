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

import * as runnerApi from "../proto/beam_runner_api";
import {
  FixedWindowsPayload,
  SlidingWindowsPayload,
  SessionWindowsPayload,
} from "../proto/standard_window_fns";
import { WindowFn } from "./window";
import {
  GlobalWindowCoder,
  IntervalWindowCoder,
} from "../coders/standard_coders";
import { GlobalWindow, Instant, IntervalWindow } from "../values";
import { requireForSerialization } from "../serialization";
import { packageName } from "../utils/packageJson";

export function globalWindows(): WindowFn<GlobalWindow> {
  return {
    beamName: "globalWindows()",
    assignWindows: (Instant) => [new GlobalWindow()],
    windowCoder: () => new GlobalWindowCoder(),
    isMerging: () => false,
    assignsToOneWindow: () => true,
    toProto: () => ({
      urn: "beam:window_fn:global_windows:v1",
      payload: new Uint8Array(),
    }),
  };
}

export function fixedWindows(
  sizeSeconds: number | Long,
  offsetSeconds: Instant = Long.fromValue(0),
): WindowFn<IntervalWindow> {
  // TODO: (Cleanup) Use a time library?
  const sizeMillis = secsToMillisLong(sizeSeconds);
  const offsetMillis = secsToMillisLong(offsetSeconds);

  return {
    beamName: offsetSeconds.eq(0)
      ? `fixedWindows(${sizeSeconds})`
      : `fixedWindows(${sizeSeconds}, ${offsetSeconds})`,
    assignWindows: (t: Instant) => {
      const start = t.sub(t.sub(offsetMillis).mod(sizeMillis));
      return [new IntervalWindow(start, start.add(sizeMillis))];
    },

    windowCoder: () => new IntervalWindowCoder(),
    isMerging: () => false,
    assignsToOneWindow: () => true,

    toProto: () => ({
      urn: "beam:window_fn:fixed_windows:v1",
      payload: FixedWindowsPayload.toBinary({
        size: millisToProto(sizeMillis),
        offset: millisToProto(offsetMillis),
      }),
    }),
  };
}

export function slidingWindows(
  sizeSeconds: number | Long,
  periodSeconds: number | Long,
  offsetSeconds: Instant = Long.fromValue(0),
): WindowFn<IntervalWindow> {
  const sizeMillis = secsToMillisLong(sizeSeconds);
  const periodMillis = secsToMillisLong(periodSeconds);
  const offsetMillis = secsToMillisLong(offsetSeconds);

  return {
    beamName: offsetSeconds.eq(0)
      ? `slidingWindows(${sizeSeconds}, ${periodSeconds})`
      : `slidingWindows(${sizeSeconds}, ${periodSeconds}, ${offsetSeconds})`,
    assignWindows: (t: Instant) => {
      let start = t.sub(t.sub(offsetMillis).mod(periodMillis));
      const windows: IntervalWindow[] = [];
      while (t.compare(start.add(sizeMillis)) < 0) {
        windows.push(new IntervalWindow(start, start.add(sizeMillis)));
        start = start.sub(periodMillis);
      }
      return windows;
    },

    windowCoder: () => new IntervalWindowCoder(),
    isMerging: () => false,
    assignsToOneWindow: () => true,

    toProto: () => ({
      urn: "beam:window_fn:sliding_windows:v1",
      payload: SlidingWindowsPayload.toBinary({
        size: millisToProto(sizeMillis),
        period: millisToProto(periodMillis),
        offset: millisToProto(offsetMillis),
      }),
    }),
  };
}

export function sessions(gapSeconds: number | Long): WindowFn<IntervalWindow> {
  const gapMillis = secsToMillisLong(gapSeconds);

  return {
    beamName: `sessions(${gapSeconds})`,
    assignWindows: (t: Instant) => [new IntervalWindow(t, t.add(gapMillis))],
    windowCoder: () => new IntervalWindowCoder(),
    isMerging: () => true,
    assignsToOneWindow: () => true,

    toProto: () => ({
      urn: "beam:window_fn:session_windows:v1",
      payload: SessionWindowsPayload.toBinary({
        gapSize: millisToProto(gapMillis),
      }),
    }),
  };
}

function secsToMillisLong(secs: number | Long): Long {
  if (typeof secs === "number") {
    return Long.fromValue(secs * 1000);
  } else {
    return secs.mul(1000);
  }
}

function millisToProto(t: Long) {
  return { seconds: BigInt(t.div(1000).toString()), nanos: 0 };
}

requireForSerialization(`${packageName}/transforms/windowings`, exports);
requireForSerialization(`${packageName}/transforms/windowings`, {
  millisToProto,
});
requireForSerialization(
  `${packageName}/transforms/windowings`,
  FixedWindowsPayload,
);
requireForSerialization(
  `${packageName}/transforms/windowings`,
  SlidingWindowsPayload,
);
requireForSerialization(
  `${packageName}/transforms/windowings`,
  SessionWindowsPayload,
);

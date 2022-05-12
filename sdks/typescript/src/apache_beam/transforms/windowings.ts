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
  SessionWindowsPayload,
} from "../proto/standard_window_fns";
import { WindowFn } from "./window";
import {
  GlobalWindowCoder,
  IntervalWindowCoder,
} from "../coders/standard_coders";
import { GlobalWindow, Instant, IntervalWindow } from "../values";

export class GlobalWindows implements WindowFn<GlobalWindow> {
  assignWindows(Instant) {
    return [new GlobalWindow()];
  }
  windowCoder() {
    return new GlobalWindowCoder();
  }
  toProto() {
    return {
      urn: "beam:window_fn:global_windows:v1",
      payload: new Uint8Array(),
    };
  }
  isMerging() {
    return false;
  }
  assignsToOneWindow() {
    return true;
  }
}

export class FixedWindows implements WindowFn<IntervalWindow> {
  size: Long;
  offset: Instant; // TODO: (Cleanup) Or should this be a long as well?

  // TODO: (Cleanup) Use a time library?
  constructor(
    sizeSeconds: number | Long,
    offsetSeconds: Instant = Long.fromValue(0)
  ) {
    if (typeof sizeSeconds == "number") {
      this.size = Long.fromValue(sizeSeconds).mul(1000);
    } else {
      this.size = sizeSeconds.mul(1000);
    }
    this.offset = offsetSeconds.mul(1000);
  }

  assignWindows(t: Instant) {
    const start = t.sub(t.sub(this.offset).mod(this.size));
    return [new IntervalWindow(start, start.add(this.size))];
  }

  windowCoder() {
    return new IntervalWindowCoder();
  }

  toProto() {
    return {
      urn: "beam:window_fn:fixed_windows:v1",
      payload: FixedWindowsPayload.toBinary({
        size: millisToProto(this.size),
        offset: millisToProto(this.offset),
      }),
    };
  }

  isMerging() {
    return false;
  }

  assignsToOneWindow() {
    return true;
  }
}

export class Sessions implements WindowFn<IntervalWindow> {
  gap: Long;

  constructor(gapSeconds: number | Long) {
    if (typeof gapSeconds == "number") {
      this.gap = Long.fromValue(gapSeconds).mul(1000);
    } else {
      this.gap = gapSeconds.mul(1000);
    }
  }

  assignWindows(t: Instant) {
    return [new IntervalWindow(t, t.add(this.gap))];
  }

  windowCoder() {
    return new IntervalWindowCoder();
  }

  toProto() {
    return {
      urn: "beam:window_fn:session_windows:v1",
      payload: SessionWindowsPayload.toBinary({
        gapSize: millisToProto(this.gap),
      }),
    };
  }

  isMerging() {
    return true;
  }

  assignsToOneWindow() {
    return true;
  }
}

function millisToProto(t: Long) {
  return { seconds: BigInt(t.div(1000).toString()), nanos: 0 };
}

import { requireForSerialization } from "../serialization";
requireForSerialization("apache_beam.transforms.windowings", exports);

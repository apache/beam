import Long from "long";

import * as runnerApi from "../proto/beam_runner_api";
import {
  FixedWindowsPayload,
  SessionWindowsPayload,
} from "../proto/standard_window_fns";
import { WindowFn } from "../base";
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
  offset: Instant; // TODO: Or should this be a long as well?

  // TODO: Use a time library?
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

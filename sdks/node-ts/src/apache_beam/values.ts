import Long from "long";

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
    return Long.fromValue("9223371950454775"); // TODO: Pull constant out of proto, or at least as a constant elsewhere.
  }
}

export class IntervalWindow implements Window {
  constructor(public start: Instant, public end: Instant) {}

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
  index: number; // TODO: should be a long
  onTimeIndex: number; // TODO should be a long
  isFirst: boolean;
  isLast: boolean;
}

export enum Timing {
  EARLY = "EARLY",
  ON_TIME = "ON_TIME",
  LATE = "LATE",
  UNKNOWN = "UNKNOWN",
}

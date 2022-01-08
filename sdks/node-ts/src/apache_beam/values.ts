export type KV<K, V> = {
    key: K,
    value: V
}

export interface PaneInfo {
    timing: Timing,
    index: number, // TODO: should be a long
    nonSpeculativeIndex: number, // TODO should be a long
    isFirst: boolean,
    isLast: boolean
}

export type Instant = Long;

export interface BoundedWindow {
    maxTimestamp(): Instant
}

export interface WindowedValue<T> {
    value: T;
    windows: Array<BoundedWindow>;
    pane: PaneInfo;
    timestamp: Instant;
}

export class IntervalWindow implements BoundedWindow {
    constructor(public start: Instant, public end: Instant) {
    }

    maxTimestamp() {
        return this.end.sub(1)
    }
}

enum Timing {
    EARLY = "early",
    ON_TIME = "on_time",
    LATE = "late",
    UNKNOWN = "unknown"
}
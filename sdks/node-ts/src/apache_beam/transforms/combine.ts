import Long from "long";

import { GroupByKey, ParDo, CombineFn, PTransform, PCollection, DoFn } from "../base";
import { BoundedWindow, Instant, KV, PaneInfo } from '../values'

import { GlobalWindow, PaneInfoCoder } from '../coders/standard_coders';
import { GroupBy, keyBy } from './core'


// TODO: Where do we draw the line for top-level factories? (And for PCollection methods?)
export function countGlobally() {
    return new CombineGlobally(new CountFn());
}

export function countPerKey() {
    return new CombinePerKey(new CountFn())
}

export function combineGlobally<InputT, AccT, OutputT>(combineFn: CombineFn<InputT, AccT, OutputT>): CombineGlobally<InputT, AccT, OutputT> {
    return new CombineGlobally(combineFn);
}

export function combinePerKey<K, InputT, AccT, OutputT>(combineFn: CombineFn<InputT, AccT, OutputT>): CombinePerKey<K, InputT, AccT, OutputT> {
    return new CombinePerKey(combineFn);
}


// TODO(pabloem): Consider implementing Combines as primitives rather than with PArDos.
class CombinePerKey<K, InputT, AccT, OutputT> extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {
    combineFn: CombineFn<InputT, AccT, OutputT>
    constructor(combineFn: CombineFn<InputT, AccT, OutputT>) {
        super();
        this.combineFn = combineFn;
    }

    expand(input: PCollection<KV<any, InputT>>) {
        // TODO: Check other windowing properties as well.
        const windowingStrategy = input.pipeline!.getProto().components!.windowingStrategies[input.pipeline.getProto().components!.pcollections[input.id].windowingStrategyId];
        if (windowingStrategy?.windowFn?.urn == 'beam:window_fn:global_windows:v1') {
            return input.apply(new ParDo(new PreShuffleCombineDoFn(this.combineFn)))
                .apply(new GroupByKey())
                .apply(new ParDo(new PostShuffleCombineDoFn(this.combineFn)));
        } else {
            const combineFn = this.combineFn;
            return input
                .apply(new GroupByKey())
                .map((kv) => ({
                    key: kv.key,
                    value: combineFn.extractOutput(
                        kv.value.reduce(
                            combineFn.addInput.bind(combineFn),
                            combineFn.createAccumulator()))
                }));
        }
    }
}

class CombineGlobally<InputT, AccT, OutputT> extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
    combineFn: CombineFn<InputT, AccT, OutputT>
    constructor(combineFn: CombineFn<InputT, AccT, OutputT>) {
        super();
        this.combineFn = combineFn;
    }

    expand(input: PCollection<InputT>) {
        return input
            .map(elm => ({ key: "", value: elm }))
            .apply(new ParDo(new PreShuffleCombineDoFn(this.combineFn)))
            .apply(new GroupByKey())
            .apply(new ParDo(new PostShuffleCombineDoFn(this.combineFn)))
            .map(elm => elm.value)
    }
}

class CountFn implements CombineFn<any, number, number> {
    createAccumulator() {
        return 0
    }
    addInput(acc: number, i: any) {
        return acc + 1
    }
    mergeAccumulators(accumulators: number[]) {
        return accumulators.reduce((prev, current) => prev + current)
    }
    extractOutput(acc: number) {
        return acc
    }

}

class PreShuffleCombineDoFn<InputT, AccumT> extends DoFn<KV<any, InputT>, KV<any, AccumT>> {
    accums: Map<any, AccumT> = new Map()
    combineFn: CombineFn<InputT, AccumT, any>

    constructor(combineFn: CombineFn<InputT, AccumT, any>) {
        super();
        this.combineFn = combineFn;
    }

    *process(elm: KV<any, InputT>) {
        // TODO: Note that for non-primitives, maps have equality-on-reference, so likely little combining would happen here.
        // This should be resolved once we have combiner lifting.
        if (!this.accums[elm.key]) {
            this.accums[elm.key] = this.combineFn.createAccumulator();
        }
        this.accums[elm.key] = this.combineFn.addInput(this.accums[elm.key], elm.value);
    }

    *finishBundle() {
        for (let k in this.accums) {
            yield {
                value: { 'key': k, 'value': this.accums[k] },
                // TODO: Fix this!
                windows: [new GlobalWindow()],
                pane: PaneInfoCoder.ONE_AND_ONLY_FIRING,
                timestamp: Long.fromValue("-9223372036854775"),
            }
        }
    }
}

class PostShuffleCombineDoFn<K, AccumT, OutputT> extends DoFn<KV<K, Iterable<AccumT>>, KV<K, OutputT>> {
    combineFn: CombineFn<any, AccumT, OutputT>

    constructor(combineFn: CombineFn<any, AccumT, OutputT>) {
        super();
        this.combineFn = combineFn;
    }

    *process(elm: KV<K, Iterable<AccumT>>) {
        yield {
            key: elm.key,
            value: this.combineFn.extractOutput(this.combineFn.mergeAccumulators(elm.value)),
        };
    }
}
import { GroupByKey, ParDo, CombineFn, PTransform, PCollection, DoFn } from "../base";
import {BoundedWindow, Instant, KV, PaneInfo} from '../values'
import {GroupBy, keyBy} from './core'

export function countGlobally() {
    return new CombineGlobally(new CountFn());
}

export function countPerKey() {
    return new CombinePerKey(new CountFn())
}


// TODO(pabloem): Consider implementing Combines as primitives rather than with PArDos.
class CombinePerKey<InputT, OutputT> extends PTransform<PCollection<KV<any, InputT>>, PCollection<KV<any, OutputT>>> {
    combineFn: CombineFn<InputT, any, OutputT>
    constructor(combineFn: CombineFn<InputT, any, OutputT>) {
        super();
        this.combineFn = combineFn;
    }

    expand(input: PCollection<KV<any, InputT>>) {
        return input.apply(new ParDo(new PreShuffleCombineDoFn(this.combineFn)))
            .apply(new GroupByKey())
            .apply(new ParDo(new PostShuffleCombineDoFn(this.combineFn)))
    }
}

class CombineGlobally<InputT, OutputT> extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
    combineFn: CombineFn<InputT, any, OutputT>
    constructor(combineFn: CombineFn<InputT, any, OutputT>) {
        super();
        this.combineFn = combineFn;
    }

    expand(input: PCollection<InputT>) {
        return input
        .map(elm => ({key: "", value: elm}))
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
        return accumulators.reduce((prev, current) => prev + current)[0]
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

    process(elm: KV<any, InputT>) {
        if (!this.accums[elm.key]) {
            this.accums[elm.key] = this.combineFn.createAccumulator();
        }
        this.accums[elm.key] = this.combineFn.addInput(this.accums[elm.key], elm.value);
    }

    *finishBundle() {
        for (let k in this.accums) {
            yield {
                value: {'key': k, 'value': this.accums[k]},
                windows: <Array<BoundedWindow>><unknown>undefined,
                pane: <PaneInfo><unknown>undefined,
                timestamp: <Instant><unknown>undefined
            }
        }
    }
}

class PostShuffleCombineDoFn<AccumT, OutputT> extends DoFn<KV<any, AccumT>, KV<any, OutputT>> {
    accums: Map<any, [AccumT]> = new Map()
    combineFn: CombineFn<any, AccumT, OutputT>

    constructor(combineFn: CombineFn<any, AccumT, OutputT>) {
        super();
        this.combineFn = combineFn;
    }

    process(elm: KV<any, AccumT>) {
        if (!this.accums[elm.key]) {
            this.accums[elm.key] = []
        }
        this.accums[elm.key].push(elm.value)
    }

    *finishBundle() {
        for (let k in this.accums) {
        yield {
            value: {'key': k, 'value': this.combineFn.extractOutput(this.combineFn.mergeAccumulators(this.accums[k]))},
            windows: <Array<BoundedWindow>><unknown>undefined,
            pane: <PaneInfo><unknown>undefined,
            timestamp: <Instant><unknown>undefined
        }
    }
    }
}
import { PTransform, PCollection, Impulse, Root, CombineFn, DoFn } from "../base";
import * as translations from '../internal/translations';
import * as runnerApi from '../proto/beam_runner_api';
import { BytesCoder, KVCoder } from "../coders/standard_coders";

import { GroupByKey } from '../base'
import { GeneralObjectCoder } from "../coders/js_coders";
import { BoundedWindow, Instant, KV, PaneInfo } from "../values";
import { ParDo } from "..";

export class Create<T> extends PTransform<Root, PCollection<T>> {
    elements: T[];

    constructor(elements: T[]) {
        super("Create");
        this.elements = elements;
    }

    expand(root: Root) {
        const this_ = this;
        // TODO: Store encoded values and conditionally shuffle.
        return root
            .apply(new Impulse())
            .flatMap(function*(_) {
                yield* this_.elements
            });
    }
}

export class GroupBy extends PTransform<PCollection<any>, PCollection<KV<any, Iterable<any>>>> {
    keyFn: (element: any) => any;
    constructor(key: string | ((element: any) => any)) {
        super();
        if ((key as (element: any) => any).call !== undefined) {
            this.keyFn = key as (element: any) => any;
        } else {
            this.keyFn = function(x) { return x[key as string]; };
        }
    }

    expand(input: PCollection<any>): PCollection<KV<any, Iterable<any>>> {
        const keyFn = this.keyFn;
        return input
            .map(function(x) { return { 'key': keyFn(x), 'value': x }; })
            .apply(new GroupByKey());
    }
}

class KeyBy<InputT, KeyT> extends PTransform<PCollection<InputT>, PCollection<KV<KeyT, InputT>>> {
    keyFn: (elm: InputT) => KeyT
    constructor(keyFn: (elm: InputT) => KeyT) {
        super();
        this.keyFn = keyFn;
    }
    expand(input: PCollection<InputT>) {
        return input.map(elm => ({'key': this.keyFn(elm), 'value': elm}))
    }
}

export function keyBy<InputT, KeyT>(keyFn: (elm: InputT) => KeyT) {
    return new KeyBy(keyFn);
}
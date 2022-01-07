import { PTransform, PCollection, Impulse, Root } from "../base";
import * as translations from '../internal/translations';
import * as runnerApi from '../proto/beam_runner_api';
import { BytesCoder, KVCoder } from "../coders/standard_coders";

import { GroupByKey } from '../base'
import { GeneralObjectCoder } from "../coders/js_coders";

export class Create extends PTransform<Root, PCollection> {
    elements: any[];

    constructor(elements: any[]) {
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

export class GroupBy extends PTransform<PCollection, PCollection> {
    keyFn: (element: any) => any;
    constructor(key: string | ((element: any) => any)) {
        super();
        if ((key as (element: any) => any).call !== undefined) {
            this.keyFn = key as (element: any) => any;
        } else {
            this.keyFn = function(x) { return x[key as string]; };
        }
    }

    expand(input: PCollection): PCollection {
        const keyFn = this.keyFn;
        return input
            .map(function(x) { return { 'key': keyFn(x), 'value': x }; })
            .apply(new GroupByKey());
    }
}

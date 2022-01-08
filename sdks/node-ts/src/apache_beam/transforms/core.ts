import {PTransform, PCollection, Impulse, Root} from '../base';
import * as translations from '../internal/translations';
import * as runnerApi from '../proto/beam_runner_api';
import {BytesCoder, KVCoder} from '../coders/standard_coders';

import {GroupByKey} from '../base';
import {GeneralObjectCoder} from '../coders/js_coders';
import {KV} from '../values';

export class Create<T> extends PTransform<Root, PCollection<T>> {
  elements: T[];

  constructor(elements: T[]) {
    super('Create');
    this.elements = elements;
  }

  expand(root: Root) {
    const this_ = this;
    // TODO: Store encoded values and conditionally shuffle.
    return root.apply(new Impulse()).flatMap(function* (_) {
      yield* this_.elements;
    });
  }
}

export class GroupBy extends PTransform<
  PCollection<any>,
  PCollection<KV<any, Iterable<any>>>
> {
  keyFn: (element: any) => any;
  constructor(key: string | ((element: any) => any)) {
    super();
    if ((key as (element: any) => any).call !== undefined) {
      this.keyFn = key as (element: any) => any;
    } else {
      this.keyFn = function (x) {
        return x[key as string];
      };
    }
  }

  expand(input: PCollection<any>): PCollection<KV<any, Iterable<any>>> {
    const keyFn = this.keyFn;
    return input
      .map(x => {
        return {key: keyFn(x), value: x};
      })
      .apply(new GroupByKey());
  }
}

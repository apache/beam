import * as runnerApi from "../proto/beam_runner_api";
import * as urns from "../internal/urns";

import { PTransform } from "./transform";
import { PCollection, Root } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { Coder } from "../coders/coders";
import { BytesCoder, KVCoder, IterableCoder } from "../coders/required_coders";
import { ParDo } from "./pardo";
import { GeneralObjectCoder } from "../coders/js_coders";
import { KV } from "../values";
import { CombineFn } from "./group_and_combine";

export class Impulse extends PTransform<Root, PCollection<Uint8Array>> {
  // static urn: string = runnerApi.StandardPTransforms_Primitives.IMPULSE.urn;
  // TODO: use above line, not below line.
  static urn: string = "beam:transform:impulse:v1";

  constructor() {
    super("Impulse"); // TODO: pass null/nothing and get from reflection
  }

  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: Root
  ): PCollection<Uint8Array> {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: Impulse.urn,
      payload: urns.IMPULSE_BUFFER,
    });
    return pipeline.createPCollectionInternal(new BytesCoder());
  }
}

export class WithCoderInternal<T> extends PTransform<
  PCollection<T>,
  PCollection<T>
> {
  constructor(private coder: Coder<T>) {
    super("WithCoderInternal(" + coder + ")");
  }
  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: PCollection<T>
  ) {
    // IDENTITY rather than Flatten for better fusion.
    transformProto.spec = {
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.IDENTITY_DOFN_URN,
            payload: undefined!,
          }),
        })
      ),
    };

    // TODO: Consider deriving the key and value coder from the input coder.
    return pipeline.createPCollectionInternal<T>(this.coder);
  }
}

// Users should use GroupBy.
export class GroupByKey<K, V> extends PTransform<
  PCollection<KV<K, V>>,
  PCollection<KV<K, Iterable<V>>>
> {
  // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
  // TODO: use above line, not below line.
  static urn: string = "beam:transform:group_by_key:v1";

  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: PCollection<KV<K, V>>
  ) {
    // TODO: Use context.
    const pipelineComponents: runnerApi.Components =
      pipeline.getProto().components!;
    const inputCoderProto =
      pipelineComponents.coders[
        pipelineComponents.pcollections[input.getId()].coderId
      ];

    if (inputCoderProto.spec!.urn != KVCoder.URN) {
      return input
        .apply(
          new WithCoderInternal(
            new KVCoder(new GeneralObjectCoder(), new GeneralObjectCoder())
          )
        )
        .apply(new GroupByKey());
    }

    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: GroupByKey.urn,
      payload: undefined!,
    });

    // TODO: warn about BsonObjectCoder and (non)deterministic key ordering?
    const keyCoder = pipeline.getCoder(inputCoderProto.componentCoderIds[0]);
    const valueCoder = pipeline.getCoder(inputCoderProto.componentCoderIds[1]);
    const iterableValueCoder = new IterableCoder(valueCoder);
    const outputCoder = new KVCoder(keyCoder, iterableValueCoder);
    return pipeline.createPCollectionInternal(outputCoder);
  }
}

export class CombinePerKey<K, InputT, AccT, OutputT> extends PTransform<
  PCollection<KV<K, InputT>>,
  PCollection<KV<K, OutputT>>
> {
  constructor(private combineFn: CombineFn<InputT, AccT, OutputT>) {
    super();
  }

  // Let the runner do the combiner lifting, when possible, handling timestamps,
  // windowing, and triggering as needed.
  expand(input: PCollection<KV<any, InputT>>) {
    const combineFn = this.combineFn;
    return input.apply(new GroupByKey()).map((kv) => ({
      key: kv.key,
      value: combineFn.extractOutput(
        kv.value.reduce(
          combineFn.addInput.bind(combineFn),
          combineFn.createAccumulator()
        )
      ),
    }));
  }
}

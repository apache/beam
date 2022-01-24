import { ChannelCredentials } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import { Writer } from "protobufjs";

import {
  ExpansionRequest,
  ExpansionResponse,
} from "../proto/beam_expansion_api";
import {
  ExpansionServiceClient,
  IExpansionServiceClient,
} from "../proto/beam_expansion_api.client";
import {
  ArtifactRetrievalServiceClient,
  IArtifactRetrievalServiceClient,
} from "../proto/beam_artifact_api.client";
import * as runnerApi from "../proto/beam_runner_api";
import { ExternalConfigurationPayload } from "../proto/external_transforms";

import { Schema } from "../proto/schema";

import * as base from "../base";
import * as coders from "../coders/standard_coders";
import { RowCoder } from "../coders/row_coder";
import * as artifacts from "../runners/artifacts";


export class RawExternalTransform<
  InputT extends base.PValue<any>,
  OutputT extends base.PValue<any>
> extends base.AsyncPTransform<InputT, OutputT> {
  static namespaceCounter = 0;
  static freshNamespace() {
    return "namespace_" + RawExternalTransform.namespaceCounter++ + "_";
  }

  private payload?: Uint8Array;

  constructor(
    private urn: string,
    payload: Uint8Array | { [key: string]: any },
    private address: string,
    private inferPValueType: boolean = true
  ) {
    super("External(" + urn + ")");
    if (payload == undefined) {
      this.payload = undefined;
    } else if (payload instanceof Uint8Array) {
      this.payload = payload as Uint8Array;
    } else {
      this.payload = encodeSchemaPayload(payload);
    }
  }

  async asyncExpandInternal(
    pipeline: base.Pipeline,
    transformProto: runnerApi.PTransform,
    input: InputT
  ): Promise<OutputT> {
    const client = new ExpansionServiceClient(
      new GrpcTransport({
        host: this.address,
        channelCredentials: ChannelCredentials.createInsecure(),
      })
    );

    const pipelineComponents = pipeline.getProto().components!;
    const namespace = RawExternalTransform.freshNamespace();

    const request = ExpansionRequest.create({
      transform: runnerApi.PTransform.create({
        uniqueName: "test",
        spec: { urn: this.urn, payload: this.payload },
        inputs: transformProto.inputs,
      }),
      components: {},
      namespace: namespace,
    });

    // Some SDKs are not happy with PCollections created out of thin air.
    const fakeImpulseNamespace = RawExternalTransform.freshNamespace();
    for (const pcId of Object.values(transformProto.inputs)) {
      console.log("COPYING", pcId, pipelineComponents.pcollections![pcId]);
      request.components!.pcollections[pcId] =
        pipelineComponents.pcollections![pcId];
      request.components!.transforms[fakeImpulseNamespace + pcId] =
        runnerApi.PTransform.create({
          uniqueName: fakeImpulseNamespace + "_create_" + pcId,
          spec: { urn: base.Impulse.urn, payload: new Uint8Array() },
          outputs: { main: pcId },
        });
    }

    // Copy all the rest, as there may be opaque references.
    Object.assign(request.components!.coders, pipelineComponents.coders);
    Object.assign(
      request.components!.windowingStrategies,
      pipelineComponents.windowingStrategies
    );
    Object.assign(
      request.components!.environments,
      pipelineComponents.environments
    );

    console.log("Calling Expand function with");
    console.dir(request, { depth: null });
    ExpansionRequest.toBinary(request);

    const response = await client.expand(request).response;

    if (response.error) {
      throw new Error(response.error);
    }

    response.components = await this.resolveArtifacts(response.components!);

    return this.splice(pipeline, transformProto, response, namespace);
  }

  /**
   * The returned pipeline fragment may have dependencies (referenced in its)
   * environments) that are needed for execution. This function fetches (as
   * required) these artifacts from the expansion service (which may be
   * be transient) and stores them in files such that it may then forward
   * these artifacts to the choice of runner.
   */
  async resolveArtifacts(
    components: runnerApi.Components
  ): Promise<runnerApi.Components> {
    // Don't even bother creating a connection if there are no dependencies.
    if (
      Object.values(components.environments).every(
        (env) => env.dependencies.length == 0
      )
    ) {
      return components;
    }

    // An expansion service that returns environments with dependencies must
    // aslo vend an artifact retrieval service as that same port.
    const artifactClient = new ArtifactRetrievalServiceClient(
      new GrpcTransport({
        host: this.address,
        channelCredentials: ChannelCredentials.createInsecure(),
      })
    );

    // For each new environment, convert (if needed) all dependencies into
    // a more permanent form.
    for (const env of Object.values(components.environments)) {
      if (env.dependencies.length > 0) {
        env.dependencies = Array.from(
          await artifacts.resolveArtifacts(artifactClient, env.dependencies)
        );
      }
    }

    return components;
  }

  splice(
    pipeline: base.Pipeline,
    transformProto: runnerApi.PTransform,
    response: ExpansionResponse,
    namespace: string
  ): OutputT {
    function copyNamespaceComponents<T>(
      src: { [key: string]: T },
      dest: { [key: string]: T }
    ) {
      for (const [id, proto] of Object.entries(src)) {
        if (id.startsWith(namespace)) {
          dest[id] = proto;
        }
      }
    }

    function difference<T>(a: Set<T>, b: Set<T>): T[] {
      return [...a].filter((x) => !b.has(x));
    }

    // Some SDKs enforce input naming conventions.
    const newTags = difference(
      new Set(Object.keys(response.transform!.inputs)),
      new Set(Object.keys(transformProto.inputs))
    );
    if (newTags.length > 1) {
      throw new Error("Ambiguous renaming of tags.");
    } else if (newTags.length == 1) {
      const missingTags = difference(
        new Set(Object.keys(transformProto.inputs)),
        new Set(Object.keys(response.transform!.inputs))
      );
      transformProto.inputs[newTags[0]] = transformProto.inputs[missingTags[0]];
      delete transformProto.inputs[missingTags[0]];
    }

    // PCollection ids may have changed as well.
    const renamedInputs = Object.fromEntries(
      Object.keys(response.transform!.inputs).map((k) => [
        response.transform!.inputs[k],
        transformProto.inputs[k],
      ])
    );
    response.transform!.inputs = Object.fromEntries(
      Object.entries(response.transform!.inputs).map(([k, v]) => [
        k,
        renamedInputs[v],
      ])
    );
    for (const t of Object.values(response.components!.transforms)) {
      t.inputs = Object.fromEntries(
        Object.entries(t.inputs).map(([k, v]) => [
          k,
          renamedInputs[v] != undefined ? renamedInputs[v] : v,
        ])
      );
    }

    // Copy the proto contents.
    Object.assign(transformProto, response.transform);

    // Now copy everything over.
    const proto = pipeline.getProto();
    const pipelineComponents = proto.components;
    pipeline.getProto().requirements.push(...response.requirements);
    copyNamespaceComponents(
      response.components!.transforms,
      pipelineComponents!.transforms
    );
    copyNamespaceComponents(
      response.components!.pcollections,
      pipelineComponents!.pcollections
    );
    copyNamespaceComponents(
      response.components!.coders,
      pipelineComponents!.coders
    );
    copyNamespaceComponents(
      response.components!.environments,
      pipelineComponents!.environments
    );
    copyNamespaceComponents(
      response.components!.windowingStrategies,
      pipelineComponents!.windowingStrategies
    );

    // Ensure we understand the resulting coders.
    // TODO: We could still patch things together if we don't understand the coders,
    // but the errors are harder to follow.  Consider only rejecting coders that
    // actually cross the boundary.
    for (const pcId of Object.values(response.transform!.outputs)) {
      const pcProto = pipelineComponents!.pcollections[pcId];
      pipeline.context.getCoder(pcProto.coderId);
    }

    // Construct and return the resulting object.
    // TODO: Can I get the concrete OutputT?
    if (this.inferPValueType) {
      const outputKeys = [...Object.keys(response.transform!.outputs)];
      if (outputKeys.length == 0) {
        return null!;
      } else if (outputKeys.length == 1) {
        return new base.PCollection(
          pipeline,
          response.transform!.outputs[outputKeys[0]]
        ) as OutputT;
      }
    }
    return Object.fromEntries(
      Object.entries(response.transform!.outputs).map(([k, v]) => [
        k,
        new base.PCollection(pipeline, v),
      ])
    ) as OutputT;
  }
}

function encodeSchemaPayload(
  payload: any,
  schema: Schema | undefined = undefined
): Uint8Array {
  const encoded = new Writer();
  if (!schema) {
    schema = RowCoder.InferSchemaOfJSON(payload);
  }
  new RowCoder(schema!).encode(payload, encoded, null!);
  return ExternalConfigurationPayload.toBinary({
    schema: schema,
    payload: encoded.finish(),
  });
}

// import * as beam from "../../apache_beam";
// import { PortableRunner } from "../runners/portable_runner/runner";
// import { RemoteJobServiceClient } from "../runners/portable_runner/client";

// async function main() {
// //     const kvCoder = new coders.KVCoder(new coders.VarIntCoder(), new coders.VarIntCoder());
// //     const root = new base.Root(new base.Pipeline());
// //     const input = root.apply(new core.Create([{ key: 1, value: 3 }])).apply(new base.WithCoderInternal(kvCoder));
// //     //     const input2 = root.apply(new core.Create([{key: 1, value: 4}])).apply(new base.WithCoderInternal(kvCoder));
// //     // await input.asyncApply(new RawExternalTransform<base.PValue<any>, base.PValue<any>>(base.GroupByKey.urn, undefined!, 'localhost:4444'));
// //     await input.asyncApply(new RawExternalTransform<base.PValue<any>, base.PValue<any>>(
// //         'beam:transforms:python:fully_qualified_named',
// //         {
// //             constructor: 'apache_beam.transforms.GroupByKey',
// //         },
// //         'localhost:4444'));
// //     console.log('-------------------------------------------');
// //     console.dir(input.pipeline.getProto(), { depth: null });
// //
//
//     const kvCoder = new coders.KVCoder(new coders.StrUtf8Coder(), new coders.StrUtf8Coder());
//     await new PortableRunner(new RemoteJobServiceClient('localhost:3333')).run(
// //         await new DirectRunner().run(
//             async (root) => {
// //                 const lines = root.apply(new beam.Create([
// //                     "In the beginning God created the heaven and the earth.",
// //                     "And the earth was without form, and void; and darkness was upon the face of the deep.",
// //                     "And the Spirit of God moved upon the face of the waters.",
// //                     "And God said, Let there be light: and there was light.",
// //                 ]));
//
// //                     const result = root.apply(new beam.Create([1, 2, 3]))
//
//                  const result = await root.asyncApply(new RawExternalTransform<base.PValue<any>, base.PCollection<any>>(
//                     'beam:transforms:python:fully_qualified_named',
//                     {
//                         constructor: 'apache_beam.MyTest',
// //                         args: {'a0': [1, 2, 3]}
// //                         constructor: 'apache_beam.io.ReadFromText',
// //                         args: {'a0': '/Users/robertwb/Work/beam/incubator-beam/sdks/node-ts/tsconfig.json'}
//                     },
//                     'localhost:4444'));
//
//
//                 //lines.apply(wordCount)
//                 result.map(console.log)
//
//                 console.dir(root.pipeline.getProto(), { depth: null });
//                 runnerApi.Pipeline.toBinary(root.pipeline.getProto());
//
//             })
//
// }
//
// main().catch(e => console.error(e)).finally(() => process.exit());

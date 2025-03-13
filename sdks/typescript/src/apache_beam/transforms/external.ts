/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import { PValue, PCollection } from "../pvalue";
import { Pipeline } from "../internal/pipeline";

import * as coders from "../coders/standard_coders";
import * as internal from "./internal";
import * as transform from "./transform";
import { RowCoder } from "../coders/row_coder";
import { Coder } from "../coders/coders";
import * as artifacts from "../runners/artifacts";
import * as service from "../utils/service";

/**
 * Various transforms useful for creating and invoking cross-language
 * transforms.
 *
 * See also https://beam.apache.org/documentation/programming-guide/#1324-using-cross-language-transforms-in-a-typescript-pipeline
 *
 * @packageDocumentation
 */

export interface RawExternalTransformOptions {
  inferPValueType?: boolean;
  requestedOutputCoders?: { [key: string]: Coder<unknown> };
}

const defaultRawExternalTransformOptions: RawExternalTransformOptions = {
  inferPValueType: true,
  requestedOutputCoders: {},
};

// TODO: (API) (Types) This class expects PCollections to already have the
// correct Coders. It would be great if we could infer coders, or at least have
// a cleaner way to specify them than using internal.WithCoderInternal.
export function rawExternalTransform<
  InputT extends PValue<any>,
  OutputT extends PValue<any>,
>(
  urn: string,
  payload: Uint8Array | { [key: string]: any },
  serviceProviderOrAddress: string | (() => Promise<service.Service>),
  options: RawExternalTransformOptions = {},
): transform.AsyncPTransform<InputT, OutputT> {
  return new RawExternalTransform(
    urn,
    payload,
    serviceProviderOrAddress,
    options,
  );
}

class RawExternalTransform<
  InputT extends PValue<any>,
  OutputT extends PValue<any>,
> extends transform.AsyncPTransformClass<InputT, OutputT> {
  static namespaceCounter = 0;
  static freshNamespace() {
    return "namespace_" + RawExternalTransform.namespaceCounter++ + "_";
  }

  private payload?: Uint8Array;
  private serviceProvider: () => Promise<service.Service>;
  private options: RawExternalTransformOptions;

  constructor(
    private urn: string,
    payload: Uint8Array | { [key: string]: any },
    serviceProviderOrAddress: string | (() => Promise<service.Service>),
    options: RawExternalTransformOptions,
  ) {
    super("External(" + urn + ")");
    this.options = { ...defaultRawExternalTransformOptions, ...options };
    if (payload === null || payload === undefined) {
      this.payload = undefined;
    } else if (payload instanceof Uint8Array) {
      this.payload = payload as Uint8Array;
    } else {
      this.payload = encodeSchemaPayload(payload);
    }

    if (typeof serviceProviderOrAddress === "string") {
      this.serviceProvider = async () =>
        new service.ExternalService(serviceProviderOrAddress);
    } else {
      this.serviceProvider = serviceProviderOrAddress;
    }
  }

  async expandInternalAsync(
    input: InputT,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
  ): Promise<OutputT> {
    const pipelineComponents = pipeline.getProto().components!;
    const namespace = RawExternalTransform.freshNamespace();

    const request = ExpansionRequest.create({
      transform: runnerApi.PTransform.create({
        uniqueName: transformProto.uniqueName,
        spec: { urn: this.urn, payload: this.payload },
        inputs: transformProto.inputs,
      }),
      components: {},
      namespace: namespace,
    });

    // Some SDKs are not happy with PCollections created out of thin air.
    const fakeImpulseNamespace = RawExternalTransform.freshNamespace();
    for (const pcId of Object.values(transformProto.inputs)) {
      request.components!.pcollections[pcId] =
        pipelineComponents.pcollections![pcId];
      request.components!.transforms[fakeImpulseNamespace + pcId] =
        runnerApi.PTransform.create({
          uniqueName: fakeImpulseNamespace + "_create_" + pcId,
          spec: { urn: internal.impulse.urn, payload: new Uint8Array() },
          outputs: { main: pcId },
        });
    }

    for (const [output, coder] of Object.entries(
      this.options.requestedOutputCoders!,
    )) {
      request.outputCoderRequests[output] = pipeline.getCoderId(coder);
    }

    // Copy all the rest, as there may be opaque references.
    Object.assign(request.components!.coders, pipelineComponents.coders);
    Object.assign(
      request.components!.windowingStrategies,
      pipelineComponents.windowingStrategies,
    );
    Object.assign(
      request.components!.environments,
      pipelineComponents.environments,
    );

    const service = await this.serviceProvider();
    const address = await service.start();

    const client = new ExpansionServiceClient(
      new GrpcTransport({
        host: address,
        channelCredentials: ChannelCredentials.createInsecure(),
      }),
    );

    try {
      const response = await client.expand(request).response;

      if (response.error) {
        throw new Error(response.error);
      }

      response.components = await this.resolveArtifacts(
        response.components!,
        address,
      );

      return this.splice(pipeline, transformProto, response, namespace);
    } finally {
      await service.stop();
    }
  }

  /**
   * The returned pipeline fragment may have dependencies (referenced in its)
   * environments) that are needed for execution. This function fetches (as
   * required) these artifacts from the expansion service (which may be
   * be transient) and stores them in files such that it may then forward
   * these artifacts to the choice of runner.
   */
  async resolveArtifacts(
    components: runnerApi.Components,
    address: string,
  ): Promise<runnerApi.Components> {
    // Don't even bother creating a connection if there are no dependencies.
    if (
      Object.values(components.environments).every(
        (env) => env.dependencies.length === 0,
      )
    ) {
      return components;
    }

    // An expansion service that returns environments with dependencies must
    // aslo vend an artifact retrieval service as that same port.
    const artifactClient = new ArtifactRetrievalServiceClient(
      new GrpcTransport({
        host: address,
        channelCredentials: ChannelCredentials.createInsecure(),
      }),
    );

    // For each new environment, convert (if needed) all dependencies into
    // a more permanent form.
    for (const env of Object.values(components.environments)) {
      if (env.dependencies.length > 0) {
        env.dependencies = Array.from(
          await (async () => {
            let result: runnerApi.ArtifactInformation[] = [];
            for await (const dep of artifacts.resolveArtifacts(
              artifactClient,
              env.dependencies,
            )) {
              result.push(dep);
            }
            return result;
          })(),
        );
      }
    }

    return components;
  }

  splice(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    response: ExpansionResponse,
    namespace: string,
  ): OutputT {
    function copyNamespaceComponents<T>(
      src: { [key: string]: T },
      dest: { [key: string]: T },
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
      new Set(Object.keys(transformProto.inputs)),
    );
    if (newTags.length > 1) {
      throw new Error("Ambiguous renaming of tags.");
    } else if (newTags.length === 1) {
      const missingTags = difference(
        new Set(Object.keys(transformProto.inputs)),
        new Set(Object.keys(response.transform!.inputs)),
      );
      transformProto.inputs[newTags[0]] = transformProto.inputs[missingTags[0]];
      delete transformProto.inputs[missingTags[0]];
    }

    // PCollection ids may have changed as well.
    const renamedInputs = Object.fromEntries(
      Object.keys(response.transform!.inputs).map((k) => [
        response.transform!.inputs[k],
        transformProto.inputs[k],
      ]),
    );
    response.transform!.inputs = Object.fromEntries(
      Object.entries(response.transform!.inputs).map(([k, v]) => [
        k,
        renamedInputs[v],
      ]),
    );
    for (const t of Object.values(response.components!.transforms)) {
      t.inputs = Object.fromEntries(
        Object.entries(t.inputs).map(([k, v]) => [
          k,
          renamedInputs[v] !== null && renamedInputs[v] !== undefined
            ? renamedInputs[v]
            : v,
        ]),
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
      pipelineComponents!.transforms,
    );
    copyNamespaceComponents(
      response.components!.pcollections,
      pipelineComponents!.pcollections,
    );
    copyNamespaceComponents(
      response.components!.coders,
      pipelineComponents!.coders,
    );
    copyNamespaceComponents(
      response.components!.environments,
      pipelineComponents!.environments,
    );
    copyNamespaceComponents(
      response.components!.windowingStrategies,
      pipelineComponents!.windowingStrategies,
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
    // TODO: (Typescipt) Can I get the concrete OutputT at runtime?
    // TypeScript types are not available at runtime. If I understand correctly, there is no plan to change that at the moment.
    // See: https://github.com/microsoft/TypeScript/issues/47658
    // See: https://github.com/microsoft/TypeScript/issues/3628
    if (this.options.inferPValueType) {
      const outputKeys = [...Object.keys(response.transform!.outputs)];
      if (outputKeys.length === 0) {
        return null!;
      } else if (outputKeys.length === 1) {
        return new PCollection(
          pipeline,
          response.transform!.outputs[outputKeys[0]],
        ) as OutputT;
      }
    }
    return Object.fromEntries(
      Object.entries(response.transform!.outputs).map(([k, v]) => [
        k,
        new PCollection(pipeline, v),
      ]),
    ) as OutputT;
  }
}

function encodeSchemaPayload(
  payload: any,
  schema: Schema | undefined = undefined,
): Uint8Array {
  const encoded = new Writer();
  if (!schema) {
    schema = RowCoder.inferSchemaOfJSON(payload);
  }

  new RowCoder(schema!).encode(payload, encoded, null!);
  return ExternalConfigurationPayload.toBinary({
    schema: schema,
    payload: encoded.finish(),
  });
}

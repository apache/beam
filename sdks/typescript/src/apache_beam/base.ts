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

import equal from "fast-deep-equal";

import * as runnerApi from "./proto/beam_runner_api";
import * as fnApi from "./proto/beam_fn_api";
import { Coder, globalRegistry as globalCoderRegistry } from "./coders/coders";
import { GlobalWindowCoder } from "./coders/standard_coders";
import { BytesCoder, IterableCoder, KVCoder } from "./coders/standard_coders";
import { GeneralObjectCoder } from "./coders/js_coders";

import { PipelineOptions } from "./options/pipeline_options";
import { KV, Window } from "./values";
import { WindowedValue } from ".";

import { Root, PCollection, PValue, flattenPValue } from "./pvalue";
import { AsyncPTransform, PTransform } from "./transforms/transform";
import { DoFn, ParDo } from "./transforms/pardo";
import { Pipeline } from "./internal/pipeline";

// TODO: Cleanup. fix imports and remove
export * from "./pvalue";
export * from "./transforms/transform";
export * from "./transforms/create";
export * from "./transforms/group_and_combine";
export * from "./transforms/flatten";
export * from "./transforms/pardo";
export * from "./transforms/window";
export * from "./transforms/internal";
export * from "./internal/pipeline";

type Components = runnerApi.Components | fnApi.ProcessBundleDescriptor;

// TODO: Cleanup. Where to put this.
export class PipelineContext {
  components: Components;
  counter: number = 0;

  private coders: { [key: string]: Coder<any> } = {};

  constructor(components: Components) {
    this.components = components;
  }

  getCoder<T>(coderId: string): Coder<T> {
    const this_ = this;
    if (this.coders[coderId] == undefined) {
      const coderProto = this.components.coders[coderId];
      const coderConstructor = globalCoderRegistry().get(coderProto.spec!.urn);
      const components = (coderProto.componentCoderIds || []).map(
        this_.getCoder.bind(this_)
      );
      if (coderProto.spec!.payload && coderProto.spec!.payload.length) {
        this.coders[coderId] = new coderConstructor(
          coderProto.spec!.payload,
          ...components
        );
      } else {
        this.coders[coderId] = new coderConstructor(...components);
      }
    }
    return this.coders[coderId];
  }

  getCoderId(coder: Coder<any>): string {
    return this.getOrAssign(
      this.components.coders,
      coder.toProto(this),
      "coder"
    );
  }

  getPCollectionCoder<T>(pcoll: PCollection<T>): Coder<T> {
    return this.getCoder(this.getPCollectionCoderId(pcoll));
  }

  getPCollectionCoderId<T>(pcoll: PCollection<T>): string {
    const pcollId = typeof pcoll == "string" ? pcoll : pcoll.getId();
    return this.components!.pcollections[pcollId].coderId;
  }

  getWindowingStrategy(id: string): runnerApi.WindowingStrategy {
    return this.components.windowingStrategies[id];
  }

  getWindowingStrategyId(windowing: runnerApi.WindowingStrategy): string {
    return this.getOrAssign(
      this.components.windowingStrategies,
      windowing,
      "windowing"
    );
  }

  private getOrAssign<T>(
    existing: { [key: string]: T },
    obj: T,
    prefix: string
  ) {
    for (const [id, other] of Object.entries(existing)) {
      if (equal(other, obj)) {
        return id;
      }
    }
    const newId = this.createUniqueName(prefix);
    existing[newId] = obj;
    return newId;
  }

  createUniqueName(prefix: string): string {
    return prefix + "_" + this.counter++;
  }
}

let fakeSerializeCounter = 0;
const fakeSerializeMap = new Map<string, any>();
export function fakeSeralize(obj) {
  fakeSerializeCounter += 1;
  const id = "s_" + fakeSerializeCounter;
  fakeSerializeMap.set(id, obj);
  return new TextEncoder().encode(id);
}
export function fakeDeserialize(s) {
  return fakeSerializeMap.get(new TextDecoder().decode(s));
}

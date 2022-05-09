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

import * as runnerApi from "../proto/beam_runner_api";
import { PTransform } from "./transform";
import { PCollection } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { GeneralObjectCoder } from "../coders/js_coders";

export class Flatten<T> extends PTransform<PCollection<T>[], PCollection<T>> {
  // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
  // TODO: (Cleanup) use above line, not below line.
  static urn: string = "beam:transform:flatten:v1";
  name = "Flatten";

  expandInternal(
    inputs: PCollection<T>[],
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: Flatten.urn,
      payload: null!,
    });

    // TODO: Input coder if they're all the same? UnionCoder?
    return pipeline.createPCollectionInternal<T>(new GeneralObjectCoder());
  }
}

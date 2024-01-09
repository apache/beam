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
import { PTransform, withName } from "./transform";
import { PCollection } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { GeneralObjectCoder } from "../coders/js_coders";

/**
 * Returns a PTransform that flattens, or takes the union, of multiple
 * PCollections.
 *
 * See also https://beam.apache.org/documentation/programming-guide/#flatten
 */
export function flatten<T>(): PTransform<PCollection<T>[], PCollection<T>> {
  function expandInternal(
    inputs: PCollection<T>[],
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: flatten.urn,
    });

    // TODO: UnionCoder if they're not the same?
    const coders = new Set(
      inputs.map((pc) => pipeline.context.getPCollectionCoderId(pc)),
    );
    const coder =
      coders.size === 1 ? [...coders][0] : new GeneralObjectCoder<T>();
    return pipeline.createPCollectionInternal<T>(coder);
  }

  return withName("flatten", expandInternal);
}

/** @internal */
flatten.urn = "beam:transform:flatten:v1";

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

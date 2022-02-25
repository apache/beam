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
import { PValue } from "../pvalue";
import { Pipeline } from "../base";

export class AsyncPTransform<
  InputT extends PValue<any>,
  OutputT extends PValue<any>
> {
  name: string;

  constructor(name: string | null = null) {
    this.name = name || typeof this;
  }

  async asyncExpand(input: InputT): Promise<OutputT> {
    throw new Error("Method expand has not been implemented.");
  }

  async asyncExpandInternal(
    input: InputT,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
  ): Promise<OutputT> {
    return this.asyncExpand(input);
  }
}

export class PTransform<
  InputT extends PValue<any>,
  OutputT extends PValue<any>
> extends AsyncPTransform<InputT, OutputT> {
  expand(input: InputT): OutputT {
    throw new Error("Method expand has not been implemented.");
  }

  async asyncExpand(input: InputT): Promise<OutputT> {
    return this.expand(input);
  }

  expandInternal(
    input: InputT,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
  ): OutputT {
    return this.expand(input);
  }

  async asyncExpandInternal(
    input: InputT,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
  ): Promise<OutputT> {
    return this.expandInternal(input, pipeline, transformProto);
  }
}

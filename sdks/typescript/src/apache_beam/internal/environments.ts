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

export const TYPESCRIPT_DEFAULT_ENVIRONMENT_URN = "js_default";

function javascriptCapabilities(): string[] {
  // TODO: Cleanup. Actually populate.
  return [
    // This is needed for sessions to work...
    "beam:coder:interval_window:v1",
    "beam:protocol:sibling_workers:v1",
  ];
}

export function defaultJsEnvironment() {
  return runnerApi.Environment.create({
    urn: TYPESCRIPT_DEFAULT_ENVIRONMENT_URN,
    capabilities: javascriptCapabilities(),
  });
}

export function jsEnvironment(
  urn: string,
  payload: Uint8Array,
  resourceHints: { [key: string]: Uint8Array } = {},
  artifacts: runnerApi.ArtifactInformation[] = [],
): runnerApi.Environment {
  return {
    urn: urn,
    payload: payload,
    resourceHints: resourceHints,
    dependencies: artifacts,
    capabilities: javascriptCapabilities(),
    displayData: null!,
  };
}

function asNewEnvironment(
  env: runnerApi.Environment,
  urn: string,
  payload: Uint8Array,
) {
  return {
    urn: urn,
    payload: payload,
    resourceHints: env.resourceHints,
    dependencies: env.dependencies,
    capabilities: env.capabilities,
    displayData: env.displayData,
  };
}

export function asExternalEnvironment(
  env: runnerApi.Environment,
  address: string,
) {
  return asNewEnvironment(
    env,
    "beam:env:external:v1",
    runnerApi.ExternalPayload.toBinary({
      endpoint: {
        url: address,
        authentication: null!,
      },
      params: {},
    }),
  );
}

export function asDockerEnvironment(
  env: runnerApi.Environment,
  containerImage: string,
) {
  return asNewEnvironment(
    env,
    "beam:env:docker:v1",
    runnerApi.DockerPayload.toBinary({ containerImage: containerImage }),
  );
}

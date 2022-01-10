import * as runnerApi from "../proto/beam_runner_api";

export const PYTHON_DEFAULT_ENVIRONMENT_URN = "js_default";

function javascriptCapabilities(): string[] {
  return []; // TODO: Actually populate.
}

export function defaultJsEnvironment() {
  return runnerApi.Environment.create({
    urn: PYTHON_DEFAULT_ENVIRONMENT_URN,
    capabilities: javascriptCapabilities(),
  });
}

export function jsEnvironment(
  urn: string,
  payload: Uint8Array,
  resourceHints: { [key: string]: Uint8Array } = {},
  artifacts: runnerApi.ArtifactInformation[] = []
): runnerApi.Environment {
  return {
    urn: urn,
    payload: payload,
    resourceHints: resourceHints,
    dependencies: artifacts,
    capabilities: [], // TODO: Fill in.
    displayData: null!,
  };
}

function asNewEnvironment(
  env: runnerApi.Environment,
  urn: string,
  payload: Uint8Array
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
  address: string
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
    })
  );
}

export function asDockerEnvironment(
  env: runnerApi.Environment,
  containerImage: string
) {
  return asNewEnvironment(
    env,
    "beam:env:docker:v1",
    runnerApi.DockerPayload.toBinary({ containerImage: containerImage })
  );
}

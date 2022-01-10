import * as runnerApi from "../proto/beam_runner_api";
import equal from "fast-deep-equal";

export const IMPULSE_BUFFER = new TextEncoder().encode("impulse");

export const DATA_INPUT_URN = "beam:runner:source:v1";
export const DATA_OUTPUT_URN = "beam:runner:sink:v1";
export const IDENTITY_DOFN_URN = "beam:dofn:identity:0.1";

export const SERIALIZED_JS_DOFN_INFO = "beam:dofn:serialized_js_dofn_info:v1";
export const SPLITTING_JS_DOFN_URN = "beam:dofn:splitting_dofn:v1";
export const JS_WINDOW_INTO_DOFN_URN = "beam:dofn:js_window_into:v1";
export const JS_ASSIGN_TIMESTAMPS_DOFN_URN =
  "beam:dofn:js_assign_timestamps:v1";

let _coder_counter = 0;
const _CODER_ID_PREFIX = "coder_";

export function registerPipelineCoder(
  coderProto: runnerApi.Coder,
  pipelineComponents: runnerApi.Components
) {
  for (const coderId of Object.keys(pipelineComponents.coders)) {
    const existingCoder = pipelineComponents.coders[coderId];
    if (equal(existingCoder, coderProto) && coderId != undefined) {
      return coderId;
    }
  }
  const newCoderId = _CODER_ID_PREFIX + _coder_counter;
  _coder_counter += 1;
  pipelineComponents.coders[newCoderId] = coderProto;
  return newCoderId;
}

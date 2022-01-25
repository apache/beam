export const IMPULSE_BUFFER = new TextEncoder().encode("impulse");

export const DATA_INPUT_URN = "beam:runner:source:v1";
export const DATA_OUTPUT_URN = "beam:runner:sink:v1";
export const IDENTITY_DOFN_URN = "beam:dofn:identity:0.1";

export const SERIALIZED_JS_DOFN_INFO = "beam:dofn:serialized_js_dofn_info:v1";
export const SPLITTING_JS_DOFN_URN = "beam:dofn:splitting_dofn:v1";
export const JS_WINDOW_INTO_DOFN_URN = "beam:dofn:js_window_into:v1";
export const JS_ASSIGN_TIMESTAMPS_DOFN_URN =
  "beam:dofn:js_assign_timestamps:v1";

// Everything maps to the global window.
export const GLOBAL_WINDOW_MAPPING_FN_URN = "beam:window_mapping_fn:global:v1";

// The main and side inputs agree, and we map windows to themselves.
export const IDENTITY_WINDOW_MAPPING_FN_URN =
  "beam:window_mapping_fn:identity:v1";

// Construct a mapping pcA.windows -> pcB.windows vai
// (a) => pcB.windowFn.assign(a.window.maxTimestamp())
export const ASSIGN_MAX_TIMESTAMP_WINDOW_MAPPING_FN_URN =
  "beam:window_mapping_fn:assign_max_timestamp:v1";

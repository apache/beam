/*
components {
    transforms {
        value {
          key: "impulse_input"
        spec {
          urn: "beam:transform:impulse:v1"
        }
        outputs {
          key: "out"
          value: "ref_PCollection_PCollection_1"
        }
        unique_name: "impulse_input"
      }
    }
    pcollections {
      key: "ref_PCollection_PCollection_1"
      value {
        unique_name: "18Create/Map(decode).None"
        coder_id: "ref_Coder_StrUtf8Coder_1"
        is_bounded: BOUNDED
        windowing_strategy_id: "ref_Windowing_Windowing_1"
      }
    }
    windowing_strategies {
      key: "ref_Windowing_Windowing_1"
      value {
        window_fn {
          urn: "beam:window_fn:global_windows:v1"
        }
        merge_status: NON_MERGING
        window_coder_id: "ref_Coder_GlobalWindowCoder_2"
        trigger {
          default {
          }
        }
        accumulation_mode: DISCARDING
        output_time: END_OF_WINDOW
        closing_behavior: EMIT_ALWAYS
        on_time_behavior: FIRE_ALWAYS
        environment_id: "ref_Environment_default_environment_1"
      }
    }
    coders {
      key: "ref_Coder_GlobalWindowCoder_2"
      value {
        spec {
          urn: "beam:coder:global_window:v1"
        }
      }
    }
    coders {
      key: "ref_Coder_StrUtf8Coder_1"
      value {
        spec {
          urn: "beam:coder:string_utf8:v1"
        }
      }
    }
    environments {
      key: "ref_Environment_default_environment_1"
      value {
        urn: "beam:env:default:v1"
      }
    }
  }
  transform {
    spec {
      urn: "beam:transforms:python:fully_qualified_named"
      payload: "\n\275\\001\n\\021\n\\013constructor\\032\\002\\020\\007\n>\n\\004args\\032624\n2\n\n\n\\004arg0\\032\\002\\020\\007\\022$9e976441-eda8-4c73-91c2-dad1c4b4cdc6\nB\n\\006kwargs\\032826\n4\n\\014\n\\006suffix\\032\\002\\020\\007\\022$1ea53dc2-a9c2-44f3-97ca-ecd49c0a55a0\\022$a1dea4ab-af5d-43f0-a177-e87f5be8f216\\022U\\003\\000Japache_beam.transforms.fully_qualified_named_transform_test._TestTransform\\001\\000\\001x\\001\\000\\001y"
    }
    inputs {
      key: "input"
      value: "ref_PCollection_PCollection_1"
    }
    unique_name: "ExternalTransform(beam:transforms:python:fully_qualified_named)"
  }
  namespace: "external_1"
*/


import { ExpansionRequest } from '../../proto/beam_expansion_api';
import { PTransform, Components, IsBounded_Enum, MergeStatus_Enum, Trigger_Default, Trigger, AccumulationMode, AccumulationMode_Enum, OutputTime_Enum, ClosingBehavior_Enum, OnTimeBehavior_Enum } from "../../proto/beam_runner_api";

let components: Components = {
    transforms: {
        "impulse_input": {
            uniqueName: "impulse_input",
            spec: { urn: "beam:transform:impulse:v1", payload: new Uint8Array() },
            outputs: { "out": "ref_PCollection_PCollection_1" },
            subtransforms: [],
            inputs: {},
            displayData: [],
            environmentId: "",
            annotations: {}
        }
    },
    pcollections: {
        "ref_PCollection_PCollection_1": {
            uniqueName: "18Create/Map(decode).None",
            coderId: "ref_Coder_StrUtf8Coder_1",
            isBounded: IsBounded_Enum.BOUNDED,
            windowingStrategyId: "ref_Windowing_Windowing_1",
            displayData: [],
        }
    },
    windowingStrategies: {
        "ref_Windowing_Windowing_1": {
            windowFn: { urn: "beam:window_fn:global_windows:v1", payload: new Uint8Array() },
            mergeStatus: MergeStatus_Enum.NON_MERGING,
            windowCoderId: "ref_Coder_GlobalWindowCoder_2",
            trigger: { trigger: { oneofKind: "default", default: Trigger_Default } },
            accumulationMode: AccumulationMode_Enum.DISCARDING,
            outputTime: OutputTime_Enum.END_OF_WINDOW,
            closingBehavior: ClosingBehavior_Enum.EMIT_ALWAYS,
            onTimeBehavior: OnTimeBehavior_Enum.FIRE_ALWAYS,
            environmentId: "ref_Environment_default_environment_1",
            allowedLateness: 0n,
            assignsToOneWindow: false,

        }
    },
    coders: {
        "ref_Coder_GlobalWindowCoder_2": {
            spec: { urn: "beam:coder:global_window:v1", payload: new Uint8Array() },
            componentCoderIds: [],
        },
        "ref_Coder_StrUtf8Coder_1": {
            spec: { urn: "beam:coder:string_utf8:v1", payload: new Uint8Array() },
            componentCoderIds: [],
        }
    },
    environments: {
        "ref_Environment_default_environment_1": {
            urn: "beam:env:default:v1",
            payload: new Uint8Array(),
            displayData: [],
            capabilities: [],
            dependencies: [],
            resourceHints: {}
        }
    }
};


// let tPayload = "\n\\275\\001\n\\021\n\\013constructor\\032\\002\\020\\007\n>\n\\004args\\032624\n2\n\n\n\\004arg0\\032\\002\\020\\007\\022$9e976441-eda8-4c73-91c2-dad1c4b4cdc6\nB\n\\006kwargs\\032826\n4\n\\014\n\\006suffix\\032\\002\\020\\007\\022$1ea53dc2-a9c2-44f3-97ca-ecd49c0a55a0\\022$a1dea4ab-af5d-43f0-a177-e87f5be8f216\\022U\\003\\000Japache_beam.transforms.fully_qualified_named_transform_test._TestTransform\\001\\000\\001x\\001\\000\\001y";


let tPayload = [10, 92, 50, 55, 53, 92, 48, 48, 49, 10, 92, 48, 50, 49, 10, 92, 48, 49, 51, 99, 111, 110, 115, 116, 114, 117, 99, 116, 111, 114, 92, 48, 51, 50, 92, 48, 48, 50, 92, 48, 50, 48, 92, 48, 48, 55, 10, 62, 10, 92, 48, 48, 52, 97, 114, 103, 115, 92, 48, 51, 50, 54, 50, 52, 10, 50, 10, 10, 10, 92, 48, 48, 52, 97, 114, 103, 48, 92, 48, 51, 50, 92, 48, 48, 50, 92, 48, 50, 48, 92, 48, 48, 55, 92, 48, 50, 50, 36, 57, 101, 57, 55, 54, 52, 52, 49, 45, 101, 100, 97, 56, 45, 52, 99, 55, 51, 45, 57, 49, 99, 50, 45, 100, 97, 100, 49, 99, 52, 98, 52, 99, 100, 99, 54, 10, 66, 10, 92, 48, 48, 54, 107, 119, 97, 114, 103, 115, 92, 48, 51, 50, 56, 50, 54, 10, 52, 10, 92, 48, 49, 52, 10, 92, 48, 48, 54, 115, 117, 102, 102, 105, 120, 92, 48, 51, 50, 92, 48, 48, 50, 92, 48, 50, 48, 92, 48, 48, 55, 92, 48, 50, 50, 36, 49, 101, 97, 53, 51, 100, 99, 50, 45, 97, 57, 99, 50, 45, 52, 52, 102, 51, 45, 57, 55, 99, 97, 45, 101, 99, 100, 52, 57, 99, 48, 97, 53, 53, 97, 48, 92, 48, 50, 50, 36, 97, 49, 100, 101, 97, 52, 97, 98, 45, 97, 102, 53, 100, 45, 52, 51, 102, 48, 45, 97, 49, 55, 55, 45, 101, 56, 55, 102, 53, 98, 101, 56, 102, 50, 49, 54, 92, 48, 50, 50, 85, 92, 48, 48, 51, 92, 48, 48, 48, 74, 97, 112, 97, 99, 104, 101, 95, 98, 101, 97, 109, 46, 116, 114, 97, 110, 115, 102, 111, 114, 109, 115, 46, 102, 117, 108, 108, 121, 95, 113, 117, 97, 108, 105, 102, 105, 101, 100, 95, 110, 97, 109, 101, 100, 95, 116, 114, 97, 110, 115, 102, 111, 114, 109, 95, 116, 101, 115, 116, 46, 95, 84, 101, 115, 116, 84, 114, 97, 110, 115, 102, 111, 114, 109, 92, 48, 48, 49, 92, 48, 48, 48, 92, 48, 48, 49, 120, 92, 48, 48, 49, 92, 48, 48, 48, 92, 48, 48, 49, 121];


let transform: PTransform = {
    spec: {
        // urn: "beam:transforms:python:fully_qualified_named",
        // payload: new TextEncoder().encode(tPayload)
        // payload: new Uint8Array(tPayload),
        urn: "beam:transforms:xlang:test:prefix",
        payload: new Uint8Array(),
    },
    inputs: { "input": "ref_PCollection_PCollection_1" },
    outputs: {},
    subtransforms: [],
    displayData: [],
    environmentId: "",
    annotations: {},
    uniqueName: "ExternalTransform(beam:transforms:python:fully_qualified_named)"
}

let namespace = "beam-ts";


const expansionReq: ExpansionRequest = {
    components: components,
    transform: transform,
    namespace: namespace
};

export { expansionReq };
import * as runnerApi from "../proto/beam_runner_api";
import * as urns from "../internal/urns";

import { PTransform } from "./transform";
import { Coder } from "../coders/coders";
import { BoundedWindow } from "../values";
import { PCollection } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { ParDo } from "./pardo";
import { fakeSeralize } from "../base";

export interface WindowFn<W extends BoundedWindow> {
  assignWindows: (Instant) => W[];
  windowCoder: () => Coder<W>;
  toProto: () => runnerApi.FunctionSpec;
  isMerging: () => boolean;
  assignsToOneWindow: () => boolean;
}

export class WindowInto<T, W extends BoundedWindow> extends PTransform<
  PCollection<T>,
  PCollection<T>
> {
  static createWindowingStrategy(
    pipeline: Pipeline,
    windowFn: WindowFn<any>,
    windowingStrategyBase: runnerApi.WindowingStrategy | undefined = undefined
  ): runnerApi.WindowingStrategy {
    let result: runnerApi.WindowingStrategy;
    if (windowingStrategyBase == undefined) {
      result = {
        windowFn: undefined!,
        windowCoderId: undefined!,
        mergeStatus: undefined!,
        assignsToOneWindow: undefined!,
        trigger: { trigger: { oneofKind: "default", default: {} } },
        accumulationMode: runnerApi.AccumulationMode_Enum.DISCARDING,
        outputTime: runnerApi.OutputTime_Enum.END_OF_WINDOW,
        closingBehavior: runnerApi.ClosingBehavior_Enum.EMIT_ALWAYS,
        onTimeBehavior: runnerApi.OnTimeBehavior_Enum.FIRE_ALWAYS,
        allowedLateness: BigInt(0),
        environmentId: pipeline.defaultEnvironment,
      };
    } else {
      result = runnerApi.WindowingStrategy.clone(windowingStrategyBase);
    }
    result.windowFn = windowFn.toProto();
    result.windowCoderId = pipeline.context.getCoderId(windowFn.windowCoder());
    result.mergeStatus = windowFn.isMerging()
      ? runnerApi.MergeStatus_Enum.NEEDS_MERGE
      : runnerApi.MergeStatus_Enum.NON_MERGING;
    result.assignsToOneWindow = windowFn.assignsToOneWindow();
    return result;
  }

  constructor(
    private windowFn: WindowFn<W>,
    private windowingStrategyBase:
      | runnerApi.WindowingStrategy
      | undefined = undefined
  ) {
    super("WindowInto(" + windowFn + ", " + windowingStrategyBase + ")");
  }

  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: PCollection<T>
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.JS_WINDOW_INTO_DOFN_URN,
            payload: fakeSeralize({ windowFn: this.windowFn }),
          }),
        })
      ),
    });

    const inputCoder = pipeline.context.getPCollectionCoderId(input);
    return pipeline.createPCollectionInternal<T>(
      inputCoder,
      WindowInto.createWindowingStrategy(
        pipeline,
        this.windowFn,
        this.windowingStrategyBase
      )
    );
  }
}

// TODO: Add restrictions on moving backwards?
export class AssignTimestamps<T> extends PTransform<
  PCollection<T>,
  PCollection<T>
> {
  constructor(private func: (T, Instant) => typeof Instant) {
    super();
  }

  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: PCollection<T>
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.JS_ASSIGN_TIMESTAMPS_DOFN_URN,
            payload: fakeSeralize({ func: this.func }),
          }),
        })
      ),
    });

    return pipeline.createPCollectionInternal<T>(
      pipeline.context.getPCollectionCoderId(input)
    );
  }
}

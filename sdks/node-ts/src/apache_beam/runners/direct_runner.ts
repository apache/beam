import * as protobufjs from "protobufjs";
import Long from "long";

import * as runnerApi from "../proto/beam_runner_api";
import { PTransform, PCollection } from "../proto/beam_runner_api";
import { ProcessBundleDescriptor } from "../proto/beam_fn_api";
import { JobState_Enum } from "../proto/beam_job_api";

import { Pipeline, Root, Impulse, GroupByKey } from "../base";
import { Runner, PipelineResult } from "./runner";
import * as worker from "../worker/worker";
import * as operators from "../worker/operators";
import {
  BoundedWindow,
  GlobalWindow,
  Instant,
  PaneInfo,
  WindowedValue,
} from "../values";
import { PaneInfoCoder } from "../coders/standard_coders";
import { Coder, Context as CoderContext } from "../coders/coders";

export class DirectRunner extends Runner {
  async runPipeline(p): Promise<PipelineResult> {
    const descriptor: ProcessBundleDescriptor = {
      id: "",
      transforms: p.proto.components!.transforms,
      pcollections: p.proto.components!.pcollections,
      windowingStrategies: p.proto.components!.windowingStrategies,
      coders: p.proto.components!.coders,
      environments: p.proto.components!.environments,
    };

    const processor = new worker.BundleProcessor(descriptor, null!, [
      Impulse.urn,
    ]);
    await processor.process("bundle_id", 0);

    return {
      waitUntilFinish: (duration?: number) =>
        Promise.resolve(JobState_Enum.DONE),
    };
  }
}

// Only to be used in direct runner, as this will fire an element per worker, not per pipeline.
class DirectImpulseOperator implements operators.IOperator {
  receiver: operators.Receiver;

  constructor(
    transformId: string,
    transform: PTransform,
    context: operators.OperatorContext
  ) {
    this.receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs))
    );
  }

  process(wvalue: WindowedValue<any>) {
    return operators.NonPromise;
  }

  async startBundle() {
    this.receiver.receive({
      value: new Uint8Array(),
      windows: [new GlobalWindow()],
      pane: PaneInfoCoder.ONE_AND_ONLY_FIRING,
      timestamp: Long.fromValue("-9223372036854775"), // TODO: Pull constant out of proto, or at least as a constant elsewhere.
    });
  }

  async finishBundle() {}
}

operators.registerOperator(Impulse.urn, DirectImpulseOperator);

// Only to be used in direct runner, as this will only group within a single bundle.
// TODO: This could be used as a base for the PGBKOperation operator,
// and then this class could simply invoke that with an unbounded size and the
// concat-to-list CombineFn.
class DirectGbkOperator implements operators.IOperator {
  receiver: operators.Receiver;
  groups: Map<any, any[]>;
  keyCoder: Coder<any>;
  windowCoder: Coder<any>;

  constructor(
    transformId: string,
    transform: PTransform,
    context: operators.OperatorContext
  ) {
    this.receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs))
    );
    const inputPc =
      context.descriptor.pcollections[
        onlyElement(Object.values(transform.inputs))
      ];
    this.keyCoder = context.pipelineContext.getCoder(
      context.descriptor.coders[inputPc.coderId].componentCoderIds[0]
    );
    const windowingStrategy =
      context.descriptor.windowingStrategies[inputPc.windowingStrategyId];
    // TODO: Check or implement triggers, etc.
    if (
      windowingStrategy.mergeStatus != runnerApi.MergeStatus_Enum.NON_MERGING
    ) {
      throw new Error("Non-merging WindowFn: " + windowingStrategy);
    }
    this.windowCoder = context.pipelineContext.getCoder(
      windowingStrategy.windowCoderId
    );
  }

  process(wvalue: WindowedValue<any>) {
    // TODO: Assert non-merging, EOW timestamp, etc.
    for (const window of wvalue.windows) {
      const wkey =
        encodeToBase64(window, this.windowCoder) +
        " " +
        encodeToBase64(wvalue.value.key, this.keyCoder);
      if (!this.groups.has(wkey)) {
        this.groups.set(wkey, []);
      }
      this.groups.get(wkey)!.push(wvalue.value.value);
    }
    return operators.NonPromise;
  }

  async startBundle() {
    this.groups = new Map();
  }

  async finishBundle() {
    const this_ = this;
    for (const [wkey, values] of this.groups) {
      const [encodedWindow, encodedKey] = wkey.split(" ");
      const window = decodeFromBase64(encodedWindow, this.windowCoder);
      const maybePromise = this_.receiver.receive({
        value: {
          key: decodeFromBase64(encodedKey, this.keyCoder),
          value: values,
        },
        windows: [window],
        timestamp: window.maxTimestamp(),
        pane: PaneInfoCoder.ONE_AND_ONLY_FIRING,
      });
      if (maybePromise != operators.NonPromise) {
        await maybePromise;
      }
    }
    this.groups = null!;
  }
}

operators.registerOperator(GroupByKey.urn, DirectGbkOperator);

export function encodeToBase64<T>(element: T, coder: Coder<T>): string {
  const writer = new protobufjs.Writer();
  coder.encode(element, writer, CoderContext.wholeStream);
  return Buffer.from(writer.finish()).toString("base64");
}

export function decodeFromBase64<T>(s: string, coder: Coder<T>): T {
  return coder.decode(
    new protobufjs.Reader(Buffer.from(s, "base64")),
    CoderContext.wholeStream
  );
}

function onlyElement<T>(arg: T[]): T {
  if (arg.length > 1) {
    Error("Expecting exactly one element.");
  }
  return arg[0];
}

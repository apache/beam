import { PTransform, PCollection } from "../proto/beam_runner_api";
import { ProcessBundleDescriptor } from "../proto/beam_fn_api";

import {Runner, Pipeline, Root, Impulse, GroupByKey, PaneInfo, BoundedWindow, PipelineResult} from '../base'
import * as worker from '../worker/worker';
import * as operators from '../worker/operators';
import { WindowedValue } from '../base';


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

        const processor = new worker.BundleProcessor(descriptor, null!, [Impulse.urn]);
        await processor.process("bundle_id", 0);

        return {waitUntilFinish: (duration?: number) => Promise.reject('not implemented')};
    }
}


// Only to be used in direct runner, as this will fire an element per worker, not per pipeline.
class DirectImpulseOperator implements operators.IOperator {
    receiver: operators.Receiver;

    constructor(transformId: string, transform: PTransform, context: operators.OperatorContext) {
        this.receiver = context.getReceiver(onlyElement(Object.values(transform.outputs)));
    }

    process(wvalue: WindowedValue<any>) { }

    startBundle() {
        this.receiver.receive({
            value: '',
            windows: <Array<BoundedWindow>> <unknown> undefined,
            pane: <PaneInfo> <unknown> undefined,
            timestamp: <Date> <unknown> undefined
        });
    }

    finishBundle() { }
}

operators.registerOperator(Impulse.urn, DirectImpulseOperator);


// Only to be used in direct runner, as this will only group within a single bundle.
class DirectGbkOperator implements operators.IOperator {
    receiver: operators.Receiver;
    groups: Map<any, any[]>;

    constructor(transformId: string, transform: PTransform, context: operators.OperatorContext) {
        this.receiver = context.getReceiver(onlyElement(Object.values(transform.outputs)));
    }

    process(wvalue: WindowedValue<any>) {
        if (!this.groups.has(wvalue.value.key)) {
            this.groups.set(wvalue.value.key, []);
        }
        this.groups.get(wvalue.value.key)!.push(wvalue.value.value);
    }

    startBundle() {
        this.groups = new Map();
    }

    finishBundle() {
        const this_ = this;
        this.groups.forEach((values, key, _) => {
            this_.receiver.receive({
                value: { key: key, value: values },
                windows: <Array<BoundedWindow>> <unknown> undefined,
                timestamp: <Date> <unknown> undefined,
                pane: <PaneInfo> <unknown> undefined
            });
        });
        this.groups = null!;
    }
}

operators.registerOperator(GroupByKey.urn, DirectGbkOperator);


function onlyElement<T>(arg: T[]): T {
    if (arg.length > 1) {
        Error("Expecting exactly one element.");
    }
    return arg[0];
}

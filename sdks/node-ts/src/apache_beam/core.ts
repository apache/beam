const runnerApi = require('./proto/beam_runner_api');
const translations = require('./internal/translations')


// TODO(pabloem): Use something better, hah.
var _pcollection_counter = 0;

/**
 * Represents an 'edge' in a graph. These may be PCollections, PCollection views,
 * and Pipelines themselves.
 */
class PValue {
    // TODO: Have a reference to its graph representation
    type: string = "unknown";
    name: string;

    constructor(name: string) {
        this.name = name;
    }

    isPipeline(): boolean {
        return this.type === "pipeline";
    }

    apply(transform: PTransform): PValue {
        return transform.expand(this);
    }

    map(callable: DoFn | GenericCallable): PValue {
        return this.apply(new ParDo(callable));
    }

    // Top-level functions:
    // - flatMap
    // - filter?
}

/**
 * A Pipeline is the base object to start building a Beam DAG. It is the
 * first object that a user creates, and then they may start applying
 * transformations to it to build a DAG.
 */
export class Pipeline extends PValue {
    type: string = "pipeline";
    portablePipeline: any;
}

export class PCollection extends PValue {
    type: string = "pcollection";
    portablePcollection: any;

    constructor(name: string, portablePcollection: any) {
        super(name)
        this.portablePcollection = portablePcollection;
    }
}

export class PTransform {
    expand(input: PValue): PValue {
        throw new Error('Method expand has not been implemented.');
    }
}


/**
 * @returns true if the input is a `DoFn`.
 * 
 * Since type information is lost at runtime, we check the object's attributes
 * to determine whether it's a DoFn or not.
 * 
 * @example
 * Prints "true" for a new `DoFn` but "false" for a function:
 * ```ts
 * console.log(new DoFn());
 * console.log(in => in * 2));
 * ```
 * @param callableOrDoFn 
 * @returns 
 */
function isDoFn(callableOrDoFn: DoFn | GenericCallable) {
    const df = (callableOrDoFn as DoFn)
    if (df.type !== undefined && df.type === "dofn") {
        return true;
    } else {
        return false;
    }
}

export class ParDo extends PTransform {
    private doFn;
    constructor(callableOrDoFn: DoFn | GenericCallable) {
        super()
        if (isDoFn(callableOrDoFn)) {
            this.doFn = callableOrDoFn;
        } else {
            this.doFn = new _CallableWrapperDoFn(callableOrDoFn as GenericCallable);
        }
    }

    expand(input: PValue): PValue {
        console.log(runnerApi.StandardPTransforms_Primitives.PAR_DO.urn);

        const pardoProto = runnerApi.PTransform.create({
            // TODO(pabloem): Get the name for the PTransform
            'uniqueName': 'todouniquename',
            'spec': runnerApi.FunctionSpec.create({
                // TODO(pabloem): URNS ARE DISAPPEARING!
                'urn': runnerApi.StandardPTransforms_Primitives.PAR_DO.urn,
                'payload': runnerApi.ParDoPayload.create({
                    'doFn': runnerApi.FunctionSpec.create()
                })
            }),
            // TODO(pabloem): Add inputs
            'inputs': {},
            'outputs': {'out': 'ref_PCollection_' + _pcollection_counter}
        });
        _pcollection_counter += 1;

        // TODO(pablom): Do this properly
        return new PCollection(pardoProto.outputs.out, pardoProto);
    }
}

interface GenericCallable {
    (input: any): any
}

export class DoFn {
    type: string = "dofn";
    process(element: any) {
        throw new Error('Method process has not been implemented!');
    }

    startBundle(element: any) {
        throw new Error('Method process has not been implemented!');
    }

    finishBundle(element: any) {
        throw new Error('Method process has not been implemented!');
    }
}

class _CallableWrapperDoFn extends DoFn {
    private fn;
    constructor(fn: GenericCallable) {
        super();
        this.fn = fn;
    }
    process(element: any) {
        return this.fn(element);
    }
}

export class Impulse extends PTransform {
    expand(input: PValue): PValue {
        if (!input.isPipeline()) {
            throw new Error("User is attempting to apply Impulse transform to a non-pipeline object.");
        }

        const impulseProto = runnerApi.PTransform.create({
            // TODO(pabloem): Get the name for the PTransform
            'uniqueName': 'todouniquename',
            'spec': runnerApi.FunctionSpec.create({
                'urn': translations.DATA_INPUT_URN,
                'payload': translations.IMPULSE_BUFFER
            }),
            'outputs': {'out': 'ref_PCollection_' + _pcollection_counter}
        });
        _pcollection_counter += 1;
        
        return new PCollection(impulseProto.outputs.out, impulseProto);
    }
}
import { Writer, Reader } from 'protobufjs';
import * as runnerApi from '../proto/beam_runner_api';
import { PipelineContext } from '../base';

interface Class<T> {
    new(...args: any[]): T;
}

class CoderRegistry {
    internal_registry = {};
    get(urn: string): Class<Coder<any>> {
        const constructor: Class<Coder<any>> = this.internal_registry[urn];
        if (constructor === undefined) {
            throw new Error('Could not find coder for URN ' + urn)
        }
        return constructor;
    }

    register(urn: string, coderClass: Class<Coder<any>>) {
        this.internal_registry[urn] = coderClass;
    }
}
export const CODER_REGISTRY = new CoderRegistry();

export enum Context {
    wholeStream = "wholeStream",
    needsDelimiters = "needsDelimiters"
}

export interface Coder<T> {
    encode(element: T, writer: Writer, context: Context);

    decode(reader: Reader, context: Context): T;

    toProto(pipelineContext: PipelineContext): runnerApi.Coder;
}

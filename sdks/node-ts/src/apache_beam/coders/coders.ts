import { Writer, Reader } from 'protobufjs';

interface Class<T> {
    new(...args: any[]): T;
}

class CoderRegistry {
    internal_registry = {};
    get(urn: string): Coder<any> {
        const constructor: Class<Coder<any>> = this.internal_registry[urn];
        if (constructor === undefined) {
            return null!;
        }
        return new constructor();
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
}

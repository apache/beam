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
    selfDelimiting,
    needsDelimiters
}

export interface Coder<T> {
    encode(element: T, writer: Writer, context: Context);

    decode(reader: Reader, context: Context): T;
}

class FakeCoder<T> implements Coder<T> {
    encode(element: T, writer: Writer) {
        throw new Error('Not implemented!');
    }

    decode(reader: Reader, context: Context): T {
        throw new Error('Not implemented!');
    }
}

export class BytesCoder extends FakeCoder<ArrayBuffer> {
    static URN: string = "beam:coder:bytes:v1";
    constructor() {
        super();
    }
}
CODER_REGISTRY.register(BytesCoder.URN, BytesCoder);

export type KV<K, V> = {
    key: K,
    value: V
}

export class KVCoder<K, V> extends FakeCoder<KV<K, V>> {
    static URN: string = "beam:coder:kvcoder:v1";
    type: string = 'kvcoder';

    keyCoder: Coder<K>;
    valueCoder: Coder<V>;

    constructor(keyCoder: Coder<K>, valueCoder: Coder<V>) {
        super();
        this.keyCoder = keyCoder;
        this.valueCoder = valueCoder;
    }
}

export class IterableCoder<T> extends FakeCoder<Iterable<T>> {
    static URN: string = "beam:coder:iterable:v1";
    type: string = 'iterablecoder';

    elementCoder: Coder<T>;
    constructor(elementCoder: Coder<T>) {
        super();
        this.elementCoder = elementCoder;
    }
}
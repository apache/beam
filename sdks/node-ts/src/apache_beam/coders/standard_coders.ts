import * as runnerApi from '../proto/beam_runner_api';
import * as translations from '../internal/translations'

import { Writer, Reader } from 'protobufjs';
import struct = require('python-struct');
import { Coder, Context, CODER_REGISTRY } from "./coders";


class FakeCoder<T> implements Coder<T> {
    encode(value: T, writer: Writer, context: Context) {
        throw new Error('Not implemented!');
    }

    decode(reader: Reader, context: Context): T {
        throw new Error('Not implemented!');
    }
}

export class BytesCoder implements Coder<Uint8Array> {
    static URN: string = "beam:coder:bytes:v1";
    static INSTANCE: BytesCoder = new BytesCoder();
    type: string = "bytescoder";

    encode(value: Uint8Array, writer: Writer, context: Context) {
        var writeBytes =
            function writeBytes_for(val, buf, pos) {
                for (var i = 0; i < val.length; ++i){
                    buf[pos + i] = val[i];
                }

            };

        var len = value.length;
        var hackedWriter = <any>writer;
        switch (context) {
            case Context.wholeStream:
                hackedWriter._push(writeBytes, len, value);
                break;
            case Context.needsDelimiters:
                writer.int32(len)
                hackedWriter._push(writeBytes, len, value);
                break;
            default:
                throw new Error("Unknown type of encoding context");
        }
    }

    decode(reader: Reader, context: Context): Uint8Array {
        switch (context) {
            case Context.wholeStream:
                return reader.buf;
                break;
            case Context.needsDelimiters:
                var length = reader.int32();
                var value = reader.buf.slice(reader.pos, reader.pos + length)
                return value;
            default:
                throw new Error("Unknown type of decoding context");
        }
    }

    toProto(pipelineComponents: runnerApi.Components): runnerApi.Coder {
        return {
            spec: {
                urn: BytesCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(BytesCoder.URN, BytesCoder);

export type KV<K, V> = {
    key: K,
    value: V
}

export class KVCoder<K, V> extends FakeCoder<KV<K, V>> {
    static URN: string = "beam:coder:kv:v1";
    type: string = 'kvcoder';

    keyCoder: Coder<K>;
    valueCoder: Coder<V>;

    constructor(keyCoder: Coder<K>, valueCoder: Coder<V>) {
        super();
        this.keyCoder = keyCoder;
        this.valueCoder = valueCoder;
    }
    toProto(pipelineComponents: runnerApi.Components): runnerApi.Coder {
        return {
            spec: {
                urn: KVCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [
                translations.registerPipelineCoder(this.keyCoder.toProto!(pipelineComponents), pipelineComponents),
                translations.registerPipelineCoder(this.valueCoder.toProto!(pipelineComponents), pipelineComponents),
            ],
        }
    }

    encode(element: KV<K, V>, writer: Writer, context: Context) {
        console.log('encoding key ' + element.key as string + ' and value ' + element.value as string)
        this.keyCoder.encode(element.key, writer, context);
        this.valueCoder.encode(element.value, writer, context);
    }

    decode(reader: Reader, context: Context): KV<K, V> {
        // const encodedKey = BytesCoder.INSTANCE.decode(reader, context);
        // const encodedValue = BytesCoder.INSTANCE.decode(reader, context);
        console.log((this.keyCoder as any));
        console.log((this.valueCoder as any));
        // return {
        //     'key': this.keyCoder.decode(new Reader(encodedKey), Context.wholeStream),
        //     'value': this.valueCoder.decode(new Reader(encodedValue), context)
        // };
        return {
            'key': this.keyCoder.decode(reader, context),
            'value': this.valueCoder.decode(reader, context)
        }
    }
}
CODER_REGISTRY.register(KVCoder.URN, KVCoder);

export class IterableCoder<T> extends FakeCoder<Iterable<T>> {
    static URN: string = "beam:coder:iterable:v1";
    type: string = 'iterablecoder';

    elementCoder: Coder<T>;
    constructor(elementCoder: Coder<T>) {
        super();
        this.elementCoder = elementCoder;
    }
    toProto(pipelineComponents: runnerApi.Components): runnerApi.Coder {
        return {
            spec: {
                urn: IterableCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [
                translations.registerPipelineCoder(this.elementCoder.toProto!(pipelineComponents), pipelineComponents)
            ],
        }
    }
}
CODER_REGISTRY.register(IterableCoder.URN, IterableCoder);

export class StrUtf8Coder implements Coder<String> {
    static URN: string = "beam:coder:string_utf8:v1";
    type: string = 'stringutf8coder';
    encoder = new TextEncoder();
    decoder = new TextDecoder();

    encode(element: String, writer: Writer, context: Context) {
        const encodedElement = this.encoder.encode(element as string);
        BytesCoder.INSTANCE.encode(encodedElement, writer, context);
    }

    decode(reader: Reader, context: Context): String {
        return this.decoder.decode(BytesCoder.INSTANCE.decode(reader, context));
    }
}
CODER_REGISTRY.register(StrUtf8Coder.URN, StrUtf8Coder);


// TODO(pabloem): Is this the most efficient implementation?
export class VarIntCoder implements Coder<Long | Number | BigInt> {
    static URN: string = "beam:coder:varint:v1";
    type: string = "varintcoder";
    encode(element: Number | Long | BigInt, writer: Writer, context: Context) {
        var numEl = element as number
        var encoded = [];
        if (numEl < 0) {
            // TODO(pabloem): Unable to encode negative integers due to JS encoding them
            //    internally as floats. We need to change this somewhat.
            numEl += (1 << 64)
            if (numEl <= 0) {
                throw new Error('Value too large (negative (' + numEl + ')).')
            }
        }
        while (true) {
            var bits = numEl & 0x7F
            numEl >>= 7
            if (numEl) {
                bits |= 0x80
            }
            encoded.push(bits as never);
            if (!numEl) {
                break
            }
        }

        const encodedArr = new Uint8Array(encoded);
        BytesCoder.INSTANCE.encode(encodedArr, writer, context);
    }

    decode(reader: Reader, context: Context): Long | Number | BigInt {
        var shift = 0
        var result = 0
        const encoded: Uint8Array = BytesCoder.INSTANCE.decode(reader, context);
        var i = 0;
        while (true) {
            const byte = encoded[i];
            i++;
            if (byte < 0) {
                throw new Error('VarLong not terminated.')
            }
            const bits = byte & 0x7F
            if (shift >= 64 || (shift >= 63 && bits > 1)) {
                throw new Error('VarLong too long.')
            }
            const shuftedBits = bits << shift;
            result = result | shuftedBits
            shift += 7
            if (!(byte & 0x80)) {
                break
            }
            if (shift >= 32) {
                // TODO(pabloem) support numbers larger than 1 << 32
                // We are unable to decode further into javascript
                break;
            }
            // TODO(pabloem): Remove this because it's giving us trouble!
            // if (result >= 1 << 63) {
            //     result -= 1 << 64
            // }
        }
        return result
    }
}
CODER_REGISTRY.register(VarIntCoder.URN, VarIntCoder);

export class DoubleCoder extends FakeCoder<Number> {
    static URN: string = "beam:coder:double:v1";
    encode(element: Number, writer: Writer, context: Context) {
        writer.double(element as number);
    }

    decode(reader: Reader, context: Context): Number {
        return reader.double();
    }
}
CODER_REGISTRY.register(DoubleCoder.URN, DoubleCoder);

export class BoolCoder extends FakeCoder<Boolean> {
    static URN: string = "beam:coder:bool:v1";
    type: string = "boolcoder";
    encode(element: Boolean, writer: Writer, context: Context) {
        writer.bool(element as boolean);
    }

    decode(reader: Reader, context: Context): Boolean {
        return reader.bool();
    }
}
CODER_REGISTRY.register(BoolCoder.URN, BoolCoder);

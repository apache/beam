import { Writer, Reader } from 'protobufjs';
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

    encode(value: Uint8Array, writer: Writer, context: Context) {
        var writeBytes =
            function writeBytes_for(val, buf, pos) {
                for (var i = 0; i < val.length; ++i)
                    buf[pos + i] = val[i];
            };

        var len = value.length;
        var hackedWriter = <any> writer;
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
CODER_REGISTRY.register(KVCoder.URN, KVCoder);

export class IterableCoder<T> extends FakeCoder<Iterable<T>> {
    static URN: string = "beam:coder:iterable:v1";
    type: string = 'iterablecoder';

    elementCoder: Coder<T>;
    constructor(elementCoder: Coder<T>) {
        super();
        this.elementCoder = elementCoder;
    }
}
CODER_REGISTRY.register(IterableCoder.URN, IterableCoder);

export class StrUtf8Coder extends FakeCoder<String> {
    static URN: string = "beam:coder:string_utf8:v1";
    type: string = 'stringutf8coder';
    encoder = new TextEncoder();
    decoder = new TextDecoder();

    constructor() {
        super();
    }

    encode(element: String, writer: Writer, context: Context) {
        writer.bytes(this.encoder.encode(element as string));
    }

    decode(reader: Reader, context: Context): String {
        return this.decoder.decode(reader.bytes());
    }
}
CODER_REGISTRY.register(StrUtf8Coder.URN, StrUtf8Coder);

export class VarIntCoder extends FakeCoder<Long | Number | BigInt> {
    static URN: string = "beam:coder:varint:v1";
    encode(element: Number | Long | BigInt, writer: Writer, context: Context) {
        writer.uint64(element as number);
    }

    decode(reader: Reader, context: Context): Long | Number | BigInt {
        // TODO(pabloem): How do we deal with large integers?
        return reader.uint64().low;
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
    encode(element: Boolean, writer: Writer, context: Context) {
        writer.bool(element as boolean);
    }

    decode(reader: Reader, context: Context): Boolean {
        return reader.bool();
    }
}
CODER_REGISTRY.register(BoolCoder.URN, BoolCoder);
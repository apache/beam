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

export class IterableCoder<T> extends FakeCoder<Iterable<T>> {
    static URN: string = "beam:coder:iterable:v1";
    type: string = 'iterablecoder';

    elementCoder: Coder<T>;
    constructor(elementCoder: Coder<T>) {
        super();
        this.elementCoder = elementCoder;
    }
}
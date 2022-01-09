import * as runnerApi from '../proto/beam_runner_api';

import { Reader, Writer } from 'protobufjs';
import { Coder, CODER_REGISTRY, Context } from "./coders";
import { PipelineContext } from '../base';
import Long from "long";
import { BoundedWindow, Instant, IntervalWindow, KV, PaneInfo, Timing, WindowedValue } from "../values";

function writeBytes(val, buf, pos) {
    for (var i = 0; i < val.length; ++i) {
        buf[pos + i] = val[i];
    }
}

function writeByte(val, buf, pos) {
    buf[pos] = val & 0xff;
}

export class BytesCoder implements Coder<Uint8Array> {
    static URN: string = "beam:coder:bytes:v1";
    static INSTANCE: BytesCoder = new BytesCoder();
    type: string = "bytescoder";

    encode(value: Uint8Array, writer: Writer, context: Context) {
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
                return reader.buf.slice(reader.pos);
                break;
            case Context.needsDelimiters:
                var length = reader.int32();
                var value = reader.buf.slice(reader.pos, reader.pos + length)
                reader.pos += length
                return value;
            default:
                throw new Error("Unknown type of decoding context");
        }
    }

    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
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

export class KVCoder<K, V> implements Coder<KV<K, V>> {
    static URN: string = "beam:coder:kv:v1";
    type: string = 'kvcoder';

    keyCoder: Coder<K>;
    valueCoder: Coder<V>;

    constructor(keyCoder: Coder<K>, valueCoder: Coder<V>) {
        this.keyCoder = keyCoder;
        this.valueCoder = valueCoder;
    }
    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: KVCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [
                pipelineContext.getCoderId(this.keyCoder),
                pipelineContext.getCoderId(this.valueCoder),
            ],
        }
    }

    encode(element: KV<K, V>, writer: Writer, context: Context) {
        this.keyCoder.encode(element.key, writer, Context.needsDelimiters);
        this.valueCoder.encode(element.value, writer, context);
    }

    decode(reader: Reader, context: Context): KV<K, V> {
        var key = this.keyCoder.decode(reader, Context.needsDelimiters)
        var value = this.valueCoder.decode(reader, context)
        return {
            'key': key,
            'value': value
        }
    }
}
CODER_REGISTRY.register(KVCoder.URN, KVCoder);

function swapEndian32(x: number): number {
    return ((x & 0xFF000000) >> 24)
        | ((x & 0x00FF0000) >> 8)
        | ((x & 0x0000FF00) << 8)
        | ((x & 0x000000FF) << 24);
}

export class IterableCoder<T> implements Coder<Iterable<T>> {
    static URN: string = "beam:coder:iterable:v1";
    type: string = 'iterablecoder';

    elementCoder: Coder<T>;
    constructor(elementCoder: Coder<T>) {
        this.elementCoder = elementCoder;
    }
    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: IterableCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [
                pipelineContext.getCoderId(this.elementCoder),
            ],
        }
    }

    encode(element: Iterable<T>, writer: Writer, context: Context) {
        if ((element as Array<T>).length !== undefined) {
            const eArray = (element as Array<T>)
            writer.fixed32(swapEndian32(eArray.length))
            for (let i = 0; i < eArray.length; ++i) {
                this.elementCoder.encode(eArray[i], writer, Context.needsDelimiters)
            }
        } else {
            throw new Error('Length-unknown iterables are not yet implemented')
        }
    }

    decode(reader: Reader, context: Context): Iterable<T> {
        const len = swapEndian32(reader.fixed32());
        if (len >= 0) {
            const result = new Array(len)
            for (let i = 0; i < len; i++) {
                result[i] = this.elementCoder.decode(reader, Context.needsDelimiters)
            }
            return result;
        } else {
            var result = new Array();
            while (true) {
                // TODO: these actually go up to int64
                var count = reader.int32();
                if (count === 0) {
                    return result;
                }
                for (var i = 0; i < count; i++) {
                    result.push(this.elementCoder.decode(reader, Context.needsDelimiters))
                }
            }
        }
    }
}
CODER_REGISTRY.register(IterableCoder.URN, IterableCoder);

export class LengthPrefixedCoder<T> implements Coder<T> {
    static URN: string = "beam:coder:length_prefix:v1";

    elementCoder: Coder<T>;
    constructor(elementCoder: Coder<T>) {
        this.elementCoder = elementCoder;
    }
    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: LengthPrefixedCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [
                pipelineContext.getCoderId(this.elementCoder),
            ],
        }
    }

    encode(element: T, writer: Writer, context: Context) {
        writer.fork();
        this.elementCoder.encode(element, writer, Context.wholeStream);
        writer.ldelim();
    }

    decode(reader: Reader, context: Context): T {
        return this.elementCoder.decode(new Reader(reader.bytes()), Context.wholeStream);
    }
}
CODER_REGISTRY.register(LengthPrefixedCoder.URN, LengthPrefixedCoder);

export class FullWindowedValueCoder<T, W extends BoundedWindow> implements Coder<WindowedValue<T>> {
    static URN: string = "beam:coder:windowed_value:v1";
    windowIterableCoder: IterableCoder<W>  // really W

    constructor(public elementCoder: Coder<T>, public windowCoder: Coder<W>) {
        this.windowIterableCoder = new IterableCoder(windowCoder);
    }
    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: FullWindowedValueCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [
                pipelineContext.getCoderId(this.elementCoder),
                pipelineContext.getCoderId(this.windowCoder),
            ],
        }
    }

    encode(windowedValue: WindowedValue<T>, writer: Writer, context: Context) {
        InstantCoder.INSTANCE.encode(windowedValue.timestamp, writer, Context.needsDelimiters)
        this.windowIterableCoder.encode(<Array<W>>windowedValue.windows, writer, Context.needsDelimiters); // Windows.
        PaneInfoCoder.INSTANCE.encode(windowedValue.pane, writer, Context.needsDelimiters)
        this.elementCoder.encode(windowedValue.value, writer, context);
    }

    decode(reader: Reader, context: Context): WindowedValue<T> {
        const timestamp = InstantCoder.INSTANCE.decode(reader, Context.needsDelimiters)
        const windows = this.windowIterableCoder.decode(reader, Context.needsDelimiters)
        const pane = PaneInfoCoder.INSTANCE.decode(reader, Context.needsDelimiters)
        const value = this.elementCoder.decode(reader, context)
        return {
            value: value,
            windows: <Array<BoundedWindow>>windows,
            pane: pane,
            timestamp: timestamp
        };
    }
}
CODER_REGISTRY.register(FullWindowedValueCoder.URN, FullWindowedValueCoder);

export class GlobalWindow implements BoundedWindow {
  maxTimestamp(): Instant {
      return Long.fromValue("9223371950454775");  // TODO: Pull constant out of proto, or at least as a constant elsewhere.
  }
}

export class GlobalWindowCoder implements Coder<GlobalWindow> {
    static URN: string = "beam:coder:global_window:v1";
    static INSTANCE: GlobalWindowCoder = new GlobalWindowCoder();

    encode(value: GlobalWindow, writer: Writer, context: Context) {
    }

    decode(reader: Reader, context: Context) {
        return new GlobalWindow()
    }

    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: GlobalWindowCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(GlobalWindowCoder.URN, GlobalWindowCoder);

// 4 bits
enum PaneInfoEncoding {
    NO_INDEX = 0b0000,

    ONE_INDEX = 0b0001,

    // both overall pane index and also non-speculative index
    TWO_INDICES = 0b0010
}
export class PaneInfoCoder implements Coder<PaneInfo> {
    static INSTANCE = new PaneInfoCoder()
    static ONE_AND_ONLY_FIRING = PaneInfoCoder.INSTANCE.decode(new Reader(new Uint8Array([0x09])), null!);

    private static decodeTiming(timingNumber): Timing {
        switch (timingNumber) {
            case 0b00:
                return Timing.EARLY
            case 0b01:
                return Timing.ON_TIME
            case 0b10:
                return Timing.LATE
            case 0b11:
                return Timing.UNKNOWN
            default:
                throw new Error("Timing number 0b" + timingNumber.toString(2) + " has more than two bits of info")
        }
    }

    private static encodeTiming(timing: Timing): number {
        switch (timing) {
            case Timing.EARLY:
                return 0b00
            case Timing.ON_TIME:
                return 0b01
            case Timing.LATE:
                return 0b10
            case Timing.UNKNOWN:
                return 0b11
            default:
                throw new Error("Unknown timing constant: " + timing)
        }
    }

    private static chooseEncoding(value: PaneInfo): number {
        if ((value.index == 0 && value.onTimeIndex == 0) || value.timing == Timing.UNKNOWN) {
            return PaneInfoEncoding.NO_INDEX;
        }
        else if (value.index == value.onTimeIndex || value.timing == Timing.EARLY) {
            return PaneInfoEncoding.ONE_INDEX
        } else {
            return PaneInfoEncoding.TWO_INDICES;
        }
    }

    decode(reader: Reader, context: Context): PaneInfo {
        const headerByte = reader.buf[reader.pos]
        reader.pos += 1

        // low 4 bits are used regardless of encoding
        const isFirst = !!(headerByte & 0b00000001)
        const isLast = !!(headerByte & 0b00000010)
        const timing = PaneInfoCoder.decodeTiming((headerByte & 0b00001100) >> 2)

        // High 4 bits indicate how to interpret remaining 4 bits
        // and whether to read more from the input stream
        const encoding = (headerByte & 0xF0) >> 4
        switch (encoding) {
            case PaneInfoEncoding.NO_INDEX:
                // No index necessary, common case where there is only one (non-speculative) pane
                return {
                    isFirst: isFirst,
                    isLast: isLast,
                    index: 0,
                    onTimeIndex: 0,
                    timing: timing
                }

            case PaneInfoEncoding.ONE_INDEX:
                // Only pane index included, as the non-speculative index can be derived
                const onlyIndex = VarIntCoder.INSTANCE.decode(reader, Context.needsDelimiters)
                return {
                    isFirst: isFirst,
                    isLast: isLast,
                    index: onlyIndex,
                    onTimeIndex: timing == Timing.EARLY ? -1 : onlyIndex,
                    timing: timing
                }

            case PaneInfoEncoding.TWO_INDICES:
                // Both pane index and non-speculative index included
                const paneIndex = VarIntCoder.INSTANCE.decode(reader, Context.needsDelimiters)
                const nonSpeculativeIndex = VarIntCoder.INSTANCE.decode(reader, Context.needsDelimiters)
                return {
                    isFirst: isFirst,
                    isLast: isLast,
                    index: paneIndex,
                    onTimeIndex: nonSpeculativeIndex,
                    timing: timing
                }
            default:
                throw new Error("Unknown PaneInfo encoding 0x" + encoding.toString(16))
        }
    }

    encode(value: PaneInfo, writer: Writer, context: Context) {
        // low 4 bits are used regardless of encoding
        const low4 = ((value.isFirst ? 0b000000001 : 0)
            | (value.isLast ? 0b00000010 : 0)
            | (PaneInfoCoder.encodeTiming(value.timing) << 2));

        const encodingNibble: PaneInfoEncoding = PaneInfoCoder.chooseEncoding(value)
        var hackedWriter = <any>writer;
        hackedWriter._push(writeByte, 1, low4 | (encodingNibble << 4));

        switch (encodingNibble) {
            case PaneInfoEncoding.NO_INDEX:
                // the header byte contains all the info
                return;
            case PaneInfoEncoding.ONE_INDEX:
                VarIntCoder.INSTANCE.encode(value.index, writer, context)
                return;
            case PaneInfoEncoding.TWO_INDICES:
                VarIntCoder.INSTANCE.encode(value.index, writer, Context.needsDelimiters)
                VarIntCoder.INSTANCE.encode(value.onTimeIndex, writer, context)
                return;
            default:
                throw new Error("Unknown PaneInfo encoding: " + encodingNibble)
        }
    }

    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        throw new Error("No proto encoding for PaneInfoCoder, always part of WindowedValue codec");
    }
}

export class InstantCoder implements Coder<Instant> {
    static INSTANCE: InstantCoder = new InstantCoder();
    static INSTANT_BYTES = 8

    decode(reader: Reader, context: Context): Instant {
        const shiftedMillis = Long.fromBytesBE(Array.from(reader.buf.slice(reader.pos, reader.pos + InstantCoder.INSTANT_BYTES)));
        reader.pos += InstantCoder.INSTANT_BYTES
        return shiftedMillis.add(Long.MIN_VALUE)
    }

    encode(element: Instant, writer: Writer, context: Context) {
        const shiftedMillis = element.sub(Long.MIN_VALUE)
        const bytes = Uint8Array.from(shiftedMillis.toBytesBE());
        BytesCoder.INSTANCE.encode(bytes, writer, Context.wholeStream)
    }

    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return <runnerApi.Coder><unknown>undefined;
    }
}

export class IntervalWindowCoder implements Coder<IntervalWindow> {
    static URN: string = "beam:coder:interval_window:v1";
    static INSTANCE: IntervalWindowCoder = new IntervalWindowCoder();
    static DURATION_BYTES = 8

    encode(value: IntervalWindow, writer: Writer, context: Context) {
        InstantCoder.INSTANCE.encode(value.end, writer, context)
        writer.int64(value.end.sub(value.start))
    }

    decode(reader: Reader, context: Context) {
        var end = InstantCoder.INSTANCE.decode(reader, context)
        var duration = <Long>reader.int64()
        return new IntervalWindow(end.sub(duration), end)
    }

    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: IntervalWindowCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(IntervalWindowCoder.URN, IntervalWindowCoder);

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
    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: StrUtf8Coder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(StrUtf8Coder.URN, StrUtf8Coder);


export class VarIntCoder implements Coder<number> {
    static URN: string = "beam:coder:varint:v1";
    static INSTANCE = new VarIntCoder()

    type: string = "varintcoder";
    encode(element: Number | Long | BigInt, writer: Writer, context: Context) {
        var numEl = element as number
        writer.int32(numEl)
        return
    }

    decode(reader: Reader, context: Context): number {
        return reader.int32();
    }

    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: VarIntCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(VarIntCoder.URN, VarIntCoder);

export class DoubleCoder implements Coder<number> {
    static URN: string = "beam:coder:double:v1";
    encode(element: number, writer: Writer, context: Context) {
        const farr = new Float64Array([element]);
        const barr = new Uint8Array(farr.buffer).reverse();
        BytesCoder.INSTANCE.encode(barr, writer, Context.wholeStream)
    }

    decode(reader: Reader, context: Context): number {
        const barr = new Uint8Array(reader.buf, reader.pos, 8)
        const dView = new DataView(barr.buffer);
        reader.float()
        return dView.getFloat64(0, false)
    }
    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: DoubleCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(DoubleCoder.URN, DoubleCoder);

export class BoolCoder implements Coder<Boolean> {
    static URN: string = "beam:coder:bool:v1";
    type: string = "boolcoder";
    encode(element: Boolean, writer: Writer, context: Context) {
        writer.bool(element as boolean);
    }

    decode(reader: Reader, context: Context): Boolean {
        return reader.bool();
    }
    toProto(pipelineContext: PipelineContext): runnerApi.Coder {
        return {
            spec: {
                urn: BoolCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(BoolCoder.URN, BoolCoder);

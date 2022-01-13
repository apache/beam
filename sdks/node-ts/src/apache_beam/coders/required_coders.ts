import * as runnerApi from "../proto/beam_runner_api";

import { Reader, Writer } from "protobufjs";
import {
  Coder,
  Context,
  globalRegistry,
  writeRawByte,
  writeRawBytes,
} from "./coders";
import { PipelineContext } from "../base";
import Long from "long";
import {
  BoundedWindow,
  GlobalWindow,
  Instant,
  IntervalWindow,
  KV,
  PaneInfo,
  Timing,
  WindowedValue,
} from "../values";

/**
 * @fileoverview Defines all of the Apache Beam required coders.
 *
 * These are the coders necessary for encoding the data types required by
 * the Apache Beam model. They provide standardized ways of encode data for
 * communication between the runner, the Beam workers, and the user's code.
 * For example for any aggregations the runner and the SDK need to agree on
 * the encoding of key-value pairs; so that the SDK will encode keys properly,
 * and the runner will be able to group elements of the
 * same key together.
 *
 * The formal specifications for these coders can be found in
 * model/pipeline/src/main/proto/beam_runner_api.proto
 */

/**
 * Coder for byte-array data types.
 */
export class BytesCoder implements Coder<Uint8Array> {
  static URN: string = "beam:coder:bytes:v1";
  static INSTANCE: BytesCoder = new BytesCoder();
  type: string = "bytescoder";

  /**
   * Encode the input element (a byte-string) into the output byte stream from `writer`.
   * If context is `needsDelimiters`, the byte string is encoded prefixed with a
   * varint representing its length.
   *
   * If the context is `wholeStream`, the byte string is encoded as-is.
   *
   * For example:
   * ```js
   * const w1 = new Writer()
   * const data = new TextEncoder().encode("bytes")
   * new BytesCoder().encode(data, w1, Context.needsDelimiters)
   * console.log(w1.finish())  // ==> prints Uint8Array(6) [ 5, 98, 121, 116, 101, 115 ], where 5 is the length prefix.
   * const w2 = new Writer()
   * new BytesCoder().encode("bytes", w1, Context.wholeStream)
   * console.log(w2.finish())  // ==> prints Uint8Array(5) [ 98, 121, 116, 101, 115 ], without the length prefix
   * ```
   * @param value - a byte array to encode. This represents an element to be encoded.
   * @param writer - a writer to access the stream of bytes with encoded data
   * @param context - whether to encode the data with delimiters (`Context.needsDelimiters`), or without (`Context.wholeStream`).
   */
  encode(value: Uint8Array, writer: Writer, context: Context) {
    var len = value.length;
    var hackedWriter = <any>writer;
    switch (context) {
      case Context.wholeStream:
        writeRawBytes(value, writer);
        break;
      case Context.needsDelimiters:
        writer.bytes(value);
        break;
      default:
        throw new Error("Unknown type of encoding context");
    }
  }

  /**
   * Decode the input byte stream into a byte array.
   * If context is `needsDelimiters`, the first bytes will be interpreted as a var-int32 encoding
   * the length of the data.
   *
   * If the context is `wholeStream`, the whole input stream is decoded as-is.
   *
   * @param reader - a reader to access the input byte stream
   * @param context - whether the data is encoded with delimiters (`Context.needsDelimiters`), or without (`Context.wholeStream`).
   * @returns
   */
  decode(reader: Reader, context: Context): Uint8Array {
    switch (context) {
      case Context.wholeStream:
        return reader.buf.slice(reader.pos);
        break;
      case Context.needsDelimiters:
        var length = reader.int32();
        var value = reader.buf.slice(reader.pos, reader.pos + length);
        reader.pos += length;
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
    };
  }
}

globalRegistry().register(BytesCoder.URN, BytesCoder);

export class KVCoder<K, V> implements Coder<KV<K, V>> {
  static URN: string = "beam:coder:kv:v1";
  type: string = "kvcoder";

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
    };
  }

  encode(element: KV<K, V>, writer: Writer, context: Context) {
    this.keyCoder.encode(element.key, writer, Context.needsDelimiters);
    this.valueCoder.encode(element.value, writer, context);
  }

  decode(reader: Reader, context: Context): KV<K, V> {
    var key = this.keyCoder.decode(reader, Context.needsDelimiters);
    var value = this.valueCoder.decode(reader, context);
    return {
      key: key,
      value: value,
    };
  }
}

globalRegistry().register(KVCoder.URN, KVCoder);

/**
 * Swap the endianness of the input number. The input number is expected to be
 * a 32-bit integer.
 */
function swapEndian32(x: number): number {
  return (
    ((x & 0xff000000) >> 24) |
    ((x & 0x00ff0000) >> 8) |
    ((x & 0x0000ff00) << 8) |
    ((x & 0x000000ff) << 24)
  );
}

export class IterableCoder<T> implements Coder<Iterable<T>> {
  static URN: string = "beam:coder:iterable:v1";
  type: string = "iterablecoder";

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
      componentCoderIds: [pipelineContext.getCoderId(this.elementCoder)],
    };
  }

  encode(element: Iterable<T>, writer: Writer, context: Context) {
    if ((element as Array<T>).length !== undefined) {
      const eArray = element as Array<T>;
      writer.fixed32(swapEndian32(eArray.length));
      for (let i = 0; i < eArray.length; ++i) {
        this.elementCoder.encode(eArray[i], writer, Context.needsDelimiters);
      }
    } else {
      throw new Error("Length-unknown iterables are not yet implemented");
    }
  }

  decode(reader: Reader, context: Context): Iterable<T> {
    const len = swapEndian32(reader.fixed32());
    if (len >= 0) {
      const result = new Array(len);
      for (let i = 0; i < len; i++) {
        result[i] = this.elementCoder.decode(reader, Context.needsDelimiters);
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
          result.push(
            this.elementCoder.decode(reader, Context.needsDelimiters)
          );
        }
      }
    }
  }
}

globalRegistry().register(IterableCoder.URN, IterableCoder);

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
      componentCoderIds: [pipelineContext.getCoderId(this.elementCoder)],
    };
  }

  encode(element: T, writer: Writer, context: Context) {
    writer.fork();
    this.elementCoder.encode(element, writer, Context.wholeStream);
    writer.ldelim();
  }

  decode(reader: Reader, context: Context): T {
    return this.elementCoder.decode(
      new Reader(reader.bytes()),
      Context.wholeStream
    );
  }
}

globalRegistry().register(LengthPrefixedCoder.URN, LengthPrefixedCoder);

////////// Windowing-related coders. //////////

export class FullWindowedValueCoder<T, W extends BoundedWindow>
  implements Coder<WindowedValue<T>>
{
  static URN: string = "beam:coder:windowed_value:v1";
  windowIterableCoder: IterableCoder<W>; // really W

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
    };
  }

  encode(windowedValue: WindowedValue<T>, writer: Writer, context: Context) {
    InstantCoder.INSTANCE.encode(
      windowedValue.timestamp,
      writer,
      Context.needsDelimiters
    );
    this.windowIterableCoder.encode(
      <Array<W>>windowedValue.windows,
      writer,
      Context.needsDelimiters
    ); // Windows.
    PaneInfoCoder.INSTANCE.encode(
      windowedValue.pane,
      writer,
      Context.needsDelimiters
    );
    this.elementCoder.encode(windowedValue.value, writer, context);
  }

  decode(reader: Reader, context: Context): WindowedValue<T> {
    const timestamp = InstantCoder.INSTANCE.decode(
      reader,
      Context.needsDelimiters
    );
    const windows = this.windowIterableCoder.decode(
      reader,
      Context.needsDelimiters
    );
    const pane = PaneInfoCoder.INSTANCE.decode(reader, Context.needsDelimiters);
    const value = this.elementCoder.decode(reader, context);
    return {
      value: value,
      windows: <Array<BoundedWindow>>windows,
      pane: pane,
      timestamp: timestamp,
    };
  }
}

globalRegistry().register(FullWindowedValueCoder.URN, FullWindowedValueCoder);

export class GlobalWindowCoder implements Coder<GlobalWindow> {
  static URN: string = "beam:coder:global_window:v1";
  static INSTANCE: GlobalWindowCoder = new GlobalWindowCoder();

  encode(value: GlobalWindow, writer: Writer, context: Context) {}

  decode(reader: Reader, context: Context) {
    return new GlobalWindow();
  }

  toProto(pipelineContext: PipelineContext): runnerApi.Coder {
    return {
      spec: {
        urn: GlobalWindowCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}

globalRegistry().register(GlobalWindowCoder.URN, GlobalWindowCoder);

export class InstantCoder implements Coder<Instant> {
  static INSTANCE: InstantCoder = new InstantCoder();
  static INSTANT_BYTES = 8;

  decode(reader: Reader, context: Context): Instant {
    const shiftedMillis = Long.fromBytesBE(
      Array.from(
        reader.buf.slice(reader.pos, reader.pos + InstantCoder.INSTANT_BYTES)
      )
    );
    reader.pos += InstantCoder.INSTANT_BYTES;
    return shiftedMillis.add(Long.MIN_VALUE);
  }

  encode(element: Instant, writer: Writer, context: Context) {
    const shiftedMillis = element.sub(Long.MIN_VALUE);
    const bytes = Uint8Array.from(shiftedMillis.toBytesBE());
    writeRawBytes(bytes, writer);
  }

  toProto(pipelineContext: PipelineContext): runnerApi.Coder {
    return <runnerApi.Coder>(<unknown>undefined);
  }
}

// 4 bits
enum PaneInfoEncoding {
  NO_INDEX = 0b0000,

  ONE_INDEX = 0b0001,

  // both overall pane index and also non-speculative index
  TWO_INDICES = 0b0010,
}
export class PaneInfoCoder implements Coder<PaneInfo> {
  static INSTANCE = new PaneInfoCoder();
  static ONE_AND_ONLY_FIRING = PaneInfoCoder.INSTANCE.decode(
    new Reader(new Uint8Array([0x09])),
    null!
  );

  private static decodeTiming(timingNumber): Timing {
    switch (timingNumber) {
      case 0b00:
        return Timing.EARLY;
      case 0b01:
        return Timing.ON_TIME;
      case 0b10:
        return Timing.LATE;
      case 0b11:
        return Timing.UNKNOWN;
      default:
        throw new Error(
          "Timing number 0b" +
            timingNumber.toString(2) +
            " has more than two bits of info"
        );
    }
  }

  private static encodeTiming(timing: Timing): number {
    switch (timing) {
      case Timing.EARLY:
        return 0b00;
      case Timing.ON_TIME:
        return 0b01;
      case Timing.LATE:
        return 0b10;
      case Timing.UNKNOWN:
        return 0b11;
      default:
        throw new Error("Unknown timing constant: " + timing);
    }
  }

  private static chooseEncoding(value: PaneInfo): number {
    if (
      (value.index == 0 && value.onTimeIndex == 0) ||
      value.timing == Timing.UNKNOWN
    ) {
      return PaneInfoEncoding.NO_INDEX;
    } else if (
      value.index == value.onTimeIndex ||
      value.timing == Timing.EARLY
    ) {
      return PaneInfoEncoding.ONE_INDEX;
    } else {
      return PaneInfoEncoding.TWO_INDICES;
    }
  }

  decode(reader: Reader, context: Context): PaneInfo {
    const headerByte = reader.buf[reader.pos];
    reader.pos += 1;

    // low 4 bits are used regardless of encoding
    const isFirst = !!(headerByte & 0b00000001);
    const isLast = !!(headerByte & 0b00000010);
    const timing = PaneInfoCoder.decodeTiming((headerByte & 0b00001100) >> 2);

    // High 4 bits indicate how to interpret remaining 4 bits
    // and whether to read more from the input stream
    const encoding = (headerByte & 0xf0) >> 4;
    switch (encoding) {
      case PaneInfoEncoding.NO_INDEX:
        // No index necessary, common case where there is only one (non-speculative) pane
        return {
          isFirst: isFirst,
          isLast: isLast,
          index: 0,
          onTimeIndex: 0,
          timing: timing,
        };

      case PaneInfoEncoding.ONE_INDEX:
        // Only pane index included, as the non-speculative index can be derived
        const onlyIndex = reader.int32();
        return {
          isFirst: isFirst,
          isLast: isLast,
          index: onlyIndex,
          onTimeIndex: timing == Timing.EARLY ? -1 : onlyIndex,
          timing: timing,
        };

      case PaneInfoEncoding.TWO_INDICES:
        // Both pane index and non-speculative index included
        const paneIndex = reader.int32();
        const nonSpeculativeIndex = reader.int32();
        return {
          isFirst: isFirst,
          isLast: isLast,
          index: paneIndex,
          onTimeIndex: nonSpeculativeIndex,
          timing: timing,
        };
      default:
        throw new Error("Unknown PaneInfo encoding 0x" + encoding.toString(16));
    }
  }

  encode(value: PaneInfo, writer: Writer, context: Context) {
    // low 4 bits are used regardless of encoding
    const low4 =
      (value.isFirst ? 0b000000001 : 0) |
      (value.isLast ? 0b00000010 : 0) |
      (PaneInfoCoder.encodeTiming(value.timing) << 2);

    const encodingNibble: PaneInfoEncoding =
      PaneInfoCoder.chooseEncoding(value);
    writeRawByte(low4 | (encodingNibble << 4), writer);

    switch (encodingNibble) {
      case PaneInfoEncoding.NO_INDEX:
        // the header byte contains all the info
        return;
      case PaneInfoEncoding.ONE_INDEX:
        writer.int32(value.index);
        return;
      case PaneInfoEncoding.TWO_INDICES:
        writer.int32(value.index);
        writer.int32(value.onTimeIndex);
        return;
      default:
        throw new Error("Unknown PaneInfo encoding: " + encodingNibble);
    }
  }

  toProto(pipelineContext: PipelineContext): runnerApi.Coder {
    throw new Error(
      "No proto encoding for PaneInfoCoder, always part of WindowedValue codec"
    );
  }
}

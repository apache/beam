/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as runnerApi from "../proto/beam_runner_api";

import { Reader, Writer } from "protobufjs";
import {
  Coder,
  Context,
  ProtoContext,
  globalRegistry,
  writeRawBytes,
} from "./coders";
import { BytesCoder, InstantCoder } from "./required_coders";
import Long from "long";
import {
  Window,
  Instant,
  IntervalWindow,
  KV,
  PaneInfo,
  Timing,
  WindowedValue,
} from "../values";

// Historical
export * from "./required_coders";

/**
 * @fileoverview Defines all of the Apache Beam standard coders.
 *
 * Beyond required coders, standard coders provide a efficient ways of encode
 * data for communication between the runner and various Beam workers for
 * types that commonly cross process boundaries. Though none of these coders
 * are strictly necessary, if encodings are given for these types it is highly
 * advised to use these definitions that are interoperable with runners and
 * other SDKs.
 *
 * For schema-aware transforms RowCoder, which is a coder for rows of data
 * with a predetermined schema, is also advised.
 *
 * The formal specifications for these coders can be found in
 * model/pipeline/src/main/proto/beam_runner_api.proto
 */

export class StrUtf8Coder implements Coder<String> {
  static URN: string = "beam:coder:string_utf8:v1";
  type: string = "stringutf8coder";
  encoder = new TextEncoder();
  decoder = new TextDecoder();

  encode(element: String, writer: Writer, context: Context) {
    const encodedElement = this.encoder.encode(element as string);
    BytesCoder.INSTANCE.encode(encodedElement, writer, context);
  }

  decode(reader: Reader, context: Context): String {
    return this.decoder.decode(BytesCoder.INSTANCE.decode(reader, context));
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: StrUtf8Coder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}
globalRegistry().register(StrUtf8Coder.URN, StrUtf8Coder);

export class VarIntCoder implements Coder<number> {
  static URN: string = "beam:coder:varint:v1";
  static INSTANCE = new VarIntCoder();

  type: string = "varintcoder";

  encode(element: Number | Long | BigInt, writer: Writer, context: Context) {
    var numEl = element as number;
    writer.int32(numEl);
    return;
  }

  decode(reader: Reader, context: Context): number {
    return reader.int32();
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: VarIntCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}
globalRegistry().register(VarIntCoder.URN, VarIntCoder);

export class DoubleCoder implements Coder<number> {
  static URN: string = "beam:coder:double:v1";

  encode(element: number, writer: Writer, context: Context) {
    const farr = new Float64Array([element]);
    const barr = new Uint8Array(farr.buffer).reverse();
    writeRawBytes(barr, writer);
  }

  decode(reader: Reader, context: Context): number {
    const barr = new Uint8Array(reader.buf, reader.pos, 8);
    const dView = new DataView(barr.buffer);
    reader.float();
    return dView.getFloat64(0, false);
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: DoubleCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}
globalRegistry().register(DoubleCoder.URN, DoubleCoder);

export class BoolCoder implements Coder<Boolean> {
  static URN: string = "beam:coder:bool:v1";
  type: string = "boolcoder";

  encode(element: Boolean, writer: Writer, context: Context) {
    writer.bool(element as boolean);
  }

  decode(reader: Reader, context: Context): Boolean {
    return reader.bool();
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: BoolCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}
globalRegistry().register(BoolCoder.URN, BoolCoder);

export class NullableCoder<T> implements Coder<T | undefined> {
  static URN: string = "beam:coder:nullable:v1";
  type: string = "nullablecoder";

  elementCoder: Coder<T>;

  constructor(elementCoder: Coder<T>) {
    this.elementCoder = elementCoder;
  }

  encode(element: T | undefined, writer: Writer, context: Context) {
    if (element === null || element === undefined) {
      writer.bool(false);
    } else {
      writer.bool(true);
      this.elementCoder.encode(element, writer, context);
    }
  }

  decode(reader: Reader, context: Context): T | undefined {
    if (reader.bool()) {
      return this.elementCoder.decode(reader, context);
    } else {
      return undefined;
    }
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: NullableCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [pipelineContext.getCoderId(this.elementCoder)],
    };
  }
}
globalRegistry().register(NullableCoder.URN, NullableCoder);

export class IntervalWindowCoder implements Coder<IntervalWindow> {
  static URN: string = "beam:coder:interval_window:v1";
  static INSTANCE: IntervalWindowCoder = new IntervalWindowCoder();

  encode(value: IntervalWindow, writer: Writer, context: Context) {
    InstantCoder.INSTANCE.encode(value.end, writer, context);
    writer.int64(value.end.sub(value.start));
  }

  decode(reader: Reader, context: Context) {
    var end = InstantCoder.INSTANCE.decode(reader, context);
    var duration = <Long>reader.int64();
    return new IntervalWindow(end.sub(duration), end);
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: IntervalWindowCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}

globalRegistry().register(IntervalWindowCoder.URN, IntervalWindowCoder);

import { requireForSerialization } from "../serialization";
requireForSerialization("apache_beam.coders.standard_coders", exports);

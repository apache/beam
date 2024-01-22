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
import { IntervalWindow } from "../values";
import { requireForSerialization } from "../serialization";
import { packageName } from "../utils/packageJson";

// Historical
export * from "./required_coders";

/**
 * @fileoverview Defines all of the Apache Beam standard coders.
 *
 * Beyond required coders, standard coders provide an efficient way to encode
 * data for communication between the runner and various Beam workers for
 * types that commonly cross process boundaries. Though none of these coders
 * is strictly necessary, if encodings are given for these types it is highly
 * advised to use these definitions that are interoperable with runners and
 * other SDKs.
 *
 * For the schema-aware transform RowCoder, which is a coder for rows of data
 * with a predetermined schema, it is also advised.
 *
 * The formal specifications for these coders can be found in
 * model/pipeline/src/main/proto/beam_runner_api.proto
 */

export class StrUtf8Coder implements Coder<string> {
  static URN = "beam:coder:string_utf8:v1";
  type = "stringutf8coder";
  encoder = new TextEncoder();
  decoder = new TextDecoder();

  encode(element: string, writer: Writer, context: Context) {
    const encodedElement = this.encoder.encode(element);
    BytesCoder.INSTANCE.encode(encodedElement, writer, context);
  }

  decode(reader: Reader, context: Context): string {
    return this.decoder.decode(BytesCoder.INSTANCE.decode(reader, context));
  }

  toProto(): runnerApi.Coder {
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
  static URN = "beam:coder:varint:v1";
  static INSTANCE = new VarIntCoder();

  type = "varintcoder";

  encode(element: number, writer: Writer) {
    writer.int32(element);
  }

  decode(reader: Reader): number {
    return reader.int32();
  }

  toProto(): runnerApi.Coder {
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
  static URN = "beam:coder:double:v1";

  encode(element: number, writer: Writer) {
    const farr = new Float64Array([element]);
    const barr = new Uint8Array(farr.buffer).reverse();
    writeRawBytes(barr, writer);
  }

  decode(reader: Reader): number {
    const barr = new Uint8Array(reader.buf);
    const dView = new DataView(barr.buffer.slice(reader.pos, reader.pos + 8));
    reader.double();
    return dView.getFloat64(0, false);
  }

  toProto(): runnerApi.Coder {
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

export class BoolCoder implements Coder<boolean> {
  static URN = "beam:coder:bool:v1";
  type = "boolcoder";

  encode(element: boolean, writer: Writer) {
    writer.bool(element);
  }

  decode(reader: Reader): boolean {
    return reader.bool();
  }

  toProto(): runnerApi.Coder {
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
  static URN = "beam:coder:nullable:v1";
  type = "nullablecoder";

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
  static URN = "beam:coder:interval_window:v1";
  static INSTANCE: IntervalWindowCoder = new IntervalWindowCoder();

  encode(value: IntervalWindow, writer: Writer, context: Context) {
    InstantCoder.INSTANCE.encode(value.end, writer, context);
    writer.int64(value.end.sub(value.start));
  }

  decode(reader: Reader, context: Context) {
    const end = InstantCoder.INSTANCE.decode(reader, context);
    const duration = <Long>reader.int64();
    return new IntervalWindow(end.sub(duration), end);
  }

  toProto(): runnerApi.Coder {
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

requireForSerialization(
  `${packageName}/coders/standard_coders`,
  exports as Record<string, unknown>,
);

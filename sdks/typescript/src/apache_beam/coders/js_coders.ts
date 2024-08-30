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

import * as BSON from "bson";
import { Writer, Reader } from "protobufjs";
import { Coder, Context, ProtoContext, globalRegistry } from "./coders";
import {
  BoolCoder,
  DoubleCoder,
  StrUtf8Coder,
  VarIntCoder,
} from "./standard_coders";
import { IterableCoder } from "./required_coders";
import * as runnerApi from "../proto/beam_runner_api";
import { requireForSerialization } from "../serialization";
import { packageName } from "../utils/packageJson";

/**
 * A Coder<T> that encodes a javascript object with BSON.
 */
export class BsonObjectCoder<T> implements Coder<T> {
  static URN = "beam:coder:bsonjs:v1";

  encode(element: T, writer: Writer, context: Context) {
    const buff = BSON.serialize(element);
    writer.bytes(buff);
  }

  decode(reader: Reader, context: Context): T {
    const encoded = reader.bytes();
    return BSON.deserialize(encoded) as T;
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: BsonObjectCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}
globalRegistry().register(BsonObjectCoder.URN, BsonObjectCoder);

class NumberOrFloatCoder implements Coder<number> {
  static URN = "beam:coder:numbersforjs:v1";
  intCoder: Coder<number> = new VarIntCoder();
  doubleCoder: Coder<number> = new DoubleCoder();

  encode(element: number, writer: Writer, context: Context) {
    if (element % 1) {
      writer.string("f");
      this.doubleCoder.encode(element, writer, context);
    } else {
      writer.string("i");
      this.intCoder.encode(element, writer, context);
    }
  }

  decode(reader: Reader, context: Context): number {
    const typeMarker = reader.string();
    if (typeMarker === "f") {
      return this.doubleCoder.decode(reader, context);
    } else {
      return this.intCoder.decode(reader, context);
    }
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: NumberOrFloatCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}

/**
 * A Coder<T> that encodes common javascript types such as strings, numbers,
 * nulls, or objects.
 */
export class GeneralObjectCoder<T> implements Coder<T> {
  static URN = "beam:coder:genericobjectjs:v1";
  codersByType = {
    string: new StrUtf8Coder(),
    number: new NumberOrFloatCoder(),
    object: new BsonObjectCoder(),
    boolean: new BoolCoder(),
    array: new IterableCoder(this),
  };

  // This is a map of type names to type markers. It maps a type name to its
  // marker within a stream.
  typeMarkers = {
    string: "S",
    number: "N",
    object: "O",
    boolean: "B",
    array: "A",
  };

  // This is a map of type markers to type names. It maps a type marker to its
  // type name.
  markerToTypes = {
    S: "string",
    N: "number",
    O: "object",
    B: "boolean",
    A: "array",
  };

  encode(element: T, writer: Writer, context: Context) {
    if (element === null || element === undefined) {
      // typeof is "object" but BSON can't handle it.
      writer.string("Z");
    } else {
      const type = Array.isArray(element) ? "array" : typeof element;
      // TODO: Perf. Write a single byte (no need for the length prefix).
      writer.string(this.typeMarkers[type]);
      this.codersByType[type].encode(element, writer, context);
    }
  }

  decode(reader: Reader, context: Context): T {
    const typeMarker = reader.string();
    if (typeMarker === "Z") {
      return null!;
    } else {
      const type = this.markerToTypes[typeMarker];
      return this.codersByType[type].decode(reader, context);
    }
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: GeneralObjectCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}
globalRegistry().register(GeneralObjectCoder.URN, GeneralObjectCoder);

requireForSerialization(`${packageName}/coders/js_coders`, exports);
requireForSerialization(`${packageName}/coders/js_coders`, {
  NumberOrFloatCoder: NumberOrFloatCoder,
});

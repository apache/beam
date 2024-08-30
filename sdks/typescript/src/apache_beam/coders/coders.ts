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

import { Writer, Reader } from "protobufjs";
import * as runnerApi from "../proto/beam_runner_api";

export interface ProtoContext {
  getCoderId(coder: Coder<unknown>): string;
}

interface Class<T> {
  new (...args: unknown[]): T;
}

/**
 * A registry of coder classes.
 *
 * Coder classes are registered by their URN. Most coders have a URN of the type `"beam:coder:{coder_name}:v1"`,
 * and they can register a constructor that takes zero or more parameters as input.
 * Constructor input parameters for a coder are usually internal coders that represent sub-components
 * of the enclosing coder (e.g. a coder of key-value pairs (a.k.a. `KVCoder`) may receive the coder
 * for the key and the coder for the value as parameters).
 */
class CoderRegistry {
  internal_registry: Record<string, (...args: unknown[]) => Coder<unknown>> =
    {};

  getCoder(
    urn: string,
    payload: Uint8Array | undefined = undefined,
    ...components: Coder<unknown>[]
  ) {
    const constructor: (...args) => Coder<unknown> =
      this.internal_registry[urn];

    if (constructor === undefined) {
      throw new Error("Could not find coder for URN " + urn);
    }
    if (payload && payload.length > 0) {
      return constructor(payload, ...components);
    } else {
      return constructor(...components);
    }
  }

  // TODO: Figure out how to branch on constructors (called with new) and
  // ordinary functions.
  register(urn: string, coderClass: Class<Coder<unknown>>) {
    this.registerClass(urn, coderClass);
  }

  registerClass(urn: string, coderClass: Class<Coder<unknown>>) {
    this.registerConstructor(urn, (...args) => new coderClass(...args));
  }

  registerConstructor(
    urn: string,
    constructor: (...args: unknown[]) => Coder<unknown>,
  ) {
    this.internal_registry[urn] = constructor;
  }
}

const CODER_REGISTRY = new CoderRegistry();
export function globalRegistry(): CoderRegistry {
  return CODER_REGISTRY;
}

/**
 * The context for encoding a PCollection element.
 * For example, for strings of utf8 characters or bytes, `wholeStream` encoding means
 * that the string will be encoded as-is; while `needsDelimiter` encoding means that the
 * string will be encoded prefixed with its length.
 *
 * ```js
 * coder = new StrUtf8Coder()
 * w1 = new Writer()
 * coder.encode("my string", w, Context.wholeStream)
 * console.log(w1.finish())  // <= Prints the pure byte-encoding of the string
 * w2 = new Writer()
 * coder.encode("my string", w, Context.needsDelimiters)
 * console.log(w2.finish())  // <= Prints a length-prefix string of bytes
 * ```
 */
export enum Context {
  /**
   * Whole stream encoding/decoding means that the encoding/decoding function does not need to worry about
   * delimiting the start and end of the current element in the stream of bytes.
   */
  wholeStream = "wholeStream",
  /**
   * Needs-delimiters encoding means that the encoding of data must be such that when decoding,
   * the coder is able to stop decoding data at the end of the current element.
   */
  needsDelimiters = "needsDelimiters",
}

/**
 * This is the base interface for coders, which are responsible in Apache Beam to encode and decode
 * elements of a PCollection.
 */
export interface Coder<T> {
  /**
   * Encode an element into a stream of bytes.
   * @param element - an element within a PCollection
   * @param writer - a writer that interfaces the coder with the output byte stream.
   * @param context - the context within which the element should be encoded.
   */
  encode(element: T, writer: Writer, context: Context);

  /**
   * Decode an element from an incoming stream of bytes.
   * @param reader - a reader that interfaces the coder with the input byte stream
   * @param context - the context within which the element should be encoded
   */
  decode(reader: Reader, context: Context): T;

  /**
   * Convert this coder into its protocol buffer representation for the Runner API.
   * A coder in protobuf format can be shared with other components such as Beam runners,
   * SDK workers; and reconstructed into its runtime representation if necessary.
   * @param pipelineContext - a context that holds relevant pipeline attributes such as other coders already in the pipeline.
   */
  toProto(pipelineContext: ProtoContext): runnerApi.Coder;
}

function writeByteCallback(
  val: number,
  buf: { [x: string]: number },
  pos: number,
) {
  buf[pos] = val & 0xff;
}

/** @internal */
export interface HackedWriter extends Writer {
  _push?(...args: unknown[]);
}

/**
 * Write a single byte, as an unsigned integer, directly to the writer.
 */
export function writeRawByte(b: unknown, writer: HackedWriter) {
  writer._push?.(writeByteCallback, 1, b);
}

function writeBytesCallback(
  val: number[],
  buf: { [x: string]: number },
  pos: number,
) {
  for (let i = 0; i < val.length; ++i) {
    buf[pos + i] = val[i];
  }
}

/**
 * Writes a sequence of bytes, as unsigned integers, directly to the writer,
 * without a prefixing with the length of the bytes that writer.bytes() does.
 */
export function writeRawBytes(value: Uint8Array, writer: HackedWriter) {
  writer._push?.(writeBytesCallback, value.length, value);
}

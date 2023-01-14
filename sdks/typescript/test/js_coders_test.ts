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

import * as beam from "../src/apache_beam";
import * as assert from "assert";
import { Context } from "../src/apache_beam/coders/coders";
import {
  BytesCoder,
  IterableCoder,
  KVCoder,
  StrUtf8Coder,
  VarIntCoder,
} from "../src/apache_beam/coders/standard_coders";
import {
  BsonObjectCoder,
  GeneralObjectCoder,
} from "../src/apache_beam/coders/js_coders";
import { BufferReader, BufferWriter } from "protobufjs";

describe("JavaScript native coders", function () {
  describe("BSON Object Coder", function () {
    const bsonCoder = new BsonObjectCoder();
    it("encodes and decodes an object properly", function () {
      const inputObject = {
        str: "astring",
        int: 1,
        float: 1.2345,
        obj: { any: "any" },
        null: null,
        bool: true,
        // 'undef': undefined,  // TODO(pabloem): Figure out how to support undefined encoding/decoding.
        bigint: Number.MAX_SAFE_INTEGER + 100,
      };
      const writer = new BufferWriter();

      const encoded = bsonCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        bsonCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
  });

  describe("general object coder", function () {
    const objCoder = new GeneralObjectCoder();
    it("encodes and decodes an object properly", function () {
      const inputObject = {
        str: "astring",
        int: 1,
        float: 1.2345,
        obj: { any: "any" },
        null: null,
        bool: true,
        array: [1, 2, 3],
        // 'undef': undefined,  // TODO(pabloem): Figure out how to support undefined encoding/decoding.
        bigint: Number.MAX_SAFE_INTEGER + 100,
      };
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it("encodes and decodes a string properly", function () {
      const inputObject = "abcde";
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.equal(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it("encodes and decodes an integer properly", function () {
      const inputObject = 12345678;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it("encodes and decodes a float properly", function () {
      const inputObject = 0.12345678;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it("encodes and decodes a BigInt properly", function () {
      // TODO(pabloem): THIS TEST NEEDS TO BE sKIPPED BECAUSE VERY LARGE INTS ARE BROKEN
      this.skip();
      const inputObject = Number.MAX_SAFE_INTEGER + 123456789;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it("encodes and decodes a true boolean properly", function () {
      const inputObject = true;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.equal(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it("encodes and decodes a false boolean properly", function () {
      const inputObject = false;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.equal(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
  });
});

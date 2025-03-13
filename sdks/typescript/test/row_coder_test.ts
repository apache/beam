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
import { RowCoder } from "../src/apache_beam/coders/row_coder";
import { BufferReader, BufferWriter } from "protobufjs";

function removeUndefined(obj) {
  for (const prop in obj) {
    if (obj[prop] === undefined) {
      delete obj[prop];
    }
  }
  return obj;
}

describe("encodes and decodes with RowCoder", function () {
  const rowCoder = new RowCoder(
    RowCoder.inferSchemaOfJSON({
      str_field: "str",
      int_field: 4,
      //float_field: 2.7,
      repeated_field: [1, 2, 3],
      nested_field: { a: 1, b: 2 },
    })
  );

  const cases = {
    full: {
      str_field: "Aaaaaaaaaaaaaa",
      int_field: 4,
      //float_field: 2.7,
      repeated_field: [1, 2, 3],
      nested_field: { a: 1, b: 2 },
    },
    empty: {},
    half: { str_field: "Bbbbbbbbbb", nested_field: { a: 123, b: 321 } },
    str_only: { str_field: "Abc" },
    int_only: { int_field: 49 },
    repated_only: { repeated_field: [1, 4, 9, 16] },
    nested_only: { nested_field: { a: 100, b: -100 } },
  };

  for (let descr in cases) {
    it("encodes and decodes " + descr, function () {
      const writer = new BufferWriter();
      const inputObject = cases[descr];

      const encoded = rowCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        removeUndefined(rowCoder.decode(reader, Context.needsDelimiters)),
        inputObject
      );
    });
  }
});

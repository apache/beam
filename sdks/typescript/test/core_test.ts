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
import { BytesCoder } from "../src/apache_beam/coders/standard_coders";
import { Pipeline } from "../src/apache_beam/internal/pipeline";
// TODO(pabloem): Fix installation.

describe("core module", function () {
  describe("runs a basic impulse expansion", function () {
    it("runs a basic Impulse expansion", function () {
      var p = new Pipeline();
      var res = new beam.Root(p).apply(new beam.Impulse());

      assert.equal(res.type, "pcollection");
      assert.deepEqual(p.context.getPCollectionCoder(res), new BytesCoder());
    });
    it("runs a ParDo expansion", function () {
      var p = new Pipeline();
      var res = new beam.Root(p)
        .apply(new beam.Impulse())
        .map(function (v: any) {
          return v * 2;
        });
    });
    it("runs a GroupBy expansion", function () {});
  });
});

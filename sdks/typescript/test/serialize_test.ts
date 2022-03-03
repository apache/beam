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

import { expect } from "chai";
import {
  deserialize,
  serialize,
  BuiltinList,
  generateDefaultBuiltins,
} from "serialize-closures";

describe("serialization tests", function () {
  function roundtrip(value, builtins?: BuiltinList) {
    return deserialize(
      JSON.parse(JSON.stringify(serialize(value, builtins))),
      builtins
    );
  }

  function expectRoundtrip(value, builtins?: BuiltinList) {
    expect(roundtrip(value, builtins)).to.deep.equal(value);
  }

  function* myGenerator() {
    yield 42;
    yield 84;
  }

  function simpleGenerator() {
    expect(myGenerator().next()).to.equal(42);
  }

  function roundtripGeneratorConstructor() {
    expect(roundtrip(myGenerator)().next()).to.equal(42);
  }

  function roundtripGeneratorInProgress() {
    const gen = myGenerator();
    expect(gen.next()).to.equal(42);
    expect(roundtrip(gen).next()).to.equal(84);
  }

  it("serializes and deserializes function() defined functions", async function () {
    // function that returns a simple generator
    const fn1 = function (a: any): Function {
      const fn = function* (a: string): any {
        var words = a.split(/[^a-z]+/);
        for (var i = 0; i < words.length; i++) {
          yield words[i];
        }
      };
      return fn;
    };
    var foo = roundtrip(fn1)("a");
    var bar = fn1("a");
    var foo_itr = foo("kerry is great");
    var bar_itr = bar("kerry is great");

    expect(foo_itr.next()).to.deep.equal(bar_itr.next());
  });

  // it("serializes and deserializes arrow functions", async function() {
  //
  //  });
});

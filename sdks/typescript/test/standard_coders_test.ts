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

import {
  Coder,
  Context,
  globalRegistry,
} from "../src/apache_beam/coders/coders";
import { Writer, Reader } from "protobufjs";
import Long from "long";

import * as assertions from "assert";
import * as yaml from "js-yaml";
import * as fs from "fs";
import * as util from "util";
import { GlobalWindow, Timing } from "../src/apache_beam/values";
import { IterableCoder } from "../src/apache_beam/coders/required_coders";

const STANDARD_CODERS_FILE =
  "../../model/fn-execution/src/main/resources/org/apache/beam/model/fnexecution/v1/standard_coders.yaml";

interface CoderRepr {
  urn: string;
  payload?: Uint8Array;
  components?: Array<CoderRepr>;
  non_deterministic?: boolean;
}

type CoderSpec = {
  coder: CoderRepr;
  nested?: boolean;
  examples: object;
};

// TODO(pabloem): Empty this list.
const UNSUPPORTED_CODERS = [
  "beam:coder:timer:v1",
  "beam:coder:param_windowed_value:v1",
  "beam:coder:sharded_key:v1",
  "beam:coder:custom_window:v1",
  "beam:coder:state_backed_iterable:v1",
  // Tested elsewhere (though it'd still be good to enable).
  "beam:coder:row:v1",
];

const UNSUPPORTED_EXAMPLES = {
  // Floating point truncation when parsing json definition.
  "beam:coder:interval_window:v1": ["8020c49ba5e353f700"],
};

const _urn_to_json_value_parser = {
  "beam:coder:bytes:v1": (_) => (x) => new TextEncoder().encode(x),
  "beam:coder:bool:v1": (_) => (x) => x,
  "beam:coder:string_utf8:v1": (_) => (x) => x as string,
  "beam:coder:varint:v1": (_) => (x) => x,
  "beam:coder:double:v1": (_) => (x) => x === "NaN" ? NaN : x,
  "beam:coder:kv:v1": (components) => (x) => ({
    key: components[0](x["key"]),
    value: components[1](x["value"]),
  }),
  "beam:coder:iterable:v1": (components) => (x) =>
    x.map((elm) => components[0](elm)),
  "beam:coder:global_window:v1": (_) => (x) => new GlobalWindow(),
  "beam:coder:interval_window:v1":
    (_) => (x: { end: number; span: number }) => ({
      start: Long.fromNumber(x.end).sub(x.span),
      end: Long.fromNumber(x.end),
    }),
  "beam:coder:windowed_value:v1": (components) => (x) => ({
    value: components[0](x.value),
    windows: x.windows.map(components[1]),
    pane: {
      isLast: x.pane.is_last,
      isFirst: x.pane.is_first,
      index: x.pane.index,
      onTimeIndex: x.pane.on_time_index,
      timing: Timing[x.pane.timing],
    },
    timestamp: Long.fromNumber(x.timestamp),
  }),
  "beam:coder:nullable:v1": (components) => (x) => {
    return x === null || x === undefined ? undefined : components[0](x);
  },
};

function get_json_value_parser(coderRepr: CoderRepr) {
  // TODO(pabloem): support 'beam:coder:row:v1' coder.
  var value_parser_factory = _urn_to_json_value_parser[coderRepr.urn];

  if (value_parser_factory === undefined) {
    throw new Error(
      util.format("Do not know how to parse example values for %s", coderRepr)
    );
  }

  const components = coderRepr.components || [];
  const componentParsers = components.map((componentRepr) =>
    get_json_value_parser(componentRepr)
  );
  return value_parser_factory(componentParsers);
}

describe("standard Beam coders on Javascript", function () {
  const docs: Array<CoderSpec> = yaml.loadAll(
    fs.readFileSync(STANDARD_CODERS_FILE, "utf8")
  );
  docs.forEach((doc) => {
    const urn = doc.coder.urn;
    if (UNSUPPORTED_CODERS.includes(urn)) {
      return;
    }

    // The YAML is designed so doc.nested is three-valued, and undefined means "test both variations"
    var contexts: Context[] = [];
    if (doc.nested !== true) {
      contexts.push(Context.wholeStream);
    }
    if (doc.nested !== false) {
      contexts.push(Context.needsDelimiters);
    }

    contexts.forEach((context) => {
      describe("in Context " + context, function () {
        const spec = doc;
        var components;
        if (spec.coder.components) {
          components = spec.coder.components.map(
            // Second level coders have neither payloads nor components.
            (c) => globalRegistry().getCoder(c.urn)
          );
        } else {
          components = [];
        }
        const coder = globalRegistry().getCoder(urn, undefined, ...components);
        runCoderTest(coder, urn, context, spec);
      });
    });
  });
});

function runCoderTest<T>(coder: Coder<T>, urn, context, spec: CoderSpec) {
  describe(
    util.format(
      "coder %s (%s)",
      util.inspect(coder, { colors: true, breakLength: Infinity }),
      spec.coder.non_deterministic ? "nondeterministic" : "deterministic"
    ),
    function () {
      const parser = get_json_value_parser(spec.coder);
      for (let expected in spec.examples) {
        var value = parser(spec.examples[expected]);
        const expectedEncoded = Buffer.from(expected, "binary");
        if (
          (UNSUPPORTED_EXAMPLES[spec.coder.urn] || []).includes(
            expectedEncoded.toString("hex")
          )
        ) {
          continue;
        }
        coderCase(
          coder,
          value,
          expectedEncoded,
          context,
          spec.coder.non_deterministic || false
        );
      }
    }
  );
}

function coderCase<T>(
  coder: Coder<T>,
  obj,
  expectedEncoded: Uint8Array,
  context,
  non_deterministic
) {
  let encodeObj;
  // Normally we would not support non-deterministic cases, but in the
  // implementation that we have, the non-deterministic encodings happen
  // to match our samples.
  if (non_deterministic && coder instanceof IterableCoder) {
    // We support the particular case of iterables of unknown length.
    // The encoding here is not deterministic, but the test case works
    // fine.
    let typedIterable: Iterable<any> = (function* it() {
      for (let elm in obj) {
        yield obj[elm];
      }
    })();
    encodeObj = typedIterable;
  } else {
    encodeObj = obj;
  }
  it(
    util.format(
      "encodes %s to %s",
      util.inspect(encodeObj, { colors: true, depth: Infinity }),
      Buffer.from(expectedEncoded).toString("hex")
    ),
    function () {
      var writer = new Writer();
      coder.encode(encodeObj, writer, context);
      assertions.deepEqual(writer.finish(), expectedEncoded);
    }
  );

  it(
    util.format(
      "decodes %s to %s correctly",
      Buffer.from(expectedEncoded).toString("hex"),
      util.inspect(obj, { colors: true, depth: Infinity })
    ),
    function () {
      const decoded = coder.decode(new Reader(expectedEncoded), context);
      assertions.deepEqual(decoded, obj);
    }
  );
}

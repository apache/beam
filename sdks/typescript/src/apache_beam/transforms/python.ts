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

/**
 * Utilities for invoking Python transforms on PCollections.
 *
 * See also https://beam.apache.org/documentation/programming-guide/#1324-using-cross-language-transforms-in-a-typescript-pipeline
 *
 * @packageDocumentation
 */

import * as beam from "../../apache_beam";
import { StrUtf8Coder } from "../coders/standard_coders";
import * as external from "../transforms/external";
import { PythonService } from "../utils/service";
import * as row_coder from "../coders/row_coder";

/**
 * Returns a PTransform applying a Python transform (typically identified by
 * fully qualified name) to a PCollection, e.g.
 *
 *```js
 * const input_pcoll = ...
 * const output_pcoll = input_pcoll.apply(
 *     pythonTransform("beam.Map", [pythonCallable("str.upper")]));
 *```
 * See also https://beam.apache.org/documentation/programming-guide/#1324-using-cross-language-transforms-in-a-typescript-pipeline
 */
export function pythonTransform<
  InputT extends beam.PValue<any>,
  OutputT extends beam.PValue<any>,
>(
  constructor: string,
  args_or_kwargs: any[] | { [key: string]: any } | undefined = undefined,
  kwargs: { [key: string]: any } | undefined = undefined,
  options: external.RawExternalTransformOptions = {},
): beam.AsyncPTransform<InputT, OutputT> {
  let args;
  if (args_or_kwargs === undefined) {
    args = [];
  } else if (Array.isArray(args_or_kwargs)) {
    args = args_or_kwargs;
  } else {
    if (kwargs != undefined) {
      throw new Error("Keyword arguments cannot be specified twice.");
    }
    args = [];
    kwargs = args_or_kwargs;
  }

  if (kwargs === undefined) {
    kwargs = {};
  }

  return external.rawExternalTransform<InputT, OutputT>(
    "beam:transforms:python:fully_qualified_named",
    {
      constructor: constructor,
      args: Object.fromEntries(args.map((arg, ix) => ["arg" + ix, arg])),
      kwargs,
    },
    //     "localhost:4444"
    async () =>
      PythonService.forModule(
        "apache_beam.runners.portability.expansion_service_main",
        ["--fully_qualified_name_glob=*", "--port", "{{PORT}}"],
      ),
    options,
  );
}

/**
 * A type representing a Python callable as a string.
 *
 * Supported formats include fully-qualified names such as `math.sin`,
 * expressions such as `lambda x: x * x` or `str.upper`, and multi-line function
 * definitions such as `def foo(x): ...` or class definitions like
 * `class Foo(...): ...`. If the source string contains multiple lines then lines
 * prior to the last will be evaluated to provide the context in which to
 * evaluate the expression, for example::
 *```py
 *    import math
 *
 *    lambda x: x - math.sin(x)
 *```
 * is a valid chunk of source code.
 */
export function pythonCallable(expr: string) {
  return { expr, beamLogicalType: "beam:logical_type:python_callable:v1" };
}

row_coder.registerLogicalType({
  urn: "beam:logical_type:python_callable:v1",
  reprType: row_coder.RowCoder.inferTypeFromJSON("string", false),
  toRepr: (pc) => pc.expr,
  fromRepr: (expr) => pythonCallable(expr),
});

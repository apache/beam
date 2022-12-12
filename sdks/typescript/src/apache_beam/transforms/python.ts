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

import * as beam from "../../apache_beam";
import { StrUtf8Coder } from "../coders/standard_coders";
import * as external from "../transforms/external";
import { PythonService } from "../utils/service";
import * as row_coder from "../coders/row_coder";

export function pythonTransform<
  InputT extends beam.PValue<any>,
  OutputT extends beam.PValue<any>
>(
  constructor: string,
  args_or_kwargs: any[] | { [key: string]: any } | undefined = undefined,
  kwargs: { [key: string]: any } | undefined = undefined,
  options: external.RawExternalTransformOptions = {}
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
        ["--fully_qualified_name_glob=*", "--port", "{{PORT}}"]
      ),
    options
  );
}

export function pythonCallable(expr: string) {
  return { expr, beamLogicalType: "beam:logical_type:python_callable:v1" };
}

row_coder.registerLogicalType({
  urn: "beam:logical_type:python_callable:v1",
  reprType: row_coder.RowCoder.inferTypeFromJSON("string", false),
  toRepr: (pc) => pc.expr,
  fromRepr: (expr) => pythonCallable(expr),
});

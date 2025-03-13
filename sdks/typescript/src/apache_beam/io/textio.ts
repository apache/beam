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
import { withCoderInternal } from "../transforms/internal";
import { pythonTransform } from "../transforms/python";
import { PythonService } from "../utils/service";
import { camelToSnakeOptions } from "../utils/utils";
import { Schema } from "../proto/schema";
import { RowCoder } from "../coders/row_coder";

export function readFromText(
  filePattern: string,
): beam.AsyncPTransform<beam.Root, beam.PCollection<string>> {
  return async function readFromText(root: beam.Root) {
    return root.applyAsync(
      pythonTransform<beam.Root, beam.PCollection<string>>(
        "apache_beam.io.ReadFromText",
        {
          file_pattern: filePattern,
        },
      ),
    );
  };
}

export function writeToText(
  filePathPrefix,
  options: {
    fileNameSuffix?: string;
    appendTrailingMewlines?: boolean;
    numShards?: number;
    shardNameTemplate?: string;
    header?: string;
    footer?: string;
  } = {},
): (
  toWrite: beam.PCollection<string>,
) => Promise<{ filesWritten: beam.PCollection<string> }> {
  return async function writeToText(pcoll: beam.PCollection<string>) {
    return {
      filesWritten: await pcoll
        .map((e) => (typeof e == "string" ? e : "" + e))
        .apply(withCoderInternal(new StrUtf8Coder()))
        .applyAsync(
          pythonTransform("apache_beam.io.WriteToText", {
            file_path_prefix: filePathPrefix,
            ...camelToSnakeOptions(options),
          }),
        ),
    };
  };
}

export function readFromCsv(
  filePattern: string,
  options: {} = {},
): (root: beam.Root) => Promise<beam.PCollection<any>> {
  return async function readFromCsv(root: beam.Root) {
    return root.applyAsync(
      pythonTransform("apache_beam.dataframe.io.ReadViaPandas", {
        path: filePattern,
        format: "csv",
        ...camelToSnakeOptions(options),
      }),
    );
  };
}

export function writeToCsv(
  filePathPrefix: string,
  schema: Schema | undefined = undefined,
  options: {} = {},
): (
  toWrite: beam.PCollection<Object>,
) => Promise<{ filesWritten: beam.PCollection<string> }> {
  return async function writeToCsv(toWrite: beam.PCollection<Object>) {
    if (schema != undefined) {
      toWrite = toWrite.apply(withCoderInternal(RowCoder.fromSchema(schema)));
    }
    return {
      filesWritten: await toWrite.applyAsync(
        pythonTransform("apache_beam.dataframe.io.WriteViaPandas", {
          path: filePathPrefix,
          format: "csv",
          index: false,
          ...camelToSnakeOptions(options),
        }),
      ),
    };
  };
}

export function readFromJson(
  filePattern: string,
  options: {} = {},
): (root: beam.Root) => Promise<beam.PCollection<any>> {
  return async function readFromJson(root: beam.Root) {
    return root.applyAsync(
      pythonTransform("apache_beam.dataframe.io.ReadViaPandas", {
        path: filePattern,
        format: "json",
        orient: "records",
        lines: true,
        ...camelToSnakeOptions(options),
      }),
    );
  };
}

export function writeToJson(
  filePathPrefix: string,
  schema: Schema | undefined = undefined,
  options: {} = {},
): (
  toWrite: beam.PCollection<Object>,
) => Promise<{ filesWritten: beam.PCollection<string> }> {
  return async function writeToJson(toWrite: beam.PCollection<Object>) {
    if (schema != undefined) {
      toWrite = toWrite.apply(withCoderInternal(RowCoder.fromSchema(schema)));
    }
    return {
      filesWritten: await toWrite.applyAsync(
        pythonTransform("apache_beam.dataframe.io.WriteViaPandas", {
          path: filePathPrefix,
          format: "json",
          orient: "records",
          lines: true,
          ...camelToSnakeOptions(options),
        }),
      ),
    };
  };
}

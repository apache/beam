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
import { RowCoder } from "../coders/row_coder";
import { schemaio } from "./schemaio";
import { Schema } from "../proto/schema";

// TODO: Read and write should use a different schema for v2.
const bigqueryIOConfigSchema = RowCoder.inferSchemaOfJSON({
  table: "string",
  query: "string",
  queryLocation: "string",
  createDisposition: "string",
});

export function readFromBigQuery<T>(
  options:
    | { table: string; schema?: Schema }
    | { query: string; schema?: Schema },
): beam.AsyncPTransform<beam.Root, beam.PCollection<T>> {
  return schemaio<beam.Root, beam.PCollection<T>>(
    "readFromBigQuery",
    "beam:transform:org.apache.beam:schemaio_bigquery_read:v1",
    options,
    bigqueryIOConfigSchema,
  );
}

export function writeToBigQuery<T>(
  table: string,
  options: { createDisposition?: "Never" | "IfNeeded" } = {},
): beam.AsyncPTransform<beam.Root, beam.PCollection<T>> {
  if (options.createDisposition == undefined) {
    options.createDisposition = "IfNeeded";
  }
  return schemaio<beam.Root, beam.PCollection<T>>(
    "writeToBigquery",
    "beam:transform:org.apache.beam:schemaio_bigquery_write:v1",
    { table, createDisposition: options.createDisposition },
    bigqueryIOConfigSchema,
  );
}

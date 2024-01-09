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
import { withCoderInternal } from "../transforms/internal";

export function readFromAvro<T>(
  filePattern: string,
  // TODO: Allow schema to be inferred.
  options: { schema: Schema },
): beam.AsyncPTransform<beam.Root, beam.PCollection<T>> {
  return schemaio<beam.Root, beam.PCollection<T>>(
    "readFromTable",
    "beam:transform:org.apache.beam:schemaio_avro_read:v1",
    { location: filePattern, schema: options.schema },
  );
}

export function writeToAvro<T>(filePath: string, options: { schema: Schema }) {
  return async function writeToAvro(
    pcoll: beam.PCollection<Object>,
  ): Promise<{}> {
    // TODO: Allow schema to be inferred.
    if (options.schema) {
      pcoll = pcoll.apply(
        withCoderInternal(RowCoder.fromSchema(options.schema)),
      );
    }
    return pcoll.applyAsync(
      schemaio<beam.PCollection<T>, {}>(
        "writeToAvro",
        "beam:transform:org.apache.beam:schemaio_avro_write:v1",
        { location: filePath, schema: options.schema },
      ),
    );
  };
}

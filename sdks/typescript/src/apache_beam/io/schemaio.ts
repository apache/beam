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
import * as external from "../transforms/external";
import { Schema } from "../proto/schema";
import { RowCoder } from "../coders/row_coder";
import { serviceProviderFromJavaGradleTarget } from "../utils/service";
import * as protobufjs from "protobufjs";

export function schemaio<
  InputT extends beam.PValue<any>,
  OutputT extends beam.PValue<any>,
>(
  name,
  urn,
  config,
  configSchema: Schema | undefined = undefined,
): beam.AsyncPTransform<InputT, OutputT> {
  // Location is separate for historical reasons.
  let maybeLocation: { location?: string } = {};
  if (config.location) {
    maybeLocation.location = config.location;
    delete config.location;
  }
  // Similarly for Schema, which is passed as bytes.
  let maybeSchema: { dataSchema?: Uint8Array } = {};
  if (config.schema) {
    maybeSchema.dataSchema = Schema.toBinary(config.schema);
    delete config.schema;
  }
  // The config is encoded and passed as bytes.
  const writer = new protobufjs.Writer();
  if (configSchema === undefined) {
    configSchema = RowCoder.inferSchemaOfJSON(config);
  }
  new RowCoder(configSchema!).encode(config, writer, null!);
  const encodedConfig = writer.finish();

  return beam.withName(
    name,
    external.rawExternalTransform<InputT, OutputT>(
      urn,
      { config: encodedConfig, ...maybeLocation, ...maybeSchema },
      serviceProviderFromJavaGradleTarget(
        "sdks:java:io:google-cloud-platform:expansion-service:shadowJar",
      ),
    ),
  );
}

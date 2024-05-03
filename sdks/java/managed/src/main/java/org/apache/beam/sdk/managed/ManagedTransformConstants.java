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
package org.apache.beam.sdk.managed;

import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** This class contains constants for supported managed transform identifiers. */
public class ManagedTransformConstants {
  public static final String ICEBERG_READ = "beam:schematransform:org.apache.beam:iceberg_read:v1";
  public static final String ICEBERG_WRITE =
      "beam:schematransform:org.apache.beam:iceberg_write:v1";
  public static final String KAFKA_READ = "beam:schematransform:org.apache.beam:kafka_read:v1";

  public static final Map<String, String> KAFKA_READ_MAPPINGS =
      ImmutableMap.<String, String>builder()
          .put("topic", "topic")
          .put("bootstrap_servers", "bootstrapServers")
          .put("consumer_config_updates", "consumerConfigUpdates")
          .put("confluent_schema_registry_url", "confluentSchemaRegistryUrl")
          .put("confluent_schema_registry_subject", "confluentSchemaRegistrySubject")
          .put("data_format", "format")
          .put("schema", "schema")
          .put("file_descriptor_path", "fileDescriptorPath")
          .put("message_name", "messageName")
          .build();

  // Configuration parameter names exposed via the Managed interface may differ from the parameter
  // names in the
  // actual SchemaTransform implementation.
  // Any naming differences should be laid out here so that we can remap the keys before building
  // the transform
  public static final Map<String, Map<String, String>> MAPPINGS =
      ImmutableMap.of(KAFKA_READ, KAFKA_READ_MAPPINGS);
}

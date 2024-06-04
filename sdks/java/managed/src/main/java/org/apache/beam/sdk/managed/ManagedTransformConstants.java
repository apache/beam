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

/**
 * This class contains constants for supported managed transforms, including:
 *
 * <ul>
 *   <li>Identifiers of supported transforms
 *   <li>Configuration parameter renaming
 * </ul>
 *
 * <p>Configuration parameter names exposed via Managed interface may differ from the parameter
 * names in the underlying SchemaTransform implementation.
 *
 * <p>Any naming differences are laid out in {@link ManagedTransformConstants#MAPPINGS} to update
 * the configuration object before it's used to build the underlying transform.
 *
 * <p>Mappings don't need to include ALL underlying parameter names, as we may not want to expose
 * every single parameter through the Managed interface.
 */
public class ManagedTransformConstants {
  // Standard input PCollection tag
  public static final String INPUT = "input";

  public static final String ICEBERG_READ = "beam:schematransform:org.apache.beam:iceberg_read:v1";
  public static final String ICEBERG_WRITE =
      "beam:schematransform:org.apache.beam:iceberg_write:v1";
  public static final String KAFKA_READ = "beam:schematransform:org.apache.beam:kafka_read:v1";
  public static final String KAFKA_WRITE = "beam:schematransform:org.apache.beam:kafka_write:v1";

  private static final Map<String, String> KAFKA_READ_MAPPINGS =
      ImmutableMap.<String, String>builder().put("data_format", "format").build();

  private static final Map<String, String> KAFKA_WRITE_MAPPINGS =
      ImmutableMap.<String, String>builder().put("data_format", "format").build();

  public static final Map<String, Map<String, String>> MAPPINGS =
      ImmutableMap.<String, Map<String, String>>builder()
          .put(KAFKA_READ, KAFKA_READ_MAPPINGS)
          .put(KAFKA_WRITE, KAFKA_WRITE_MAPPINGS)
          .build();
}

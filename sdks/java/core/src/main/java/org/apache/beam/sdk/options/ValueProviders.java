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
package org.apache.beam.sdk.options;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Map;

/** Utilities for working with the {@link ValueProvider} interface. */
public class ValueProviders {
  private ValueProviders() {}

  /**
   * Given {@code serializedOptions} as a JSON-serialized {@link PipelineOptions}, updates the
   * values according to the provided values in {@code runtimeValues}.
   *
   * @deprecated Use {@link org.apache.beam.sdk.testing.TestPipeline#newProvider} for testing {@link
   *     ValueProvider} code.
   */
  @Deprecated
  public static String updateSerializedOptions(
      String serializedOptions, Map<String, String> runtimeValues) {
    ObjectNode root, options;
    try {
      root = PipelineOptionsFactory.MAPPER.readValue(serializedOptions, ObjectNode.class);
      options = (ObjectNode) root.get("options");
      checkNotNull(options, "Unable to locate 'options' in %s", serializedOptions);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unable to parse %s", serializedOptions), e);
    }

    for (Map.Entry<String, String> entry : runtimeValues.entrySet()) {
      options.put(entry.getKey(), entry.getValue());
    }
    try {
      return PipelineOptionsFactory.MAPPER.writeValueAsString(root);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse re-serialize options", e);
    }
  }
}

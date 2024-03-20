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
package org.apache.beam.sdk.schemas.transforms;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * Provider to create {@link SchemaTransform} instances for use in Beam SQL and other SDKs.
 *
 * <p><b>Internal only:</b> This interface is actively being worked on and it will likely change as
 * we provide implementations for more standard Beam transforms. We provide no backwards
 * compatibility guarantees and it should not be implemented outside of the Beam repository.
 */
@Internal
public interface SchemaTransformProvider {
  /** Returns an id that uniquely represents this transform. */
  String identifier();

  /**
   * Returns a description regarding the {@link SchemaTransform} represented by the {@link
   * SchemaTransformProvider}. Please keep the language generic (i.e. not specific to any
   * programming language). The description may be markdown formatted.
   */
  default String description() {
    return "";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the transform itself.
   */
  Schema configurationSchema();

  /**
   * Produce a {@link SchemaTransform} from some transform-specific configuration object. Can throw
   * a {@link InvalidConfigurationException} or a {@link InvalidSchemaException}.
   */
  SchemaTransform from(Row configuration);

  /** Returns the input collection names of this transform. */
  default List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  /** Returns the output collection names of this transform. */
  default List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  /**
   * List the dependencies needed for this transform. Jars from classpath are used by default when
   * Optional.empty() is returned.
   */
  default Optional<List<String>> dependencies(Row configuration, PipelineOptions options) {
    return Optional.empty();
  }
}

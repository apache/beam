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

import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.values.Row;

/**
 * Like {@link SchemaTransformProvider} except uses a configuration object instead of Schema and
 * Row.
 *
 * <p>ConfigT should be available in the SchemaRegistry.
 *
 * <p><b>Internal only:</b> This interface is actively being worked on and it will likely change as
 * we provide implementations for more standard Beam transforms. We provide no backwards
 * compatibility guarantees and it should not be implemented outside of the Beam repository.
 */
@Internal
public abstract class TypedSchemaTransformProvider<ConfigT> implements SchemaTransformProvider {

  protected abstract Class<ConfigT> configurationClass();

  /**
   * Produce a SchemaTransform from ConfigT. Can throw a {@link InvalidConfigurationException} or a
   * {@link InvalidSchemaException}.
   */
  protected abstract SchemaTransform from(ConfigT configuration);

  /**
   * List the dependencies needed for this transform. Jars from classpath are used by default when
   * Optional.empty() is returned.
   */
  Optional<List<String>> dependencies(ConfigT configuration, PipelineOptions options) {
    return Optional.empty();
  }

  @Override
  public final Schema configurationSchema() {
    try {
      // Sort the fields by name to ensure a consistent schema is produced
      return SchemaRegistry.createDefault().getSchema(configurationClass()).sorted();
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(
          "Unable to find schema for "
              + identifier()
              + " SchemaTransformProvider's configuration.");
    }
  }

  @Override
  public final SchemaTransform from(Row configuration) {
    return from(configFromRow(configuration));
  }

  @Override
  public final Optional<List<String>> dependencies(Row configuration, PipelineOptions options) {
    return dependencies(configFromRow(configuration), options);
  }

  private ConfigT configFromRow(Row configuration) {
    try {
      return SchemaRegistry.createDefault()
          .getFromRowFunction(configurationClass())
          .apply(configuration);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(
          "Unable to find schema for " + identifier() + "SchemaTransformProvider's config");
    }
  }
}

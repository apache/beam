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

import static org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.InvalidSchemaException;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

/**
 * Like {@link SchemaTransformProvider} except uses a configuration object instead of Schema and
 * Row.
 *
 * <p>ConfigT should be available in the SchemaRegistry.
 *
 * <p>{@link #configurationSchema()} produces a configuration {@link Schema} that is inferred from
 * {@code ConfigT} using the SchemaRegistry. A Beam {@link Row} can still be used to produce a
 * {@link SchemaTransform} using {@link #from(Row)}, as long as the Row fits the configuration
 * Schema.
 *
 * <p>NOTE: The inferred field names in the configuration {@link Schema} and {@link Row} follow the
 * {@code snake_case} naming convention.
 *
 * <p><b>Internal only:</b> This interface is actively being worked on and it will likely change as
 * we provide implementations for more standard Beam transforms. We provide no backwards
 * compatibility guarantees and it should not be implemented outside of the Beam repository.
 */
@Internal
public abstract class TypedSchemaTransformProvider<ConfigT> implements SchemaTransformProvider {

  @SuppressWarnings("unchecked")
  protected Class<ConfigT> configurationClass() {
    @Nullable
    ParameterizedType parameterizedType = (ParameterizedType) getClass().getGenericSuperclass();
    checkStateNotNull(
        parameterizedType, "Could not get the TypedSchemaTransformProvider's parameterized type.");
    checkArgument(
        parameterizedType.getActualTypeArguments().length == 1,
        String.format(
            "Expected one parameterized type, but got %s.",
            parameterizedType.getActualTypeArguments().length));

    return (Class<ConfigT>) parameterizedType.getActualTypeArguments()[0];
  }

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
      // We also establish a `snake_case` convention for all SchemaTransform configurations
      return SchemaRegistry.createDefault().getSchema(configurationClass()).sorted().toSnakeCase();
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(
          "Unable to find schema for "
              + identifier()
              + " SchemaTransformProvider's configuration.");
    }
  }

  /**
   * Produces a {@link SchemaTransform} from a Row configuration. Row fields are expected to have
   * `snake_case` naming convention.
   */
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
      SchemaRegistry registry = SchemaRegistry.createDefault();
      SerializableFunction<Row, ConfigT> rowToConfigT =
          registry.getFromRowFunction(configurationClass());

      // Configuration objects handled by the AutoValueSchema provider will expect Row fields with
      // camelCase naming convention
      SchemaProvider schemaProvider = registry.getSchemaProvider(configurationClass());
      if (schemaProvider.getClass().equals(DefaultSchemaProvider.class)
          && checkNotNull(
                  ((DefaultSchemaProvider) schemaProvider)
                      .getUnderlyingSchemaProvider(configurationClass()))
              .getClass()
              .equals(AutoValueSchema.class)) {
        configuration = configuration.toCamelCase();
      }
      return rowToConfigT.apply(configuration);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(
          "Unable to find schema for " + identifier() + "SchemaTransformProvider's config");
    }
  }
}

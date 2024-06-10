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

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An abstraction representing schema capable and aware transforms. The interface is intended to be
 * used in conjunction with the interface {@link SchemaTransformProvider}.
 *
 * <p>The interfaces can be implemented to make transforms available in other SDKs in addition to
 * Beam SQL.
 *
 * <p><b>Internal only:</b> This interface is actively being worked on and it will likely change as
 * we provide implementations for more standard Beam transforms. We provide no backwards
 * compatibility guarantees and it should not be implemented outside of the Beam repository.
 */
@Internal
public abstract class SchemaTransform extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {
  private @Nullable Row configurationRow;
  private @Nullable String identifier;
  private boolean registered = false;

  /**
   * Stores the transform's identifier and configuration {@link Row} used to build this instance.
   * Doing so allows this transform to be translated from/to proto using {@link
   * org.apache.beam.sdk.util.construction.PTransformTranslation.SchemaTransformTranslator}.
   */
  public SchemaTransform register(Row configurationRow, String identifier) {
    this.configurationRow = configurationRow;
    this.identifier = identifier;
    registered = true;

    return this;
  }

  /**
   * Like {@link #register(Row, String)}, but with a configuration POJO. This is a convenience
   * method for {@link TypedSchemaTransformProvider} implementations.
   */
  public <ConfigT> SchemaTransform register(
      ConfigT configuration, Class<ConfigT> configClass, String identifier) {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    try {
      // Get initial row with values
      // then sort lexicographically and convert to snake_case
      Row configRow =
          registry.getToRowFunction(configClass).apply(configuration).sorted().toSnakeCase();
      return register(configRow, identifier);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(
          String.format(
              "Unable to find schema for this SchemaTransform's config type: %s", configClass),
          e);
    }
  }

  public Row getConfigurationRow() {
    return Preconditions.checkNotNull(
        configurationRow,
        "Could not fetch Row configuration for %s. Please store it using .register().",
        getClass());
  }

  public String getIdentifier() {
    return Preconditions.checkNotNull(
        identifier,
        "Could not fetch identifier for %s. Please store it using .register().",
        getClass());
  }

  public boolean isRegistered() {
    return registered;
  }
}

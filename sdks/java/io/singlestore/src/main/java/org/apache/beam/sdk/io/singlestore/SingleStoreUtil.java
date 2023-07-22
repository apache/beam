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
package org.apache.beam.sdk.io.singlestore;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

/** Provides utility functions for working with {@link SingleStoreIO}. */
final class SingleStoreUtil {
  public static String escapeIdentifier(String identifier) {
    return '`' + identifier.replace("`", "``") + '`';
  }

  public static String escapeString(String identifier) {
    return "'" + identifier.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  public static <OutputT> Coder<OutputT> inferCoder(
      SingleStoreIO.RowMapper<OutputT> rowMapper,
      CoderRegistry registry,
      SchemaRegistry schemaRegistry,
      Logger log) {
    if (rowMapper instanceof SingleStoreIO.RowMapperWithCoder) {
      try {
        return ((SingleStoreIO.RowMapperWithCoder<OutputT>) rowMapper).getCoder();
      } catch (Exception e) {
        log.warn("Unable to infer a coder from RowMapper. Attempting to infer a coder from type.");
      }
    }

    TypeDescriptor<OutputT> outputType =
        TypeDescriptors.extractFromTypeParameters(
            rowMapper,
            SingleStoreIO.RowMapper.class,
            new TypeDescriptors.TypeVariableExtractor<
                SingleStoreIO.RowMapper<OutputT>, OutputT>() {});
    try {
      return schemaRegistry.getSchemaCoder(outputType);
    } catch (NoSuchSchemaException e) {
      log.warn(
          "Unable to infer a schema for type {}. Attempting to infer a coder without a schema.",
          outputType);
    }
    try {
      return registry.getCoder(outputType);
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException(
          String.format("Unable to infer a coder for type %s", outputType));
    }
  }

  public static String getSelectQuery(@Nullable String table, @Nullable String query) {
    if (table != null && query != null) {
      throw new IllegalArgumentException("withTable() can not be used together with withQuery()");
    } else if (table != null) {
      return "SELECT * FROM " + SingleStoreUtil.escapeIdentifier(table);
    } else if (query != null) {
      return query;
    } else {
      throw new IllegalArgumentException("One of withTable() or withQuery() is required");
    }
  }

  public static <OutputT> OutputT getArgumentWithDefault(
      @Nullable OutputT value, OutputT defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  public static <T> @Nullable String getClassNameOrNull(@Nullable T value) {
    if (value != null) {
      return value.getClass().getName();
    }
    return null;
  }
}

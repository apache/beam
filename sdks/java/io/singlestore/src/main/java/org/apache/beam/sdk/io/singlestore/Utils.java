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
import org.slf4j.Logger;

public class Utils {
  public static String escapeIdentifier(String identifier) {
    return '`' + identifier.replace("`", "``") + '`';
  }

  public static String escapeString(String identifier) {
    return "'" + identifier.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  public static <OutputT> Coder<OutputT> inferCoder(
      Coder<OutputT> defaultCoder,
      RowMapper<OutputT> rowMapper,
      CoderRegistry registry,
      SchemaRegistry schemaRegistry,
      Logger LOG) {
    if (defaultCoder != null) {
      return defaultCoder;
    } else {
      TypeDescriptor<OutputT> outputType =
          TypeDescriptors.extractFromTypeParameters(
              rowMapper,
              RowMapper.class,
              new TypeDescriptors.TypeVariableExtractor<RowMapper<OutputT>, OutputT>() {});
      try {
        return schemaRegistry.getSchemaCoder(outputType);
      } catch (NoSuchSchemaException e) {
        LOG.warn(
            "Unable to infer a schema for type {}. Attempting to infer a coder without a schema.",
            outputType);
      }
      try {
        return registry.getCoder(outputType);
      } catch (CannotProvideCoderException e) {
        LOG.warn("Unable to infer a coder for type {}", outputType);
        return null;
      }
    }
  }
}

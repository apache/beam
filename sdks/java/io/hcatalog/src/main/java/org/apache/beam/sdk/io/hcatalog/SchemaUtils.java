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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;

/** Utils to convert between HCatalog schema types and Beam schema types. */
@Experimental(Kind.SCHEMAS)
class SchemaUtils {

  private static final Map<HCatFieldSchema.Type, FieldType> HCAT_TO_BEAM_TYPES_MAP =
      ImmutableMap.<HCatFieldSchema.Type, FieldType>builder()
          .put(HCatFieldSchema.Type.BOOLEAN, FieldType.BOOLEAN)
          .put(HCatFieldSchema.Type.TINYINT, FieldType.BYTE)
          .put(HCatFieldSchema.Type.SMALLINT, FieldType.INT16)
          .put(HCatFieldSchema.Type.INT, FieldType.INT32)
          .put(HCatFieldSchema.Type.BIGINT, FieldType.INT64)
          .put(HCatFieldSchema.Type.FLOAT, FieldType.FLOAT)
          .put(HCatFieldSchema.Type.DOUBLE, FieldType.DOUBLE)
          .put(HCatFieldSchema.Type.DECIMAL, FieldType.DECIMAL)
          .put(HCatFieldSchema.Type.STRING, FieldType.STRING)
          .put(HCatFieldSchema.Type.CHAR, FieldType.STRING)
          .put(HCatFieldSchema.Type.VARCHAR, FieldType.STRING)
          .put(HCatFieldSchema.Type.BINARY, FieldType.BYTES)
          .put(HCatFieldSchema.Type.DATE, FieldType.DATETIME)
          .put(HCatFieldSchema.Type.TIMESTAMP, FieldType.DATETIME)
          .build();

  static Schema toBeamSchema(List<FieldSchema> fields) {
    return fields.stream().map(SchemaUtils::toBeamField).collect(toSchema());
  }

  private static Schema.Field toBeamField(FieldSchema field) {
    String name = field.getName();
    HCatFieldSchema hCatFieldSchema;

    try {
      hCatFieldSchema = HCatSchemaUtils.getHCatFieldSchema(field);
    } catch (HCatException e) {
      // Converting checked Exception to unchecked Exception.
      throw new UnsupportedOperationException(
          "Error while converting FieldSchema to HCatFieldSchema", e);
    }

    switch (hCatFieldSchema.getCategory()) {
      case PRIMITIVE:
        {
          if (!HCAT_TO_BEAM_TYPES_MAP.containsKey(hCatFieldSchema.getType())) {
            throw new UnsupportedOperationException(
                "The Primitive HCat type '"
                    + field.getType()
                    + "' of field '"
                    + name
                    + "' cannot be converted to Beam FieldType");
          }

          FieldType fieldType = HCAT_TO_BEAM_TYPES_MAP.get(hCatFieldSchema.getType());
          return Schema.Field.of(name, fieldType).withNullable(true);
        }
        // TODO: Add Support for Complex Types i.e. ARRAY, MAP, STRUCT
      default:
        throw new UnsupportedOperationException(
            "The category '" + hCatFieldSchema.getCategory() + "' is not supported.");
    }
  }
}

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
package org.apache.beam.sdk.util;

import static org.apache.beam.sdk.schemas.Schema.TypeName.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.TypeName.BYTE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT16;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT32;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT64;
import static org.apache.beam.sdk.schemas.Schema.TypeName.STRING;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * Validates if the types specified in {@link Row} {@link Schema}
 * are supported for conversion from Json.
 */
class RowJsonValidation {

  private static final Set<Schema.TypeName> SUPPORTED_TYPES =
      ImmutableSet.of(BYTE, INT16, INT32, INT64, FLOAT, DOUBLE, BOOLEAN, STRING);

  static void verifyFieldTypeSupported(Schema.Field field) {
    Schema.FieldType fieldType = field.getType();
    verifyFieldTypeSupported(fieldType);
  }

  static void verifyFieldTypeSupported(Schema.FieldType fieldType) {
    Schema.TypeName fieldTypeName = fieldType.getTypeName();

    if (fieldTypeName.isCompositeType()) {
      Schema rowFieldSchema = fieldType.getRowSchema();
      rowFieldSchema.getFields().forEach(RowJsonValidation::verifyFieldTypeSupported);
      return;
    }

    if (fieldTypeName.isContainerType()) {
      verifyFieldTypeSupported(fieldType.getComponentType());
      return;
    }

    if (!SUPPORTED_TYPES.contains(fieldTypeName)) {
      throw new RowJsonDeserializer.UnsupportedRowJsonException(
          fieldTypeName.name() + " is not supported when converting JSON objects to Rows. "
          + "Supported types are: " + SUPPORTED_TYPES.toString());
    }
  }
}

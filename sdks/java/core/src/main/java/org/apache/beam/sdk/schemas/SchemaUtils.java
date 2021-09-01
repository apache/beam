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
package org.apache.beam.sdk.schemas;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/** A set of utility functions for schemas. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SchemaUtils {
  /**
   * Given two schema that have matching types, return a nullable-widened schema.
   *
   * <p>The schemas must have matching types, except for field names which can differ. The returned
   * schema will contain the field names in the first schema. All field types will be nullable if
   * the corresponding field type is nullable in either of the input schemas.
   */
  public static Schema mergeWideningNullable(Schema schema1, Schema schema2) {
    if (schema1.getFieldCount() != schema2.getFieldCount()) {
      throw new IllegalArgumentException(
          "Cannot merge schemas with different numbers of fields. "
              + "schema1: "
              + schema1
              + " schema2: "
              + schema2);
    }
    Schema.Builder builder = Schema.builder();
    for (int i = 0; i < schema1.getFieldCount(); ++i) {
      String name = schema1.getField(i).getName();
      builder.addField(
          name, widenNullableTypes(schema1.getField(i).getType(), schema2.getField(i).getType()));
    }
    return builder.build();
  }

  static FieldType widenNullableTypes(FieldType fieldType1, FieldType fieldType2) {
    if (fieldType1.getTypeName() != fieldType2.getTypeName()) {
      throw new IllegalArgumentException(
          "Cannot merge two types: "
              + fieldType1.getTypeName()
              + " and "
              + fieldType2.getTypeName());
    }

    FieldType result;
    switch (fieldType1.getTypeName()) {
      case ROW:
        result =
            FieldType.row(
                mergeWideningNullable(fieldType1.getRowSchema(), fieldType2.getRowSchema()));
        break;
      case ARRAY:
        FieldType arrayElementType =
            widenNullableTypes(
                fieldType1.getCollectionElementType(), fieldType2.getCollectionElementType());
        result = FieldType.array(arrayElementType);
        break;
      case ITERABLE:
        FieldType iterableElementType =
            widenNullableTypes(
                fieldType1.getCollectionElementType(), fieldType2.getCollectionElementType());
        result = FieldType.iterable(iterableElementType);
        break;
      case MAP:
        FieldType keyType =
            widenNullableTypes(fieldType1.getMapKeyType(), fieldType2.getMapKeyType());
        FieldType valueType =
            widenNullableTypes(fieldType1.getMapValueType(), fieldType2.getMapValueType());
        result = FieldType.map(keyType, valueType);
        break;
      case LOGICAL_TYPE:
        if (!fieldType1
            .getLogicalType()
            .getIdentifier()
            .equals(fieldType2.getLogicalType().getIdentifier())) {
          throw new IllegalArgumentException(
              "Logical types don't match and cannot be merged: "
                  + fieldType1.getLogicalType().getIdentifier()
                  + ".v.s"
                  + fieldType2.getLogicalType().getIdentifier());
        }
        // fall through
      default:
        result = fieldType1;
    }
    return result.withNullable(fieldType1.getNullable() || fieldType2.getNullable());
  }

  // Return a row to the given schema.
  public static Row convertRowToSchema(Row row, Schema schema) {
    if (schema.equivalent(row.getSchema())) return row;
    Row.Builder builder = Row.withSchema(schema);
    for (Field field : schema.getFields()) {
      if (row.getSchema().hasField(field.getName())) {
        builder.addValue(row.getValue(field.getName()));
      } else {
        if (!field.getType().getNullable()) {
          throw new IllegalArgumentException("Missing non-nullable field.");
        }
        builder.addValue(null);
      }
    }
    return builder.build();
  }

  public static Map<String, Schema> getSchema(
      SchemaTransform transform, Map<String, Schema> inputSchemas) {
    Pipeline p = Pipeline.create();
    PCollectionRowTuple inputTuple = PCollectionRowTuple.empty(p);
    for (Entry<String, Schema> entry : inputSchemas.entrySet()) {
      inputTuple =
          inputTuple.and(entry.getKey(), p.apply(Create.empty(RowCoder.of(entry.getValue()))));
    }
    PCollectionRowTuple outputTuple = inputTuple.apply(transform.buildTransform());
    Map<String, Schema> outputSchemas = new HashMap<>();
    for (Entry<TupleTag<Row>, PCollection<Row>> entry : outputTuple.getAll().entrySet()) {
      outputSchemas.put(entry.getKey().getId(), entry.getValue().getSchema());
    }
    return outputSchemas;
  }
}

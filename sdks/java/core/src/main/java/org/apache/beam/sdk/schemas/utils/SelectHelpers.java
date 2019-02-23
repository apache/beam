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
package org.apache.beam.sdk.schemas.utils;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.ListQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.MapQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;

/** Helper methods to select fields from a Schema. */
public class SelectHelpers {
  // Currently we don't flatten selected nested fields.
  public static Schema getOutputSchema(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    if (fieldAccessDescriptor.getAllFields()) {
      return inputSchema;
    }
    Schema.Builder builder = new Schema.Builder();
    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      builder.addField(inputSchema.getField(fieldId));
    }

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor fieldDescriptor = nested.getKey();
      Field field = inputSchema.getField(checkNotNull(fieldDescriptor.getFieldId()));
      FieldType outputType =
          getOutputSchemaHelper(
              field.getType(), nested.getValue(), fieldDescriptor.getQualifiers(), 0);
      builder.addField(field.getName(), outputType);
    }
    return builder.build();
  }

  private static FieldType getOutputSchemaHelper(
      FieldType inputFieldType,
      FieldAccessDescriptor fieldAccessDescriptor,
      List<Qualifier> qualifiers,
      int qualifierPosition) {
    if (qualifierPosition >= qualifiers.size()) {
      // We have walked through any containers, and are at a row type. Extract the subschema
      // for the row, preserving nullable attributes.
      checkArgument(inputFieldType.getTypeName().isCompositeType());
      return FieldType.row(getOutputSchema(inputFieldType.getRowSchema(), fieldAccessDescriptor))
          .withNullable(inputFieldType.getNullable());
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    switch (qualifier.getKind()) {
      case LIST:
        checkArgument(qualifier.getList().equals(ListQualifier.ALL));
        FieldType componentType = checkNotNull(inputFieldType.getCollectionElementType());
        FieldType outputComponent =
            getOutputSchemaHelper(
                    componentType, fieldAccessDescriptor, qualifiers, qualifierPosition + 1)
                .withNullable(componentType.getNullable());
        return FieldType.array(outputComponent).withNullable(inputFieldType.getNullable());
      case MAP:
        checkArgument(qualifier.getMap().equals(MapQualifier.ALL));
        FieldType keyType = checkNotNull(inputFieldType.getMapKeyType());
        FieldType valueType = checkNotNull(inputFieldType.getMapValueType());
        FieldType outputValueType =
            getOutputSchemaHelper(
                    valueType, fieldAccessDescriptor, qualifiers, qualifierPosition + 1)
                .withNullable(valueType.getNullable());
        return FieldType.map(keyType, outputValueType).withNullable(inputFieldType.getNullable());
      default:
        throw new RuntimeException("unexpected");
    }
  }

  public static Row selectRow(
      Row input,
      FieldAccessDescriptor fieldAccessDescriptor,
      Schema inputSchema,
      Schema outputSchema) {
    if (fieldAccessDescriptor.getAllFields()) {
      return input;
    }

    Row.Builder output = Row.withSchema(outputSchema);
    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      // TODO: Once we support specific qualifiers (like array slices), extract them here.
      output.addValue(input.getValue(fieldId));
    }

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor field = nested.getKey();
      String fieldName = inputSchema.nameOf(checkNotNull(field.getFieldId()));
      FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();
      FieldType nestedOutputType = outputSchema.getField(fieldName).getType();
      Object value =
          selectRowHelper(
              field.getQualifiers(),
              0,
              input.getValue(fieldName),
              nested.getValue(),
              nestedInputType,
              nestedOutputType);
      output.addValue(value);
    }
    return output.build();
  }

  @SuppressWarnings("unchecked")
  private static Object selectRowHelper(
      List<Qualifier> qualifiers,
      int qualifierPosition,
      Object value,
      FieldAccessDescriptor fieldAccessDescriptor,
      FieldType inputType,
      FieldType outputType) {
    if (qualifierPosition >= qualifiers.size()) {
      Row row = (Row) value;
      return selectRow(
          row, fieldAccessDescriptor, inputType.getRowSchema(), outputType.getRowSchema());
    }

    if (fieldAccessDescriptor.getAllFields()) {
      // Since we are selecting all fields (and we do not yet support array slicing), short circuit.
      return value;
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    switch (qualifier.getKind()) {
      case LIST:
        {
          FieldType nestedInputType = checkNotNull(inputType.getCollectionElementType());
          FieldType nestedOutputType = checkNotNull(outputType.getCollectionElementType());
          List<Object> list = (List) value;
          List selectedList = Lists.newArrayListWithCapacity(list.size());
          for (Object o : list) {
            Object selected =
                selectRowHelper(
                    qualifiers,
                    qualifierPosition + 1,
                    o,
                    fieldAccessDescriptor,
                    nestedInputType,
                    nestedOutputType);
            selectedList.add(selected);
          }
          return selectedList;
        }
      case MAP:
        {
          FieldType nestedInputType = checkNotNull(inputType.getMapValueType());
          FieldType nestedOutputType = checkNotNull(outputType.getMapValueType());
          Map<Object, Object> map = (Map) value;
          Map selectedMap = Maps.newHashMapWithExpectedSize(map.size());
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            Object selected =
                selectRowHelper(
                    qualifiers,
                    qualifierPosition + 1,
                    entry.getValue(),
                    fieldAccessDescriptor,
                    nestedInputType,
                    nestedOutputType);
            selectedMap.put(entry.getKey(), selected);
          }
          return selectedMap;
        }
      default:
        throw new RuntimeException("Unexpected type " + qualifier.getKind());
    }
  }
}

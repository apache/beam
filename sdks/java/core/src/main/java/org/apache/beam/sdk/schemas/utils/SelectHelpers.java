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
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

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
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;

/** Helper methods to select subrows out of rows. */
public class SelectHelpers {
  /**
   * Checks whether a FieldAccessDescriptor selects only a single field. The descriptor is expected
   * to already be resolved.
   */
  private static boolean singleFieldSelected(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    if (fieldAccessDescriptor.getAllFields()) {
      return false;
    }
    if (fieldAccessDescriptor.fieldIdsAccessed().size() == 1
        && fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()) {
      int fieldId = fieldAccessDescriptor.fieldIdsAccessed().iterator().next();
      Field field = inputSchema.getField(fieldId);
      return TypeName.ROW.equals(field.getType().getTypeName());
    }

    if (fieldAccessDescriptor.fieldIdsAccessed().isEmpty()
        && fieldAccessDescriptor.getNestedFieldsAccessed().size() == 1) {
      FieldDescriptor key =
          fieldAccessDescriptor.getNestedFieldsAccessed().keySet().iterator().next();
      Field field = inputSchema.getField(checkNotNull(key.getFieldId()));
      if (!field.getType().getTypeName().isCollectionType()
          && !field.getType().getTypeName().isMapType()
          && key.getQualifiers().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the output schema resulting from selecting the given {@link FieldAccessDescriptor} from the
   * given schema.
   *
   * <p>The unnest field controls the behavior when selecting a single field. For example, consider
   * the following Java POJOs:
   *
   * <pre>{@code
   *   class UserEvent {
   *     String userId;
   *     String eventId;
   *     int eventType;
   *     Location location;
   *  }</pre>
   *
   *
   * <pre>{@code
   *  class Location {
   *    double latitude;
   *     double longtitude;
   *  }}</pre>
   *
   *  <p> If selecting just the location field and unnest is true, then the returned schema will
   *  match just that of the singular field being selected; in this case the returned schema will
   *  be that of the Location class. If unnest is false, then the returned schema will match the
   *  levels of nesting that the original schema had. In this case, it would be an outer schema
   *  containing a single ROW field named "location" that matched the Location schema.
   *
   *  <p>In most cases, the user's expectations matches that when unnest is true.
   */
  public static Schema getOutputSchema(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor, boolean unnest) {
    if (fieldAccessDescriptor.getAllFields()) {
      return inputSchema;
    }
    boolean selectsNestedSingle = unnest && singleFieldSelected(inputSchema, fieldAccessDescriptor);

    Schema.Builder builder = new Schema.Builder();
    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      if (selectsNestedSingle) {
        // The entire nested row is selected, so we can short circuit and return that type.
        return inputSchema.getField(fieldId).getType().getRowSchema();
      }
      builder.addField(inputSchema.getField(fieldId));
    }

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor fieldDescriptor = nested.getKey();
      FieldAccessDescriptor nestedAccess = nested.getValue();
      Field field = inputSchema.getField(checkNotNull(fieldDescriptor.getFieldId()));
      if (selectsNestedSingle) {
        checkState(field.getType().getTypeName().isCompositeType());
        return getOutputSchema(field.getType().getRowSchema(), nestedAccess, unnest);
      } else {
        FieldType outputType =
            getOutputSchemaHelper(
                field.getType(), nestedAccess, fieldDescriptor.getQualifiers(), unnest, 0);
        builder.addField(field.getName(), outputType);
      }
    }
    return builder.build();
  }

  private static FieldType getOutputSchemaHelper(
      FieldType inputFieldType,
      FieldAccessDescriptor fieldAccessDescriptor,
      List<Qualifier> qualifiers,
      boolean unnest,
      int qualifierPosition) {
    if (qualifierPosition >= qualifiers.size()) {
      // We have walked through any containers, and are at a row type. Extract the subschema
      // for the row, preserving nullable attributes.
      checkArgument(inputFieldType.getTypeName().isCompositeType());
      return FieldType.row(
              getOutputSchema(inputFieldType.getRowSchema(), fieldAccessDescriptor, unnest))
          .withNullable(inputFieldType.getNullable());
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    switch (qualifier.getKind()) {
      case LIST:
        checkArgument(qualifier.getList().equals(ListQualifier.ALL));
        FieldType componentType = checkNotNull(inputFieldType.getCollectionElementType());
        FieldType outputComponent =
            getOutputSchemaHelper(
                    componentType, fieldAccessDescriptor, qualifiers, unnest, qualifierPosition + 1)
                .withNullable(componentType.getNullable());
        return FieldType.array(outputComponent).withNullable(inputFieldType.getNullable());
      case MAP:
        checkArgument(qualifier.getMap().equals(MapQualifier.ALL));
        FieldType keyType = checkNotNull(inputFieldType.getMapKeyType());
        FieldType valueType = checkNotNull(inputFieldType.getMapValueType());
        FieldType outputValueType =
            getOutputSchemaHelper(
                    valueType, fieldAccessDescriptor, qualifiers, unnest, qualifierPosition + 1)
                .withNullable(valueType.getNullable());
        return FieldType.map(keyType, outputValueType).withNullable(inputFieldType.getNullable());
      default:
        throw new RuntimeException("unexpected");
    }
  }

  private static Row selectNestedRow(
      Row input,
      FieldAccessDescriptor fieldAccessDescriptor,
      Schema inputSchema,
      Schema outputSchema) {
    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      return input.getValue(fieldId);
    }

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor field = nested.getKey();
      FieldAccessDescriptor nestedAccess = nested.getValue();
      String fieldName = inputSchema.nameOf(checkNotNull(field.getFieldId()));
      Schema nestedSchema = inputSchema.getField(field.getFieldId()).getType().getRowSchema();
      return selectRow(input.getValue(fieldName), nestedAccess, nestedSchema, outputSchema, true);
    }

    throw new IllegalStateException("Unreachable.");
  }

  /** Select out of a given {@link Row} object. */
  public static Row selectRow(
      Row input,
      FieldAccessDescriptor fieldAccessDescriptor,
      Schema inputSchema,
      Schema outputSchema,
      boolean unnest) {
    if (fieldAccessDescriptor.getAllFields()) {
      return input;
    }

    if (unnest && singleFieldSelected(inputSchema, fieldAccessDescriptor)) {
      return selectNestedRow(input, fieldAccessDescriptor, inputSchema, outputSchema);
    }

    Row.Builder output = Row.withSchema(outputSchema);
    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      // TODO: Once we support specific qualifiers (like array slices), extract them here.
      output.addValue(input.getValue(fieldId));
    }

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor field = nested.getKey();
      FieldAccessDescriptor nestedAccess = nested.getValue();
      String fieldName = inputSchema.nameOf(checkNotNull(field.getFieldId()));
      FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();
      FieldType nestedOutputType = outputSchema.getField(fieldName).getType();
      Object selectedValue =
          selectRowHelper(
              field.getQualifiers(),
              0,
              input.getValue(fieldName),
              nestedAccess,
              nestedInputType,
              nestedOutputType,
              unnest);
      output.addValue(selectedValue);
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
      FieldType outputType,
      boolean unnest) {
    if (qualifierPosition >= qualifiers.size()) {
      Row row = (Row) value;
      return selectRow(
          row, fieldAccessDescriptor, inputType.getRowSchema(), outputType.getRowSchema(), unnest);
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
                    nestedOutputType,
                    unnest);
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
                    nestedOutputType,
                    unnest);
            selectedMap.put(entry.getKey(), selected);
          }
          return selectedMap;
        }
      default:
        throw new RuntimeException("Unexpected type " + qualifier.getKind());
    }
  }
}

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

/** Helper methods to select subrows out of rows. */
public class SelectHelpers {

  private static Schema union(Iterable<Schema> schemas) {
    Schema.Builder unioned = Schema.builder();
    for (Schema schema : schemas) {
      unioned.addFields(schema.getFields());
    }
    return unioned.build();
  }

  /**
   * Get the output schema resulting from selecting the given {@link FieldAccessDescriptor} from the
   * given schema.
   *
   * <p>Fields are always extracted and then stored in a new Row. For example, consider the
   * following Java POJOs:
   *
   * <pre>{@code
   *  class UserEvent {
   *    String userId;
   *    String eventId;
   *    int eventType;
   *    Location location;
   * }
   * }</pre>
   *
   * <pre>{@code
   * class Location {
   *   double latitude;
   *   double longtitude;
   * }
   * }</pre>
   *
   * <p>If selecting just the location field, then the returned schema will wrap that of the
   * singular field being selected; in this case the returned schema will be a Row containing a
   * single Location field. If location.latitude is selected, then the returned Schema will be a Row
   * containing a double latitude field.
   *
   * <p>The same holds true when selecting from lists or maps. For example:
   *
   * <pre>{@code
   * class EventList {
   *   List<UserEvent> events;
   * }
   * }</pre>
   *
   * <p>If selecting events.location.latitude, the returned schema will contain a single array of
   * Row, where that Row contains a single double latitude field; it will not contain an array of
   * double.
   */
  public static Schema getOutputSchema(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    if (fieldAccessDescriptor.getAllFields()) {
      return inputSchema;
    }

    List<Schema> schemas = Lists.newArrayList();
    Schema.Builder builder = Schema.builder();
    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      builder.addField(inputSchema.getField(fieldId));
    }
    schemas.add(builder.build());

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor fieldDescriptor = nested.getKey();
      FieldAccessDescriptor nestedAccess = nested.getValue();
      Field field = inputSchema.getField(checkNotNull(fieldDescriptor.getFieldId()));

      FieldType outputType =
          getOutputSchemaHelper(field.getType(), nestedAccess, fieldDescriptor.getQualifiers(), 0);
      if (outputType.getTypeName().isCompositeType()) {
        schemas.add(outputType.getRowSchema());
      } else {
        schemas.add(Schema.builder().addField(field.getName(), outputType).build());
      }
    }

    return union(schemas);
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
    selectIntoRow(input, output, fieldAccessDescriptor, inputSchema);
    return output.build();
  }

  /** Select out of a given {@link Row} object. */
  public static void selectIntoRow(
      Row input,
      Row.Builder output,
      FieldAccessDescriptor fieldAccessDescriptor,
      Schema inputSchema) {
    if (fieldAccessDescriptor.getAllFields()) {
      output.addValues(input.getValues());
      return;
    }

    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      // TODO: Once we support specific qualifiers (like array slices), extract them here.
      output.addValue(input.getValue(fieldId));
    }

    Schema outputSchema = output.getSchema();
    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor field = nested.getKey();
      FieldAccessDescriptor nestedAccess = nested.getValue();
      FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();
      FieldType nestedOutputType = outputSchema.getField(output.nextFieldId()).getType();

      if (nestedOutputType.getTypeName().isCompositeType()) {
        Row.Builder nestedBuilder = Row.withSchema(nestedOutputType.getRowSchema());
        selectIntoRowHelper(
            field.getQualifiers(),
            input.getValue(field.getFieldId()),
            nestedBuilder,
            nestedAccess,
            nestedInputType,
            nestedOutputType);
        output.addValue(nestedBuilder.build());
      } else {
        selectIntoRowHelper(
            field.getQualifiers(),
            input.getValue(field.getFieldId()),
            output,
            nestedAccess,
            nestedInputType,
            nestedOutputType);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void selectIntoRowHelper(
      List<Qualifier> qualifiers,
      Object value,
      Row.Builder output,
      FieldAccessDescriptor fieldAccessDescriptor,
      FieldType inputType,
      FieldType outputType) {
    if (qualifiers.isEmpty()) {
      Row row = (Row) value;
      selectIntoRow(row, output, fieldAccessDescriptor, inputType.getRowSchema());
      return;
    }

    // There are qualifiers. That means that the result will be either a list or a map, so
    // construct the result and add that to our Row.
    output.addValue(
        selectValueHelper(qualifiers, 0, value, fieldAccessDescriptor, inputType, outputType));
  }

  private static Object selectValueHelper(
      List<Qualifier> qualifiers,
      int qualifierPosition,
      Object value,
      FieldAccessDescriptor fieldAccessDescriptor,
      FieldType inputType,
      FieldType outputType) {
    if (qualifierPosition >= qualifiers.size()) {
      // We have already constructed all arrays and maps. What remains must be a Row.
      Row row = (Row) value;
      Row.Builder output = Row.withSchema(outputType.getRowSchema());
      selectIntoRow(row, output, fieldAccessDescriptor, inputType.getRowSchema());
      return output.build();
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    switch (qualifier.getKind()) {
      case LIST:
        {
          FieldType nestedInputType = checkNotNull(inputType.getCollectionElementType());
          FieldType nestedOutputType = checkNotNull(outputType.getCollectionElementType());
          List<Object> list = (List) value;
          List<Object> selectedList = Lists.newArrayListWithCapacity(list.size());
          for (Object o : list) {
            Object selected =
                selectValueHelper(
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
                selectValueHelper(
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

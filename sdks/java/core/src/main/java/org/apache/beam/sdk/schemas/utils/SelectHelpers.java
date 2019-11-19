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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

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
   *   double longitude;
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
      Schema outputSchema =
          getOutputSchemaHelper(field.getType(), nestedAccess, fieldDescriptor.getQualifiers(), 0);
      schemas.add(outputSchema);
    }

    return union(schemas);
  }

  private static Schema getOutputSchemaHelper(
      FieldType inputFieldType,
      FieldAccessDescriptor fieldAccessDescriptor,
      List<Qualifier> qualifiers,
      int qualifierPosition) {
    if (qualifierPosition >= qualifiers.size()) {
      // We have walked through any containers, and are at a row type. Extract the subschema
      // for the row, preserving nullable attributes.
      checkArgument(inputFieldType.getTypeName().isCompositeType());
      return getOutputSchema(inputFieldType.getRowSchema(), fieldAccessDescriptor);
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    Schema.Builder builder = Schema.builder();
    switch (qualifier.getKind()) {
      case LIST:
        checkArgument(qualifier.getList().equals(ListQualifier.ALL));
        FieldType componentType = checkNotNull(inputFieldType.getCollectionElementType());
        Schema outputComponent =
            getOutputSchemaHelper(
                componentType, fieldAccessDescriptor, qualifiers, qualifierPosition + 1);
        for (Field field : outputComponent.getFields()) {
          Field newField;
          if (TypeName.ARRAY.equals(inputFieldType.getTypeName())) {
            newField =
                Field.of(field.getName(), FieldType.array(field.getType()))
                    .withNullable(inputFieldType.getNullable());
          } else {
            checkArgument(TypeName.ITERABLE.equals(inputFieldType.getTypeName()));
            newField =
                Field.of(field.getName(), FieldType.iterable(field.getType()))
                    .withNullable(inputFieldType.getNullable());
          }
          builder.addField(newField);
        }
        return builder.build();
      case MAP:
        checkArgument(qualifier.getMap().equals(MapQualifier.ALL));
        FieldType keyType = checkNotNull(inputFieldType.getMapKeyType());
        FieldType valueType = checkNotNull(inputFieldType.getMapValueType());
        Schema outputValueSchema =
            getOutputSchemaHelper(
                valueType, fieldAccessDescriptor, qualifiers, qualifierPosition + 1);
        for (Field field : outputValueSchema.getFields()) {
          Field newField =
              Field.of(field.getName(), FieldType.map(keyType, field.getType()))
                  .withNullable(inputFieldType.getNullable());
          builder.addField(newField);
        }
        return builder.build();
      default:
        throw new RuntimeException("unexpected");
    }
  }

  /** Select a sub Row from an input Row. */
  public static Row selectRow(
      Row input,
      FieldAccessDescriptor fieldAccessDescriptor,
      Schema inputSchema,
      Schema outputSchema) {
    if (fieldAccessDescriptor.getAllFields()) {
      return input;
    }

    Row.Builder output = Row.withSchema(outputSchema);
    selectIntoRow(input, output, fieldAccessDescriptor);
    return output.build();
  }

  /** Select out of a given {@link Row} object. */
  public static void selectIntoRow(
      Row input, Row.Builder output, FieldAccessDescriptor fieldAccessDescriptor) {
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
      FieldType nestedInputType = input.getSchema().getField(field.getFieldId()).getType();
      FieldType nestedOutputType = outputSchema.getField(output.nextFieldId()).getType();
      selectIntoRowHelper(
          field.getQualifiers(),
          input.getValue(field.getFieldId()),
          output,
          nestedAccess,
          nestedInputType,
          nestedOutputType);
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
      selectIntoRow(row, output, fieldAccessDescriptor);
      return;
    }

    // There are qualifiers. That means that the result will be either a list or a map, so
    // construct the result and add that to our Row.
    selectIntoRowWithQualifiers(
        qualifiers, 0, value, output, fieldAccessDescriptor, inputType, outputType);
  }

  private static void selectIntoRowWithQualifiers(
      List<Qualifier> qualifiers,
      int qualifierPosition,
      Object value,
      Row.Builder output,
      FieldAccessDescriptor fieldAccessDescriptor,
      FieldType inputType,
      FieldType outputType) {
    if (qualifierPosition >= qualifiers.size()) {
      // We have already constructed all arrays and maps. What remains must be a Row.
      Row row = (Row) value;
      selectIntoRow(row, output, fieldAccessDescriptor);
      return;
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    switch (qualifier.getKind()) {
      case LIST:
        {
          FieldType nestedInputType = checkNotNull(inputType.getCollectionElementType());
          FieldType nestedOutputType = checkNotNull(outputType.getCollectionElementType());
          Iterable<Object> iterable = (Iterable) value;

          // When selecting multiple subelements under a list, we distribute the select
          // resulting in multiple lists. For example, if there is a field "list" with type
          // {a: string, b: int}[], selecting list.a, list.b results in a schema of type
          // {a: string[], b: int[]}. This preserves the invariant that the name selected always
          // appears in the top-level schema.
          Schema tempSchema = Schema.builder().addField("a", nestedInputType).build();
          FieldAccessDescriptor tempAccessDescriptor =
              FieldAccessDescriptor.create()
                  .withNestedField("a", fieldAccessDescriptor)
                  .resolve(tempSchema);
          // TODO: doing this on each element might be inefficient. Consider caching this, or
          // using codegen based on the schema.
          Schema nestedSchema = getOutputSchema(tempSchema, tempAccessDescriptor);

          List<List<Object>> selectedLists =
              Lists.newArrayListWithCapacity(nestedSchema.getFieldCount());
          for (int i = 0; i < nestedSchema.getFieldCount(); i++) {
            selectedLists.add(Lists.newArrayListWithCapacity(Iterables.size(iterable)));
          }
          for (Object o : iterable) {
            Row.Builder selectElementBuilder = Row.withSchema(nestedSchema);
            selectIntoRowWithQualifiers(
                qualifiers,
                qualifierPosition + 1,
                o,
                selectElementBuilder,
                fieldAccessDescriptor,
                nestedInputType,
                nestedOutputType);

            Row elementBeforeDistribution = selectElementBuilder.build();
            for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
              selectedLists.get(i).add(elementBeforeDistribution.getValue(i));
            }
          }
          for (List aList : selectedLists) {
            output.addValue(aList);
          }
          break;
        }
      case MAP:
        {
          FieldType nestedInputType = checkNotNull(inputType.getMapValueType());
          FieldType nestedOutputType = checkNotNull(outputType.getMapValueType());

          // When selecting multiple subelements under a map, we distribute the select
          // resulting in multiple maps. The semantics are the same as for lists above (except we
          // only support subelement select for map values, not for map keys).
          Schema tempSchema = Schema.builder().addField("a", nestedInputType).build();
          FieldAccessDescriptor tempAccessDescriptor =
              FieldAccessDescriptor.create()
                  .withNestedField("a", fieldAccessDescriptor)
                  .resolve(tempSchema);
          Schema nestedSchema = getOutputSchema(tempSchema, tempAccessDescriptor);
          List<Map> selectedMaps = Lists.newArrayListWithExpectedSize(nestedSchema.getFieldCount());
          for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
            selectedMaps.add(Maps.newHashMap());
          }

          Map<Object, Object> map = (Map) value;
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            Row.Builder selectValueBuilder = Row.withSchema(nestedSchema);
            selectIntoRowWithQualifiers(
                qualifiers,
                qualifierPosition + 1,
                entry.getValue(),
                selectValueBuilder,
                fieldAccessDescriptor,
                nestedInputType,
                nestedOutputType);

            Row valueBeforeDistribution = selectValueBuilder.build();
            for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
              selectedMaps.get(i).put(entry.getKey(), valueBeforeDistribution.getValue(i));
            }
          }
          for (Map aMap : selectedMaps) {
            output.addValue(aMap);
          }
          break;
        }
      default:
        throw new RuntimeException("Unexpected type " + qualifier.getKind());
    }
  }
}

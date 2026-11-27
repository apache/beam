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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper methods to select subrows out of rows. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
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
    return getOutputSchemaTrackingNullable(inputSchema, fieldAccessDescriptor, false);
  }

  private static Schema getOutputSchemaTrackingNullable(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor, boolean isNullable) {
    if (fieldAccessDescriptor.getAllFields()) {
      Schema schemaToReturn = inputSchema;
      if (isNullable) {
        // Some parent field in the selector was nullable, so we must mark all of these fields
        // nullable.
        schemaToReturn =
            inputSchema.getFields().stream()
                .map(f -> f.withNullable(true))
                .collect(Schema.toSchema());
      }
      return schemaToReturn;
    }

    List<Schema> schemas = Lists.newArrayList();
    Schema.Builder builder = Schema.builder();
    for (FieldDescriptor fieldDescriptor : fieldAccessDescriptor.getFieldsAccessed()) {
      Field field = inputSchema.getField(fieldDescriptor.getFieldId());
      if (fieldDescriptor.getFieldRename() != null) {
        field = field.withName(fieldDescriptor.getFieldRename());
      }

      // If any parent field is nullable, this one should be as well. So if selecting a.b, the
      // resulting field should
      // be nullable if a is nullable (even if b was not in the original schema).
      if (isNullable) {
        field = field.withNullable(true);
      }
      builder.addField(field);
    }
    schemas.add(builder.build());

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor fieldDescriptor = nested.getKey();
      FieldAccessDescriptor nestedAccess = nested.getValue();
      Field field = inputSchema.getField(checkNotNull(fieldDescriptor.getFieldId()));
      if (fieldDescriptor.getFieldRename() != null) {
        field = field.withName(fieldDescriptor.getFieldRename());
      }
      Schema outputSchema =
          getOutputSchemaHelper(
              field.getType(),
              nestedAccess,
              fieldDescriptor.getQualifiers(),
              0,
              isNullable || field.getType().getNullable());
      schemas.add(outputSchema);
    }

    return union(schemas);
  }

  private static Schema getOutputSchemaHelper(
      FieldType inputFieldType,
      FieldAccessDescriptor fieldAccessDescriptor,
      List<Qualifier> qualifiers,
      int qualifierPosition,
      boolean isNullable) {
    if (qualifierPosition >= qualifiers.size()) {
      // We have walked through any containers, and are at a row type. Extract the subschema
      // for the row, preserving nullable attributes.
      checkArgument(inputFieldType.getTypeName().isCompositeType());
      return getOutputSchemaTrackingNullable(
          inputFieldType.getRowSchema(), fieldAccessDescriptor, isNullable);
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    Schema.Builder builder = Schema.builder();
    switch (qualifier.getKind()) {
      case LIST:
        checkArgument(qualifier.getList().equals(ListQualifier.ALL));
        FieldType componentType = checkNotNull(inputFieldType.getCollectionElementType());
        Schema outputComponent =
            getOutputSchemaHelper(
                componentType, fieldAccessDescriptor, qualifiers, qualifierPosition + 1, false);
        for (Field field : outputComponent.getFields()) {
          Field newField;
          if (TypeName.ARRAY.equals(inputFieldType.getTypeName())) {
            newField = Field.of(field.getName(), FieldType.array(field.getType()));
          } else {
            checkArgument(TypeName.ITERABLE.equals(inputFieldType.getTypeName()));
            newField = Field.of(field.getName(), FieldType.iterable(field.getType()));
          }
          builder.addField(newField.withNullable(isNullable));
        }
        return builder.build();
      case MAP:
        checkArgument(qualifier.getMap().equals(MapQualifier.ALL));
        FieldType keyType = checkNotNull(inputFieldType.getMapKeyType());
        FieldType valueType = checkNotNull(inputFieldType.getMapValueType());
        Schema outputValueSchema =
            getOutputSchemaHelper(
                valueType, fieldAccessDescriptor, qualifiers, qualifierPosition + 1, false);
        for (Field field : outputValueSchema.getFields()) {
          Field newField = Field.of(field.getName(), FieldType.map(keyType, field.getType()));
          builder.addField(newField.withNullable(isNullable));
        }
        return builder.build();
      default:
        throw new RuntimeException("unexpected");
    }
  }

  public static RowSelector getRowSelectorOptimized(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    return SelectByteBuddyHelpers.getRowSelector(inputSchema, fieldAccessDescriptor);
  }

  @org.apache.beam.sdk.annotations.Internal
  public static RowSelector getRowSelectorOptimized(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor, Schema outputSchema) {
    return SelectByteBuddyHelpers.getRowSelector(inputSchema, fieldAccessDescriptor, outputSchema);
  }

  public static RowSelector getRowSelector(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    Schema outputSchema = getOutputSchema(inputSchema, fieldAccessDescriptor);
    return getRowSelector(inputSchema, fieldAccessDescriptor, outputSchema);
  }

  @org.apache.beam.sdk.annotations.Internal
  public static RowSelector getRowSelector(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor, Schema outputSchema) {
    return input -> selectRow(input, fieldAccessDescriptor, inputSchema, outputSchema);
  }

  public static class RowSelectorContainer implements RowSelector, Serializable {
    private transient volatile @Nullable RowSelector rowSelector;
    private final Schema inputSchema;
    private final FieldAccessDescriptor fieldAccessDescriptor;
    private final @Nullable Schema outputSchema;
    private final boolean optimized;

    public RowSelectorContainer(
        Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor, boolean optimized) {
      this(inputSchema, fieldAccessDescriptor, null, optimized);
    }

    @org.apache.beam.sdk.annotations.Internal
    public RowSelectorContainer(
        Schema inputSchema,
        FieldAccessDescriptor fieldAccessDescriptor,
        @Nullable Schema outputSchema,
        boolean optimized) {
      this.inputSchema = inputSchema;
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      this.outputSchema = outputSchema;
      this.optimized = optimized;
    }

    @org.apache.beam.sdk.annotations.Internal
    public RowSelector getOrCreateSelector() {
      RowSelector selector = this.rowSelector;
      if (selector == null) {
        if (outputSchema != null) {
          selector =
              optimized
                  ? getRowSelectorOptimized(inputSchema, fieldAccessDescriptor, outputSchema)
                  : getRowSelector(inputSchema, fieldAccessDescriptor, outputSchema);
        } else {
          selector =
              optimized
                  ? getRowSelectorOptimized(inputSchema, fieldAccessDescriptor)
                  : getRowSelector(inputSchema, fieldAccessDescriptor);
        }
        this.rowSelector = selector;
      }
      return selector;
    }

    @Override
    public Row select(Row input) {
      return getOrCreateSelector().select(input);
    }
  }

  /** Select a sub Row from an input Row. */
  @SuppressWarnings("ReferenceEquality")
  private static Row selectRow(
      Row input,
      FieldAccessDescriptor fieldAccessDescriptor,
      Schema inputSchema,
      Schema outputSchema) {
    if (fieldAccessDescriptor.getAllFields()) {
      if (input == null) {
        return Row.nullRow(outputSchema);
      }
      if (outputSchema == inputSchema || inputSchema.equals(outputSchema)) {
        return input;
      }
      return rewrapRow(input, outputSchema);
    }

    List<Object> outputValues = new ArrayList<>(outputSchema.getFieldCount());
    selectIntoList(inputSchema, input, outputValues, fieldAccessDescriptor);
    return Row.attachValues(outputSchema, outputValues);
  }

  static Row rewrapRow(Row input, Schema outputSchema) {
    if (input instanceof RowWithStorage) {
      @Nullable Object @Nullable [] raw = ((RowWithStorage) input).getRawArray();
      if (raw != null) {
        return Row.attachValues(outputSchema, raw);
      }
      return Row.attachValues(outputSchema, input.getValues());
    }
    int n = outputSchema.getFieldCount();
    Object[] values = new Object[n];
    for (int i = 0; i < n; i++) {
      values[i] = input.getValue(i);
    }
    return Row.attachValues(outputSchema, values);
  }

  /** Select out of a given {@link Row} object into an output value list. */
  private static void selectIntoList(
      Schema inputSchema,
      @Nullable Row input,
      List<Object> output,
      FieldAccessDescriptor fieldAccessDescriptor) {
    if (fieldAccessDescriptor.getAllFields()) {
      if (input == null) {
        for (int i = 0; i < inputSchema.getFieldCount(); i++) {
          output.add(null);
        }
      } else if (input instanceof RowWithGetters) {
        int count = inputSchema.getFieldCount();
        for (int i = 0; i < count; i++) {
          output.add(input.getValue(i));
        }
      } else {
        output.addAll(input.getValues());
      }
      return;
    }

    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      output.add((input != null) ? input.getValue(fieldId) : null);
    }

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor field = nested.getKey();
      FieldAccessDescriptor nestedAccess = nested.getValue();
      FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();
      selectIntoListWithQualifiers(
          field.getQualifiers(),
          0,
          (input != null) ? input.getValue(field.getFieldId()) : null,
          output,
          nestedAccess,
          nestedInputType);
    }
  }

  static int countFields(
      FieldType inputType,
      List<Qualifier> qualifiers,
      int qualifierPosition,
      FieldAccessDescriptor descriptor) {
    FieldType type = inputType;
    for (int i = qualifierPosition; i < qualifiers.size(); i++) {
      Qualifier q = qualifiers.get(i);
      switch (q.getKind()) {
        case LIST:
          type = checkNotNull(type.getCollectionElementType());
          break;
        case MAP:
          type = checkNotNull(type.getMapValueType());
          break;
        default:
          throw new RuntimeException("Unexpected type " + q.getKind());
      }
    }
    return countRowFields(checkNotNull(type.getRowSchema()), descriptor);
  }

  static int countRowFields(Schema schema, FieldAccessDescriptor descriptor) {
    if (descriptor.getAllFields()) {
      return schema.getFieldCount();
    }
    int count = descriptor.getFieldsAccessed().size();
    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        descriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor fd = nested.getKey();
      FieldType type = schema.getField(fd.getFieldId()).getType();
      count += countFields(type, fd.getQualifiers(), 0, nested.getValue());
    }
    return count;
  }

  private static void selectIntoListWithQualifiers(
      List<Qualifier> qualifiers,
      int qualifierPosition,
      @Nullable Object value,
      List<Object> output,
      FieldAccessDescriptor fieldAccessDescriptor,
      FieldType inputType) {
    if (qualifierPosition >= qualifiers.size()) {
      // We have already constructed all arrays and maps. What remains must be a Row.
      selectIntoList(inputType.getRowSchema(), (Row) value, output, fieldAccessDescriptor);
      return;
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    switch (qualifier.getKind()) {
      case LIST:
        {
          FieldType nestedInputType = checkNotNull(inputType.getCollectionElementType());
          Iterable<Object> iterable = (Iterable) value;
          int nestedFieldCount =
              countFields(
                  nestedInputType, qualifiers, qualifierPosition + 1, fieldAccessDescriptor);

          if (iterable == null) {
            for (int i = 0; i < nestedFieldCount; i++) {
              output.add(null);
            }
            break;
          }
          int initialCapacity =
              iterable instanceof Collection
                  ? ((Collection<?>) iterable).size()
                  : Iterables.size(iterable);
          List<List<Object>> selectedLists = Lists.newArrayListWithCapacity(nestedFieldCount);
          for (int i = 0; i < nestedFieldCount; i++) {
            selectedLists.add(new ArrayList<>(initialCapacity));
          }
          List<Object> scratch = new ArrayList<>(nestedFieldCount);
          for (Object o : iterable) {
            scratch.clear();
            selectIntoListWithQualifiers(
                qualifiers,
                qualifierPosition + 1,
                o,
                scratch,
                fieldAccessDescriptor,
                nestedInputType);
            for (int i = 0; i < nestedFieldCount; ++i) {
              selectedLists.get(i).add(scratch.get(i));
            }
          }
          output.addAll(selectedLists);
          break;
        }
      case MAP:
        {
          FieldType nestedInputType = checkNotNull(inputType.getMapValueType());
          int nestedFieldCount =
              countFields(
                  nestedInputType, qualifiers, qualifierPosition + 1, fieldAccessDescriptor);

          if (value == null) {
            for (int i = 0; i < nestedFieldCount; i++) {
              output.add(null);
            }
            break;
          }
          Map<Object, Object> map = (Map) value;
          List<Map<Object, Object>> selectedMaps = Lists.newArrayListWithCapacity(nestedFieldCount);
          for (int i = 0; i < nestedFieldCount; ++i) {
            selectedMaps.add(Maps.newHashMapWithExpectedSize(map.size()));
          }
          List<Object> scratch = new ArrayList<>(nestedFieldCount);
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            scratch.clear();
            selectIntoListWithQualifiers(
                qualifiers,
                qualifierPosition + 1,
                entry.getValue(),
                scratch,
                fieldAccessDescriptor,
                nestedInputType);
            for (int i = 0; i < nestedFieldCount; ++i) {
              selectedMaps.get(i).put(entry.getKey(), scratch.get(i));
            }
          }
          output.addAll(selectedMaps);
          break;
        }
      default:
        throw new RuntimeException("Unexpected type " + qualifier.getKind());
    }
  }

  /**
   * This policy keeps all levels of a name. Every field name in the path to a given field is
   * concated with _ characters.
   */
  public static final SerializableFunction<List<String>, String> CONCAT_FIELD_NAMES =
      l -> {
        return String.join("_", l);
      };

  /**
   * This policy keeps the raw nested field name. If two differently-nested fields have the same
   * name, flattening will fail with this policy.
   */
  public static final SerializableFunction<List<String>, String> KEEP_NESTED_NAME =
      l -> {
        return l.get(l.size() - 1);
      };

  public static FieldAccessDescriptor allLeavesDescriptor(
      Schema schema, SerializableFunction<List<String>, String> nameFn) {
    List<String> nameComponents = Lists.newArrayList();
    Map<String, String> fieldsSelected = Maps.newLinkedHashMap();
    allLeafFields(schema, nameComponents, nameFn, fieldsSelected);
    return FieldAccessDescriptor.withFieldNamesAs(fieldsSelected).resolve(schema);
  }

  private static void allLeafFields(
      Schema schema,
      List<String> nameComponents,
      SerializableFunction<List<String>, String> nameFn,
      Map<String, String> fieldsSelected) {
    for (Field field : schema.getFields()) {
      nameComponents.add(field.getName());

      FieldType fieldType = field.getType();
      FieldType collectionElementType = fieldType.getCollectionElementType();

      if (fieldType.getTypeName().isCompositeType()) {
        allLeafFields(fieldType.getRowSchema(), nameComponents, nameFn, fieldsSelected);
      } else if (collectionElementType != null
          && collectionElementType.getTypeName().isCompositeType()) {
        allLeafFields(collectionElementType.getRowSchema(), nameComponents, nameFn, fieldsSelected);
      } else {
        String newName = nameFn.apply(nameComponents);
        fieldsSelected.put(String.join(".", nameComponents), newName);
      }
      nameComponents.remove(nameComponents.size() - 1);
    }
  }
}

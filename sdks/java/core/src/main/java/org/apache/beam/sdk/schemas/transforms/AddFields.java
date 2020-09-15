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
package org.apache.beam.sdk.schemas.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimaps;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A transform to add new nullable fields to a PCollection's schema. Elements are extended to have
 * the new schema. By default new fields are nullable, and input rows will be extended to the new
 * schema by inserting null values. However explicit default values for new fields can be set using
 * {@link Inner#field(String, Schema.FieldType, Object)}. Nested fields can be added as well.
 *
 * <p>Example use:
 *
 * <pre>{@code PCollection<Event> events = readEvents();
 * PCollection<Row> augmentedEvents =
 *   events.apply(AddFields.<Event>create()
 *       .field("userId", FieldType.STRING)
 *       .field("location.zipcode", FieldType.INT32)
 *       .field("userDetails.isSpecialUser", "FieldType.BOOLEAN", false));
 * }</pre>
 */
@Experimental(Kind.SCHEMAS)
public class AddFields {
  public static <T> Inner<T> create() {
    return new Inner<>();
  }

  /** Inner PTransform for AddFields. */
  public static class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    /** Internal object representing a new field added. */
    @AutoValue
    abstract static class NewField implements Serializable {
      abstract String getName();

      abstract FieldAccessDescriptor getDescriptor();

      abstract Schema.FieldType getFieldType();

      abstract @Nullable Object getDefaultValue();

      @AutoValue.Builder
      abstract static class Builder {
        abstract Builder setName(String name);

        abstract Builder setDescriptor(FieldAccessDescriptor descriptor);

        abstract Builder setFieldType(Schema.FieldType fieldType);

        abstract Builder setDefaultValue(@Nullable Object defaultValue);

        abstract NewField build();
      }

      abstract Builder toBuilder();

      static NewField of(
          FieldAccessDescriptor fieldAccessDescriptor,
          Schema.FieldType fieldType,
          Object defaultValue) {
        return new AutoValue_AddFields_Inner_NewField.Builder()
            .setName(getName(fieldAccessDescriptor))
            .setDescriptor(fieldAccessDescriptor)
            .setFieldType(fieldType)
            .setDefaultValue(defaultValue)
            .build();
      }

      // If this field represents a nested value, pop the FieldAccessDescriptor one level down.
      NewField descend() {
        FieldAccessDescriptor descriptor =
            Iterables.getOnlyElement(getDescriptor().getNestedFieldsAccessed().values());
        return toBuilder().setDescriptor(descriptor).setName(getName(descriptor)).build();
      }

      static String getName(FieldAccessDescriptor descriptor) {
        if (!descriptor.getFieldsAccessed().isEmpty()) {
          return Iterables.getOnlyElement(descriptor.fieldNamesAccessed());
        } else {
          return Iterables.getOnlyElement(descriptor.nestedFieldsByName().keySet());
        }
      }

      FieldAccessDescriptor.FieldDescriptor getFieldDescriptor() {
        if (!getDescriptor().getFieldsAccessed().isEmpty()) {
          return Iterables.getOnlyElement(getDescriptor().getFieldsAccessed());
        } else {
          return Iterables.getOnlyElement(getDescriptor().getNestedFieldsAccessed().keySet());
        }
      }
    }

    /** This class encapsulates all data needed to add a a new field to the schema. */
    @AutoValue
    abstract static class AddFieldsInformation implements Serializable {
      // The new output fieldtype after adding the new field.

      abstract Schema.@Nullable FieldType getOutputFieldType();

      // A list of default values corresponding to this level of the schema.
      abstract List<Object> getDefaultValues();

      // A list of nested values. This list corresponds to the output schema fields, and is
      // populated for fields that
      // have new nested values. For other fields, the list contains a null value.
      abstract List<AddFieldsInformation> getNestedNewValues();

      @AutoValue.Builder
      abstract static class Builder {
        abstract AddFieldsInformation.Builder setOutputFieldType(Schema.FieldType outputFieldType);

        abstract AddFieldsInformation.Builder setDefaultValues(List<Object> defaultValues);

        abstract AddFieldsInformation.Builder setNestedNewValues(
            List<AddFieldsInformation> nestedNewValues);

        abstract AddFieldsInformation build();
      }

      abstract AddFieldsInformation.Builder toBuilder();

      static AddFieldsInformation of(
          Schema.FieldType outputFieldType,
          List<Object> defaultValues,
          List<AddFieldsInformation> nestedNewValues) {
        return new AutoValue_AddFields_Inner_AddFieldsInformation.Builder()
            .setOutputFieldType(outputFieldType)
            .setDefaultValues(defaultValues)
            .setNestedNewValues(nestedNewValues)
            .build();
      }
    }

    private final List<NewField> newFields;

    private Inner() {
      this.newFields = Collections.emptyList();
    }

    private Inner(List<NewField> newFields) {
      this.newFields = newFields;
    }

    /**
     * Add a new field of the specified type. The new field will be nullable and will be filled in
     * with null values.
     */
    public Inner<T> field(String fieldName, Schema.FieldType fieldType) {
      return field(fieldName, fieldType.withNullable(true), null);
    }

    /**
     * Add a new field of the specified type. The new field will be filled in with the specified
     * value.
     */
    public Inner<T> field(String fieldName, Schema.FieldType fieldType, Object defaultValue) {
      if (defaultValue == null) {
        checkArgument(fieldType.getNullable());
      }

      FieldAccessDescriptor descriptor = FieldAccessDescriptor.withFieldNames(fieldName);
      checkArgument(descriptor.referencesSingleField());
      List<NewField> fields =
          ImmutableList.<NewField>builder()
              .addAll(newFields)
              .add(NewField.of(descriptor, fieldType, defaultValue))
              .build();
      return new Inner<>(fields);
    }

    private static AddFieldsInformation getAddFieldsInformation(
        Schema inputSchema, Collection<NewField> fieldsToAdd) {
      List<NewField> newTopLevelFields =
          fieldsToAdd.stream()
              .filter(n -> !n.getDescriptor().getFieldsAccessed().isEmpty())
              .collect(Collectors.toList());
      List<NewField> newNestedFields =
          fieldsToAdd.stream()
              .filter(n -> !n.getDescriptor().getNestedFieldsAccessed().isEmpty())
              .collect(Collectors.toList());
      // Group all nested fields together by the field at the current level. For example, if adding
      // a.b, a.c, a.d
      // this map will contain a -> {a.b, a.c, a.d}.
      Multimap<String, NewField> newNestedFieldsMap =
          Multimaps.index(newNestedFields, NewField::getName);

      Map<Integer, AddFieldsInformation> resolvedNestedNewValues = Maps.newHashMap();
      Schema.Builder builder = Schema.builder();
      for (int i = 0; i < inputSchema.getFieldCount(); ++i) {
        Schema.Field field = inputSchema.getField(i);
        Collection<NewField> nestedFields = newNestedFieldsMap.get(field.getName());

        // If this field is a nested field and new subfields are added further down the tree, add
        // those subfields before
        // adding to the current schema. Otherwise we just add this field as is to the new schema.
        if (!nestedFields.isEmpty()) {
          nestedFields = nestedFields.stream().map(NewField::descend).collect(Collectors.toList());

          AddFieldsInformation nestedInformation =
              getAddFieldsInformation(field.getType(), nestedFields);
          field = field.withType(nestedInformation.getOutputFieldType());
          resolvedNestedNewValues.put(i, nestedInformation);
        }
        builder.addField(field);
      }

      // Add any new fields at this level.
      List<Object> newValuesThisLevel = new ArrayList<>(newTopLevelFields.size());
      for (NewField newField : newTopLevelFields) {
        builder.addField(newField.getName(), newField.getFieldType());
        newValuesThisLevel.add(newField.getDefaultValue());
      }

      // If there are any nested field additions left that are not already processed, that means
      // that the root of the
      // nested field doesn't exist in the schema. In this case we'll walk down the new nested
      // fields and recursively create each nested level as necessary.
      for (Map.Entry<String, Collection<NewField>> newNested :
          newNestedFieldsMap.asMap().entrySet()) {
        String fieldName = newNested.getKey();

        // If the user specifies the same nested field twice in different ways (e.g. a[].x, a{}.x)
        FieldAccessDescriptor.FieldDescriptor fieldDescriptor =
            Iterables.getOnlyElement(
                newNested.getValue().stream()
                    .map(NewField::getFieldDescriptor)
                    .distinct()
                    .collect(Collectors.toList()));
        FieldType fieldType = Schema.FieldType.row(Schema.of()).withNullable(true);
        for (Qualifier qualifier : fieldDescriptor.getQualifiers()) {
          // The problem with adding recursive map fields is that we don't know what the map key
          // type should be.
          // In a field descriptor of the form mapField{}.subField, the subField is assumed to be in
          // the map value.
          // Since in this code path the mapField field does not already exist this means we need to
          // create the new
          // map field, and we have no way of knowing what type the key should be.
          // Alternatives would be to always create a default key type (e.g. FieldType.STRING) or
          // extend our selector
          // syntax to allow specifying key types.
          checkArgument(
              !qualifier.getKind().equals(Qualifier.Kind.MAP), "Map qualifiers not supported here");
          fieldType = FieldType.array(fieldType).withNullable(true);
        }
        if (!inputSchema.hasField(fieldName)) {
          // This is a brand-new nested field with no matching field in the input schema. We will
          // recursively create a nested schema to match it.
          Collection<NewField> nestedNewFields =
              newNested.getValue().stream().map(NewField::descend).collect(Collectors.toList());
          AddFieldsInformation addFieldsInformation =
              getAddFieldsInformation(fieldType, nestedNewFields);
          builder.addField(fieldName, addFieldsInformation.getOutputFieldType());
          resolvedNestedNewValues.put(builder.getLastFieldId(), addFieldsInformation);
        }
      }
      Schema schema = builder.build();

      List<AddFieldsInformation> nestedNewValueList =
          new ArrayList<>(Collections.nCopies(schema.getFieldCount(), null));
      for (Map.Entry<Integer, AddFieldsInformation> entry : resolvedNestedNewValues.entrySet()) {
        nestedNewValueList.set(entry.getKey(), entry.getValue());
      }
      return AddFieldsInformation.of(
          Schema.FieldType.row(schema), newValuesThisLevel, nestedNewValueList);
    }

    private static AddFieldsInformation getAddFieldsInformation(
        Schema.FieldType inputFieldType, Collection<NewField> nestedFields) {
      AddFieldsInformation addFieldsInformation;
      Schema.FieldType fieldType;
      switch (inputFieldType.getTypeName()) {
        case ROW:
          addFieldsInformation =
              getAddFieldsInformation(inputFieldType.getRowSchema(), nestedFields);
          fieldType = addFieldsInformation.getOutputFieldType();
          break;

        case ARRAY:
          addFieldsInformation =
              getAddFieldsInformation(inputFieldType.getCollectionElementType(), nestedFields);
          fieldType = Schema.FieldType.array(addFieldsInformation.getOutputFieldType());
          break;

        case ITERABLE:
          addFieldsInformation =
              getAddFieldsInformation(inputFieldType.getCollectionElementType(), nestedFields);
          fieldType = Schema.FieldType.iterable(addFieldsInformation.getOutputFieldType());
          break;

        case MAP:
          addFieldsInformation =
              getAddFieldsInformation(inputFieldType.getMapValueType(), nestedFields);
          fieldType =
              Schema.FieldType.map(
                  inputFieldType.getMapKeyType(), addFieldsInformation.getOutputFieldType());
          break;

        default:
          throw new RuntimeException("Cannot select a subfield of a non-composite type.");
      }
      fieldType = fieldType.withNullable(inputFieldType.getNullable());
      return addFieldsInformation.toBuilder().setOutputFieldType(fieldType).build();
    }

    private static Row fillNewFields(Row row, AddFieldsInformation addFieldsInformation) {
      Schema outputSchema = checkNotNull(addFieldsInformation.getOutputFieldType().getRowSchema());

      List<Object> newValues = Lists.newArrayListWithCapacity(outputSchema.getFieldCount());
      for (int i = 0; i < row.getFieldCount(); ++i) {
        AddFieldsInformation nested = addFieldsInformation.getNestedNewValues().get(i);
        if (nested != null) {
          // New fields were added to nested subfields of this value. Recursively fill them out
          // before adding to the new row.
          Object newValue = fillNewFields(row.getValue(i), nested.getOutputFieldType(), nested);
          newValues.add(newValue);
        } else {
          // Nothing changed. Just copy the old value into the new row.
          newValues.add(row.getValue(i));
        }
      }
      // If there are brand new simple (i.e. have no nested values) fields at this level, then add
      // the default values for all of them.
      newValues.addAll(addFieldsInformation.getDefaultValues());
      // If we are creating new recursive fields, populate new values for them here.
      for (int i = newValues.size(); i < addFieldsInformation.getNestedNewValues().size(); ++i) {
        AddFieldsInformation newNestedField = addFieldsInformation.getNestedNewValues().get(i);
        if (newNestedField != null) {
          newValues.add(fillNewFields(null, newNestedField.getOutputFieldType(), newNestedField));
        }
      }

      return Row.withSchema(outputSchema).attachValues(newValues);
    }

    private static Object fillNewFields(
        Object original, Schema.FieldType fieldType, AddFieldsInformation addFieldsInformation) {
      switch (fieldType.getTypeName()) {
        case ROW:
          if (original == null) {
            original = Row.withSchema(fieldType.getRowSchema()).build();
          }
          return fillNewFields((Row) original, addFieldsInformation);

        case ARRAY:
        case ITERABLE:
          if (original == null) {
            return Collections.emptyList();
          }
          Iterable<Object> iterable = (Iterable<Object>) original;
          List<Object> filledList = new ArrayList<>(Iterables.size(iterable));
          Schema.FieldType elementType = fieldType.getCollectionElementType();
          AddFieldsInformation elementAddFieldInformation =
              addFieldsInformation.toBuilder().setOutputFieldType(elementType).build();
          for (Object element : iterable) {
            filledList.add(fillNewFields(element, elementType, elementAddFieldInformation));
          }
          return filledList;

        case MAP:
          if (original == null) {
            return Collections.emptyMap();
          }
          Map<Object, Object> originalMap = (Map<Object, Object>) original;
          Map<Object, Object> filledMap = Maps.newHashMapWithExpectedSize(originalMap.size());
          Schema.FieldType mapValueType = fieldType.getMapValueType();
          AddFieldsInformation mapValueAddFieldInformation =
              addFieldsInformation.toBuilder().setOutputFieldType(mapValueType).build();
          for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
            filledMap.put(
                entry.getKey(),
                fillNewFields(entry.getValue(), mapValueType, mapValueAddFieldInformation));
          }
          return filledMap;

        default:
          throw new RuntimeException("Unexpected field type");
      }
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      final AddFieldsInformation addFieldsInformation =
          getAddFieldsInformation(input.getSchema(), newFields);
      Schema outputSchema = checkNotNull(addFieldsInformation.getOutputFieldType().getRowSchema());

      return input
          .apply(
              ParDo.of(
                  new DoFn<T, Row>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> o) {
                      o.output(fillNewFields(row, addFieldsInformation));
                    }
                  }))
          .setRowSchema(outputSchema);
    }
  }
}

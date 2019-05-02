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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Multimaps;

/**
 * A transform to add new nullable fields to a PCollection's schema. Elements are extended to have
 * the new schema, with null values used for the new fields. Any new fields added must be nullable.
 *
 * <p>Example use:
 *
 * <pre>{@code PCollection<Event> events = readEvents();
 * PCollection<Row> augmentedEvents =
 *   events.apply(AddFields.fields(Field.nullable("newField1", FieldType.STRING),
 *                                 Field.nullable("newField2", FieldType.INT64)));
 * }</pre>
 */
public class AddFields {
  public static <T> Inner<T> create() {
    return new Inner<>();
  }

  /** Inner PTransform for AddFields. */
  public static class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    @AutoValue
    abstract static class NewField implements Serializable {
      abstract String getName();

      abstract FieldAccessDescriptor getDescriptor();

      abstract Schema.FieldType getFieldType();

      @Nullable
      abstract Object getDefaultValue();

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
    }

    @AutoValue
    abstract static class AddFieldsInformation implements Serializable {
      @Nullable
      abstract Schema.FieldType getOutputFieldType();

      abstract List<Object> getNewValues();

      abstract List<AddFieldsInformation> getNestedNewValues();

      @AutoValue.Builder
      abstract static class Builder {
        abstract AddFieldsInformation.Builder setOutputFieldType(Schema.FieldType outputFieldType);

        abstract AddFieldsInformation.Builder setNewValues(List<Object> newValues);

        abstract AddFieldsInformation.Builder setNestedNewValues(List<AddFieldsInformation> nestedNewValues);

        abstract AddFieldsInformation build();
      }

      abstract AddFieldsInformation.Builder toBuilder();

      static AddFieldsInformation of(
          Schema.FieldType outputFieldType,
          List<Object> newValues,
          List<AddFieldsInformation> nestedNewValues) {
        return new AutoValue_AddFields_Inner_AddFieldsInformation.Builder()
            .setOutputFieldType(outputFieldType)
            .setNewValues(newValues)
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

    public Inner<T> field(String fieldName, Schema.FieldType fieldType) {
      return field(fieldName, fieldType.withNullable(true), null);
    }

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

    private AddFieldsInformation getAddFieldsInformation(
        Schema inputSchema, Collection<NewField> fieldsToAdd) {
      List<NewField> newTopLevelFields =
          fieldsToAdd.stream()
              .filter(n -> !n.getDescriptor().getFieldsAccessed().isEmpty())
              .collect(Collectors.toList());
      List<NewField> newNestedFields =
          fieldsToAdd.stream()
              .filter(n -> !n.getDescriptor().getNestedFieldsAccessed().isEmpty())
              .collect(Collectors.toList());

      Multimap<String, NewField> newNestedFieldsMap =
          Multimaps.index(newNestedFields, NewField::getName);

      Map<Integer, AddFieldsInformation> nestedNewValues = Maps.newHashMap();
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
          nestedNewValues.put(i, nestedInformation);
        }
        builder.addField(field);
      }

      // Add any new fields at this level.
      List<Object> newValues = new ArrayList<>(newTopLevelFields.size());
      for (NewField newField : newTopLevelFields) {
        builder.addField(newField.getName(), newField.getFieldType());
        newValues.add(newField.getDefaultValue());
      }

      // If there are any nested field additions left that are not already processed, that means
      // that the root of the
      // nested field doesn't exist in the schema. In this case we'll walk down the new nested
      // fields and recursively
      // create each nested level as necessary.
      for (Map.Entry<String, Collection<NewField>> newNested :
          newNestedFieldsMap.asMap().entrySet()) {
        if (!inputSchema.hasField(newNested.getKey())) {
          // This is a brand-new nested field with no matching field in the input schema. We will
          // recursively create
          // a nested schema to match it.
          Collection<NewField> nestedNewFields =
              newNested.getValue().stream().map(NewField::descend).collect(Collectors.toList());
          AddFieldsInformation addFieldsInformation =
              getAddFieldsInformation(
                  Schema.FieldType.row(Schema.of()).withNullable(true), nestedNewFields);
          builder.addField(newNested.getKey(), addFieldsInformation.getOutputFieldType());
          nestedNewValues.put(builder.getLastFieldId(), addFieldsInformation);
        }
      }

      Schema schema = builder.build();
      List<AddFieldsInformation> nestedNewValueList =
              new ArrayList<>(Collections.nCopies(schema.getFieldCount(), null));
      for (Map.Entry<Integer, AddFieldsInformation> entry : nestedNewValues.entrySet()) {
        nestedNewValueList.set(entry.getKey(), entry.getValue());
      }
      return AddFieldsInformation.of(
          Schema.FieldType.row(schema), newValues, nestedNewValueList);
    }

    AddFieldsInformation getAddFieldsInformation(
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

        case MAP:
          addFieldsInformation =
              getAddFieldsInformation(inputFieldType.getMapValueType(), nestedFields);
          fieldType = Schema.FieldType.map(inputFieldType.getMapKeyType(), addFieldsInformation.getOutputFieldType());
          break;

        default:
          throw new RuntimeException("Cannot select a subfield of a non-composite type.");
      }
      fieldType = fieldType.withNullable(inputFieldType.getNullable());
      return addFieldsInformation.toBuilder().setOutputFieldType(fieldType).build();
    }

    Row fillNewFields(Row row, AddFieldsInformation addFieldsInformation) {
      Schema outputSchema = checkNotNull(addFieldsInformation.getOutputFieldType().getRowSchema());

      List<Object> newValues = Lists.newArrayListWithCapacity(outputSchema.getFieldCount());
      for (int i = 0; i < row.getFieldCount(); ++i) {
        AddFieldsInformation nested = addFieldsInformation.getNestedNewValues().get(i);
        if (nested != null) {
          Object newValue =
              fillNewFields(row.getValue(i), row.getSchema().getField(i).getType(), nested);
          newValues.add(newValue);
        } else {
          newValues.add(row.getValue(i));
        }
      }
      newValues.addAll(addFieldsInformation.getNewValues());
      for (int i = row.getFieldCount(); i < addFieldsInformation.getNestedNewValues().size(); ++i) {
        AddFieldsInformation newNestedField = addFieldsInformation.getNestedNewValues().get(i);
        if (newNestedField != null) {
          newValues.add(fillNewFields(null, addFieldsInformation.getOutputFieldType(), newNestedField));
        }
      }

      return Row.withSchema(outputSchema).attachValues(newValues).build();
    }

    Object fillNewFields(
        Object original, Schema.FieldType fieldType, AddFieldsInformation addFieldsInformation) {
      switch (fieldType.getTypeName()) {
        case ROW:
          if (original == null) {
            original = Row.withSchema(fieldType.getRowSchema()).build();
          }
          return fillNewFields((Row) original, addFieldsInformation);

        case ARRAY:
          if (original == null) {
            return Collections.emptyList();
          }
          List<Object> list = (List<Object>) original;
          List<Object> filledList = new ArrayList<>(list.size());
          for (Object element : list) {
            filledList.add(
                fillNewFields(element, fieldType.getCollectionElementType(), addFieldsInformation));
          }
          return filledList;

        case MAP:
          if (original == null) {
            return Collections.emptyMap();
          }
          Map<Object, Object> originalMap = (Map<Object, Object>) original;
          Map<Object, Object> filledMap = Maps.newHashMapWithExpectedSize(originalMap.size());
          for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
            filledMap.put(
                entry.getKey(),
                fillNewFields(entry.getValue(), fieldType.getMapValueType(), addFieldsInformation));
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

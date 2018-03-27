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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.values.Row;

/**
 * {@link Schema} describes the fields in {@link Row}.
 *
 */
@Experimental
@AutoValue
public abstract class Schema implements Serializable {
  // A mapping between field names an indices.
  private BiMap<String, Integer> fieldIndices = HashBiMap.create();
  public abstract List<Field> getFields();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFields(List<Field> fields);
    abstract Schema build();
  }

  public static Schema of(List<Field> fields) {
    return Schema.fromFields(fields);
  }

  public static Schema of(Field ... fields) {
    return Schema.of(Arrays.asList(fields));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Schema)) {
      return false;
    }
    Schema other = (Schema) o;
    return Objects.equals(fieldIndices, other.fieldIndices)
        && Objects.equals(getFields(), other.getFields());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldIndices, getFields());
  }

  /**
   * An enumerated list of supported types.
   */
  public enum FieldType {
    BYTE,    // One-byte signed integer.
    INT16,   // two-byte signed integer.
    INT32,   // four-byte signed integer.
    INT64,   // eight-byte signed integer.
    DECIMAL,  // Decimal integer
    FLOAT,
    DOUBLE,
    STRING,  // String.
    DATETIME, // Date and time.
    BOOLEAN,  // Boolean.
    ARRAY,
    ROW;    // The field is itself a nested row.

    public static final List<FieldType> NUMERIC_TYPES = ImmutableList.of(
        BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE);
    public static final List<FieldType> STRING_TYPES = ImmutableList.of(STRING);
    public static final List<FieldType> DATE_TYPES = ImmutableList.of(DATETIME);
    public static final List<FieldType> CONTAINER_TYPES = ImmutableList.of(ARRAY);
    public static final List<FieldType> COMPOSITE_TYPES = ImmutableList.of(ROW);

    public boolean isNumericType() {
      return NUMERIC_TYPES.contains(this);
    }
    public boolean isStringType() {
      return STRING_TYPES.contains(this);
    }
    public boolean isDateType() {
      return DATE_TYPES.contains(this);
    }
    public boolean isContainerType() {
      return CONTAINER_TYPES.contains(this);
    }
    public boolean isCompositeType() {
      return COMPOSITE_TYPES.contains(this);
    }
  }

  /**
   * A descriptor of a single field type. This is a recursive descriptor, as nested types are
   * allowed.
   */
  @AutoValue
  public abstract static class FieldTypeDescriptor implements Serializable {
    // Returns the type of this field.
    public abstract FieldType getType();
    // For container types (e.g. ARRAY), returns the type of the contained element.
    @Nullable public abstract FieldTypeDescriptor getComponentType();
    // For ROW types, returns the schema for the row.
    @Nullable public abstract Schema getRowSchema();
    /**
     * Returns optional extra metadata.
     */
    @Nullable public abstract byte[] getMetadata();
    abstract FieldTypeDescriptor.Builder toBuilder();
    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setType(FieldType fieldType);
      abstract Builder setComponentType(@Nullable FieldTypeDescriptor componentType);
      abstract Builder setRowSchema(@Nullable Schema rowSchema);
      abstract Builder setMetadata(@Nullable byte[] metadata);
      abstract FieldTypeDescriptor build();
    }

    /**
     * Create a {@link FieldTypeDescriptor} for the given type.
     */
    public static FieldTypeDescriptor of(FieldType fieldType) {
      return new AutoValue_Schema_FieldTypeDescriptor.Builder().setType(fieldType).build();
    }

    /**
     * For container types, adds the type of the component element.
     */
    public FieldTypeDescriptor withComponentType(@Nullable FieldTypeDescriptor componentType) {
      if (componentType != null) {
        checkArgument(getType().isContainerType());
      }
      return toBuilder().setComponentType(componentType).build();
    }

    /**
     * For ROW types, sets the schema of the row.
     */
    public FieldTypeDescriptor withRowSchema(@Nullable Schema rowSchema) {
      if (rowSchema != null) {
        checkArgument(getType().isCompositeType());
      }
      return toBuilder().setRowSchema(rowSchema).build();
    }

    /**
     * Returns a copy of the descriptor with metadata sert set.
     */
    public FieldTypeDescriptor withMetadata(@Nullable byte[] metadata) {
      return toBuilder().setMetadata(metadata).build();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FieldTypeDescriptor)) {
        return false;
      }
      FieldTypeDescriptor other = (FieldTypeDescriptor) o;
      return Objects.equals(getType(), other.getType())
          && Objects.equals(getComponentType(), other.getComponentType())
          && Objects.equals(getRowSchema(), other.getRowSchema())
          && Arrays.equals(getMetadata(), other.getMetadata());

    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(
          new Object[] {getType(), getComponentType(), getRowSchema(), getMetadata()});
    }
  }


  /**
   * Field of a row. Contains the {@link FieldTypeDescriptor} along with associated metadata.
   *
   */
  @AutoValue
  public abstract static class Field implements Serializable {
    /**
     * Returns the field name.
     */
    public abstract String getName();

    /**
     * Returns the field's description.
     */
    public abstract String getDescription();

    /**
     * Returns the fields {@link FieldTypeDescriptor}.
     */
    public abstract FieldTypeDescriptor getTypeDescriptor();

    /**
     * Returns whether the field supports null values.
     */
    public abstract Boolean getNullable();


    public abstract Builder toBuilder();
    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setName(String name);
      abstract Builder setDescription(String description);
      abstract Builder setTypeDescriptor(FieldTypeDescriptor fieldTypeDescriptor);
      abstract Builder setNullable(Boolean nullable);
      abstract Field build();
    }

    /**
     * Return's a field with the give name.
     */
    public static Field of(String name, FieldTypeDescriptor fieldTypeDescriptor) {
      return new AutoValue_Schema_Field.Builder()
          .setName(name)
          .setDescription("")
          .setTypeDescriptor(fieldTypeDescriptor)
          .setNullable(false)  // By default fields are not nullable.
          .build();
    }

    /**
     * Returns a copy of the Field with the name set.
     */
    public Field withName(String name) {
      return toBuilder().setName(name).build();

    }

    /**
     * Returns a copy of the Field with the description set.
     */
    public Field withDescription(String description) {
      return toBuilder().setDescription(description).build();
    }

    /**
     * Returns a copy of the Field with the {@link org.apache.beam.sdk.values.TypeDescriptor} set.
     */
    public Field withTypeDescriptor(FieldTypeDescriptor fieldTypeDescriptor) {
      return toBuilder().setTypeDescriptor(fieldTypeDescriptor).build();
    }

    /**
     * Returns a copy of the Field with isNullable set.
     */
    public Field withNullable(boolean isNullable) {
      return toBuilder().setNullable(isNullable).build();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Field)) {
        return false;
      }
      Field other = (Field) o;
      return Objects.equals(getName(), other.getName())
          && Objects.equals(getDescription(), other.getDescription())
          && Objects.equals(getTypeDescriptor(), other.getTypeDescriptor())
          && Objects.equals(getNullable(), other.getNullable());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getDescription(), getTypeDescriptor(), getNullable());
    }
  }

  /**
   * Collects a stream of {@link Field}s into a {@link Schema}.
   */
  public static Collector<Field, List<Field>, Schema> toSchema() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        },
        Schema::fromFields);
  }

  private static Schema fromFields(List<Field> fields) {
    Schema schema = new AutoValue_Schema.Builder().setFields(fields).build();
    int index = 0;
    for (Field field : fields) {
      schema.fieldIndices.put(field.getName(), index++);
    }
    return schema;
  }


  /**
   * Return the coder for a {@link Row} with this schema.
   */
  public RowCoder getRowCoder() {
    return RowCoder.of(this);
  }

  /**
   * Return the list of all field names.
   */
  public List<String> getFieldNames() {
    return getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
  }

  /**
   * Return a field by index.
   */
  public Field getField(int index) {
    return getFields().get(index);
  }

  public Field getField(String name) {
    return getFields().get(indexOf(name));
  }

  /**
   * Find the index of a given field.
   */
  public int indexOf(String fieldName) {
    Integer index = fieldIndices.get(fieldName);
    if (index == null) {
      throw new IllegalArgumentException(String.format("Cannot find field %s", fieldName));
    }
    return index;
  }

  /**
   * Return the name of field by index.
   */
  public String nameOf(int fieldIndex) {
    String name = fieldIndices.inverse().get(fieldIndex);
    if (name == null) {
      throw new IllegalArgumentException(String.format("Cannot find field %d", fieldIndex));
    }
    return name;
  }

  /**
   * Return the count of fields.
   */
  public int getFieldCount() {
    return getFields().size();
  }
}

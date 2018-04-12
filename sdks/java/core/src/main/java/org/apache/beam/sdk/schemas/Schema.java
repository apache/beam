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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
public class Schema implements Serializable {
  // A mapping between field names an indices.
  private BiMap<String, Integer> fieldIndices = HashBiMap.create();
  private List<Field> fields;

  /**
   * Builder class for building {@link Schema} objects.
   */
  public static class Builder {
    List<Field> fields;

    public Builder() {
      this.fields = Lists.newArrayList();
    }

    public Builder addFields(List<Field> fields) {
      this.fields.addAll(fields);
      return this;
    }

    public Builder addFields(Field... fields) {
      return addFields(Arrays.asList(fields));
    }

    public Builder addField(Field field) {
      fields.add(field);
      return this;
    }

    public Builder addByteField(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.BYTE.type()).withNullable(nullable));
      return this;
    }

    public Builder addInt16Field(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.INT16.type()).withNullable(nullable));
      return this;
    }

    public Builder addInt32Field(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.INT32.type()).withNullable(nullable));
      return this;
    }

    public Builder addInt64Field(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.INT64.type()).withNullable(nullable));
      return this;
    }

    public Builder addDecimalField(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.DECIMAL.type()).withNullable(nullable));
      return this;
    }

    public Builder addFloatField(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.FLOAT.type()).withNullable(nullable));
      return this;
    }

    public Builder addDoubleField(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.DOUBLE.type()).withNullable(nullable));
      return this;
    }

    public Builder addStringField(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.STRING.type()).withNullable(nullable));
      return this;
    }

    public Builder addDateTimeField(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.DATETIME.type()).withNullable(nullable));
      return this;
    }

    public Builder addBooleanField(String name, boolean nullable) {
      fields.add(Field.of(name, TypeName.BOOLEAN.type()).withNullable(nullable));
      return this;
    }

    public Builder addArrayField(String name, FieldType componentType) {
      fields.add(Field.of(name, TypeName.ARRAY.type().withComponentType(componentType)));
      return this;
    }

    public Builder addRowField(String name, Schema fieldSchema, boolean nullable) {
      fields.add(Field.of(name, TypeName.ROW.type().withRowSchema(fieldSchema))
          .withNullable(nullable));
      return this;
    }

    public Schema build() {
      return new Schema(fields);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public Schema(List<Field> fields) {
    this.fields = fields;
    int index = 0;
    for (Field field :fields) {
      fieldIndices.put(field.getName(), index++);
    }
  }

  public static Schema of(Field ... fields) {
    return Schema.builder().addFields(fields).build();
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
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Fields:\n");
    for (Field field : fields) {
      builder.append(field);
      builder.append("\n");
    }
    return builder.toString();
  };

  @Override
  public int hashCode() {
    return Objects.hash(fieldIndices, getFields());
  }

  public List<Field> getFields() {
    return fields;
  }

  /**
   * An enumerated list of supported types.
   */
  public enum TypeName {
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
    MAP,
    ROW;    // The field is itself a nested row.

    private final FieldType fieldType = FieldType.of(this);

    public static final Set<TypeName> NUMERIC_TYPES = ImmutableSet.of(
        BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE);
    public static final Set<TypeName> STRING_TYPES = ImmutableSet.of(STRING);
    public static final Set<TypeName> DATE_TYPES = ImmutableSet.of(DATETIME);
    public static final Set<TypeName> CONTAINER_TYPES = ImmutableSet.of(ARRAY);
    public static final Set<TypeName> MAP_TYPES = ImmutableSet.of(MAP);
    public static final Set<TypeName> COMPOSITE_TYPES = ImmutableSet.of(ROW);

    public boolean isPrimitiveType() {
      return isNumericType() || isStringType() || isDateType();
    }
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
    public boolean isMapType() {
      return MAP_TYPES.contains(this);
    }
    public boolean isCompositeType() {
      return COMPOSITE_TYPES.contains(this);
    }

    /** Returns a {@link FieldType} representing this primitive type. */
    public FieldType type() {
      return fieldType;
    }
  }

  /**
   * A descriptor of a single field type. This is a recursive descriptor, as nested types are
   * allowed.
   */
  @AutoValue
  public abstract static class FieldType implements Serializable {
    // Returns the type of this field.
    public abstract TypeName getTypeName();
    // For container types (e.g. ARRAY), returns the type of the contained element.
    @Nullable public abstract FieldType getComponentType();
    // For MAP type, returns the type of the key element, it must be a primitive type;
    @Nullable public abstract TypeName getMapKeyType();
    // For MAP type, returns the type of the value element, it can be a nested type;
    @Nullable public abstract FieldType getMapValueType();
    // For ROW types, returns the schema for the row.
    @Nullable public abstract Schema getRowSchema();
    /**
     * Returns optional extra metadata.
     */
    @Nullable public abstract byte[] getMetadata();
    abstract FieldType.Builder toBuilder();
    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTypeName(TypeName typeName);
      abstract Builder setComponentType(@Nullable FieldType componentType);
      abstract Builder setMapKeyType(@Nullable TypeName mapKeyType);
      abstract Builder setMapValueType(@Nullable FieldType mapValueType);
      abstract Builder setRowSchema(@Nullable Schema rowSchema);
      abstract Builder setMetadata(@Nullable byte[] metadata);
      abstract FieldType build();
    }

    /**
     * Create a {@link FieldType} for the given type.
     */
    public static FieldType of(TypeName typeName) {
      return new AutoValue_Schema_FieldType.Builder().setTypeName(typeName).build();
    }

    /**
     * For container types, adds the type of the component element.
     */
    public FieldType withComponentType(@Nullable FieldType componentType) {
      if (componentType != null) {
        checkArgument(getTypeName().isContainerType());
      }
      return toBuilder().setComponentType(componentType).build();
    }

    /**
     * For MAP type, adds the type of the component key/value element.
     */
    public FieldType withMapType(@Nullable TypeName mapKeyType,
        @Nullable FieldType mapValueType) {
      if (mapKeyType != null && mapValueType != null) {
        checkArgument(getTypeName().isMapType());
        checkArgument(mapKeyType.isPrimitiveType());
      }
      return toBuilder().setMapKeyType(mapKeyType)
          .setMapValueType(mapValueType).build();
    }

    /**
     * For ROW types, sets the schema of the row.
     */
    public FieldType withRowSchema(@Nullable Schema rowSchema) {
      if (rowSchema != null) {
        checkArgument(getTypeName().isCompositeType());
      }
      return toBuilder().setRowSchema(rowSchema).build();
    }

    /**
     * Returns a copy of the descriptor with metadata  set.
     */
    public FieldType withMetadata(@Nullable byte[] metadata) {
      return toBuilder().setMetadata(metadata).build();
    }

    /**
     * Returns a copy of the descriptor with metadata  set.
     */
    public FieldType withMetadata(String metadata) {
      return toBuilder().setMetadata(metadata.getBytes(StandardCharsets.UTF_8)).build();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FieldType)) {
        return false;
      }
      FieldType other = (FieldType) o;
      return Objects.equals(getTypeName(), other.getTypeName())
          && Objects.equals(getComponentType(), other.getComponentType())
          && Objects.equals(getMapKeyType(), other.getMapKeyType())
          && Objects.equals(getMapValueType(), other.getMapValueType())
          && Objects.equals(getRowSchema(), other.getRowSchema())
          && Arrays.equals(getMetadata(), other.getMetadata());

    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(new Object[] { getTypeName(), getComponentType(),
          getMapKeyType(), getMapValueType(), getRowSchema(), getMetadata() });
    }
  }


  /**
   * Field of a row. Contains the {@link FieldType} along with associated metadata.
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
     * Returns the fields {@link FieldType}.
     */
    public abstract FieldType getType();

    /**
     * Returns whether the field supports null values.
     */
    public abstract Boolean getNullable();


    public abstract Builder toBuilder();
    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setName(String name);
      abstract Builder setDescription(String description);
      abstract Builder setType(FieldType fieldType);
      abstract Builder setNullable(Boolean nullable);
      abstract Field build();
    }

    /**
     * Return's a field with the give name.
     */
    public static Field of(String name, FieldType fieldType) {
      return new AutoValue_Schema_Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType)
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
     * Returns a copy of the Field with the {@link FieldType} set.
     */
    public Field withType(FieldType fieldType) {
      return toBuilder().setType(fieldType).build();
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
          && Objects.equals(getType(), other.getType())
          && Objects.equals(getNullable(), other.getNullable());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getDescription(), getType(), getNullable());
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
    return new Schema(fields);
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

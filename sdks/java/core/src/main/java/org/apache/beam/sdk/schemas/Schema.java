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

import com.google.auto.value.AutoValue;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.values.Row;

/** {@link Schema} describes the fields in {@link Row}. */
@Experimental(Kind.SCHEMAS)
public class Schema implements Serializable {
  // A mapping between field names an indices.
  private final BiMap<String, Integer> fieldIndices = HashBiMap.create();
  private final List<Field> fields;
  // Cache the hashCode, so it doesn't have to be recomputed. Schema objects are immutable, so this
  // is correct.
  private final int hashCode;
  // Every SchemaCoder has a UUID. The schemas created with the same UUID are guaranteed to be
  // equal, so we can short circuit comparison.
  @Nullable private UUID uuid = null;

  /** Builder class for building {@link Schema} objects. */
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

    public Builder addField(String name, FieldType type) {
      fields.add(Field.of(name, type));
      return this;
    }

    public Builder addNullableField(String name, FieldType type) {
      fields.add(Field.nullable(name, type));
      return this;
    }

    public Builder addByteField(String name) {
      fields.add(Field.of(name, FieldType.BYTE));
      return this;
    }

    public Builder addByteArrayField(String name) {
      fields.add(Field.of(name, FieldType.BYTES));
      return this;
    }

    public Builder addInt16Field(String name) {
      fields.add(Field.of(name, FieldType.INT16));
      return this;
    }

    public Builder addInt32Field(String name) {
      fields.add(Field.of(name, FieldType.INT32));
      return this;
    }

    public Builder addInt64Field(String name) {
      fields.add(Field.of(name, FieldType.INT64));
      return this;
    }

    public Builder addDecimalField(String name) {
      fields.add(Field.of(name, FieldType.DECIMAL));
      return this;
    }

    public Builder addFloatField(String name) {
      fields.add(Field.of(name, FieldType.FLOAT));
      return this;
    }

    public Builder addDoubleField(String name) {
      fields.add(Field.of(name, FieldType.DOUBLE));
      return this;
    }

    public Builder addStringField(String name) {
      fields.add(Field.of(name, FieldType.STRING));
      return this;
    }

    public Builder addDateTimeField(String name) {
      fields.add(Field.of(name, FieldType.DATETIME));
      return this;
    }

    public Builder addBooleanField(String name) {
      fields.add(Field.of(name, FieldType.BOOLEAN));
      return this;
    }

    public Builder addArrayField(String name, FieldType collectionElementType) {
      fields.add(Field.of(name, FieldType.array(collectionElementType)));
      return this;
    }

    public Builder addRowField(String name, Schema fieldSchema) {
      fields.add(Field.of(name, FieldType.row(fieldSchema)));
      return this;
    }

    public Builder addMapField(String name, FieldType keyType, FieldType valueType) {
      fields.add(Field.of(name, FieldType.map(keyType, valueType)));
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
    for (Field field : fields) {
      if (fieldIndices.get(field.getName()) != null) {
        throw new IllegalArgumentException(
            "Duplicate field " + field.getName() + " added to schema");
      }
      fieldIndices.put(field.getName(), index++);
    }
    this.hashCode = Objects.hash(fieldIndices, fields);
  }

  public static Schema of(Field... fields) {
    return Schema.builder().addFields(fields).build();
  }

  /** Set this schema's UUID. All schemas with the same UUID must be guaranteed to be identical. */
  public void setUUID(UUID uuid) {
    this.uuid = uuid;
  }

  /** Get this schema's UUID. */
  @Nullable
  public UUID getUUID() {
    return this.uuid;
  }

  /** Returns true if two Schemas have the same fields in the same order. */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Schema other = (Schema) o;
    // If both schemas have a UUID set, we can simply compare the UUIDs.
    if (uuid != null && other.uuid != null) {
      return Objects.equals(uuid, other.uuid);
    }
    return Objects.equals(fieldIndices, other.fieldIndices)
        && Objects.equals(getFields(), other.getFields());
  }

  /** Returns true if two schemas are equal ignoring field names and descriptions. */
  public boolean typesEqual(Schema other) {
    if (uuid != null && other.uuid != null && Objects.equals(uuid, other.uuid)) {
      return true;
    }
    if (getFieldCount() != other.getFieldCount()) {
      return false;
    }
    if (!Objects.equals(fieldIndices.values(), other.fieldIndices.values())) {
      return false;
    }
    for (int i = 0; i < getFieldCount(); ++i) {
      if (!getField(i).typesEqual(other.getField(i))) {
        return false;
      }
    }
    return true;
  }

  enum EquivalenceNullablePolicy {
    SAME,
    WEAKEN,
    IGNORE
  };

  /** Returns true if two Schemas have the same fields, but possibly in different orders. */
  public boolean equivalent(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.SAME);
  }

  /** Returns true if this Schema can be assigned to another Schema. * */
  public boolean assignableTo(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.WEAKEN);
  }

  /** Returns true if this Schema can be assigned to another Schema, igmoring nullable. * */
  public boolean assignableToIgnoreNullable(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.IGNORE);
  }

  private boolean equivalent(Schema other, EquivalenceNullablePolicy nullablePolicy) {
    if (other.getFieldCount() != getFieldCount()) {
      return false;
    }

    List<Field> otherFields =
        other
            .getFields()
            .stream()
            .sorted(Comparator.comparing(Field::getName))
            .collect(Collectors.toList());
    List<Field> actualFields =
        getFields()
            .stream()
            .sorted(Comparator.comparing(Field::getName))
            .collect(Collectors.toList());

    for (int i = 0; i < otherFields.size(); ++i) {
      Field otherField = otherFields.get(i);
      Field actualField = actualFields.get(i);
      if (!actualField.equivalent(otherField, nullablePolicy)) {
        return false;
      }
    }
    return true;
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
    return hashCode;
  }

  public List<Field> getFields() {
    return fields;
  }

  /**
   * An enumerated list of type constructors.
   *
   * <ul>
   *   <li>Atomic types are built from type constructors that take no arguments
   *   <li>Arrays, rows, and maps are type constructors that take additional arguments to form a
   *       valid {@link FieldType}.
   * </ul>
   */
  @SuppressWarnings("MutableConstantField")
  public enum TypeName {
    BYTE, // One-byte signed integer.
    INT16, // two-byte signed integer.
    INT32, // four-byte signed integer.
    INT64, // eight-byte signed integer.
    DECIMAL, // Arbitrary-precision decimal number
    FLOAT,
    DOUBLE,
    STRING, // String.
    DATETIME, // Date and time.
    BOOLEAN, // Boolean.
    BYTES, // Byte array.
    ARRAY,
    MAP,
    ROW; // The field is itself a nested row.

    public static final Set<TypeName> NUMERIC_TYPES =
        ImmutableSet.of(BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE);
    public static final Set<TypeName> STRING_TYPES = ImmutableSet.of(STRING);
    public static final Set<TypeName> DATE_TYPES = ImmutableSet.of(DATETIME);
    public static final Set<TypeName> COLLECTION_TYPES = ImmutableSet.of(ARRAY);
    public static final Set<TypeName> MAP_TYPES = ImmutableSet.of(MAP);
    public static final Set<TypeName> COMPOSITE_TYPES = ImmutableSet.of(ROW);

    public boolean isPrimitiveType() {
      return !isCollectionType() && !isMapType() && !isCompositeType();
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

    public boolean isCollectionType() {
      return COLLECTION_TYPES.contains(this);
    }

    public boolean isMapType() {
      return MAP_TYPES.contains(this);
    }

    public boolean isCompositeType() {
      return COMPOSITE_TYPES.contains(this);
    }

    public boolean isSubtypeOf(TypeName other) {
      return other.isSupertypeOf(this);
    }

    public boolean isSupertypeOf(TypeName other) {
      if (this == other) {
        return true;
      }

      // defined only for numeric types
      if (!isNumericType() || !other.isNumericType()) {
        return false;
      }

      switch (this) {
        case BYTE:
          return false;

        case INT16:
          return other == BYTE;

        case INT32:
          return other == BYTE || other == INT16;

        case INT64:
          return other == BYTE || other == INT16 || other == INT32;

        case FLOAT:
          return false;

        case DOUBLE:
          return other == FLOAT;

        case DECIMAL:
          return other == FLOAT || other == DOUBLE;

        default:
          throw new AssertionError("Unexpected numeric type: " + this);
      }
    }
  }

  /**
   * A descriptor of a single field type. This is a recursive descriptor, as nested types are
   * allowed.
   */
  @AutoValue
  @Immutable
  public abstract static class FieldType implements Serializable {
    // Returns the type of this field.
    public abstract TypeName getTypeName();

    // Whether this type is nullable.
    public abstract Boolean getNullable();

    // For container types (e.g. ARRAY), returns the type of the contained element.
    @Nullable
    public abstract FieldType getCollectionElementType();

    // For MAP type, returns the type of the key element, it must be a primitive type;
    @Nullable
    public abstract FieldType getMapKeyType();

    // For MAP type, returns the type of the value element, it can be a nested type;
    @Nullable
    public abstract FieldType getMapValueType();

    // For ROW types, returns the schema for the row.
    @Nullable
    public abstract Schema getRowSchema();

    /** Returns optional extra metadata. */
    @SuppressWarnings("mutable")
    @Nullable
    public abstract byte[] getMetadata();

    abstract FieldType.Builder toBuilder();

    public static FieldType.Builder forTypeName(TypeName typeName) {
      return new AutoValue_Schema_FieldType.Builder().setTypeName(typeName).setNullable(false);
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTypeName(TypeName typeName);

      abstract Builder setCollectionElementType(@Nullable FieldType collectionElementType);

      abstract Builder setNullable(Boolean nullable);

      abstract Builder setMapKeyType(@Nullable FieldType mapKeyType);

      abstract Builder setMapValueType(@Nullable FieldType mapValueType);

      abstract Builder setRowSchema(@Nullable Schema rowSchema);

      abstract Builder setMetadata(@Nullable byte[] metadata);

      abstract FieldType build();
    }

    /** Create a {@link FieldType} for the given type. */
    public static FieldType of(TypeName typeName) {
      return forTypeName(typeName).build();
    }

    /** The type of string fields. */
    public static final FieldType STRING = FieldType.of(TypeName.STRING);

    /** The type of byte fields. */
    public static final FieldType BYTE = FieldType.of(TypeName.BYTE);

    /** The type of bytes fields. */
    public static final FieldType BYTES = FieldType.of(TypeName.BYTES);

    /** The type of int16 fields. */
    public static final FieldType INT16 = FieldType.of(TypeName.INT16);

    /** The type of int32 fields. */
    public static final FieldType INT32 = FieldType.of(TypeName.INT32);

    /** The type of int64 fields. */
    public static final FieldType INT64 = FieldType.of(TypeName.INT64);

    /** The type of float fields. */
    public static final FieldType FLOAT = FieldType.of(TypeName.FLOAT);

    /** The type of double fields. */
    public static final FieldType DOUBLE = FieldType.of(TypeName.DOUBLE);

    /** The type of decimal fields. */
    public static final FieldType DECIMAL = FieldType.of(TypeName.DECIMAL);

    /** The type of boolean fields. */
    public static final FieldType BOOLEAN = FieldType.of(TypeName.BOOLEAN);

    /** The type of datetime fields. */
    public static final FieldType DATETIME = FieldType.of(TypeName.DATETIME);

    /** Create an array type for the given field type. */
    public static final FieldType array(FieldType elementType) {
      return FieldType.forTypeName(TypeName.ARRAY).setCollectionElementType(elementType).build();
    }

    public static final FieldType array(FieldType elementType, boolean nullable) {
      return FieldType.forTypeName(TypeName.ARRAY)
          .setCollectionElementType(elementType.withNullable(nullable))
          .build();
    }

    /** Create a map type for the given key and value types. */
    public static final FieldType map(FieldType keyType, FieldType valueType) {
      return FieldType.forTypeName(TypeName.MAP)
          .setMapKeyType(keyType)
          .setMapValueType(valueType)
          .build();
    }

    public static final FieldType map(
        FieldType keyType, FieldType valueType, boolean valueTypeNullable) {
      return FieldType.forTypeName(TypeName.MAP)
          .setMapKeyType(keyType)
          .setMapValueType(valueType.withNullable(valueTypeNullable))
          .build();
    }

    /** Create a map type for the given key and value types. */
    public static final FieldType row(Schema schema) {
      return FieldType.forTypeName(TypeName.ROW).setRowSchema(schema).build();
    }

    /** Returns a copy of the descriptor with metadata set. */
    public FieldType withMetadata(@Nullable byte[] metadata) {
      return toBuilder().setMetadata(metadata).build();
    }

    /** Returns a copy of the descriptor with metadata set. */
    public FieldType withMetadata(String metadata) {
      return toBuilder().setMetadata(metadata.getBytes(StandardCharsets.UTF_8)).build();
    }

    public FieldType withNullable(boolean nullable) {
      return toBuilder().setNullable(nullable).build();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FieldType)) {
        return false;
      }
      FieldType other = (FieldType) o;
      return Objects.equals(getTypeName(), other.getTypeName())
          && Objects.equals(getNullable(), other.getNullable())
          && Objects.equals(getCollectionElementType(), other.getCollectionElementType())
          && Objects.equals(getMapKeyType(), other.getMapKeyType())
          && Objects.equals(getMapValueType(), other.getMapValueType())
          && Objects.equals(getRowSchema(), other.getRowSchema())
          && Arrays.equals(getMetadata(), other.getMetadata());
    }

    /** Returns true if two FieldTypes are equal. */
    public boolean typesEqual(FieldType other) {
      if (!Objects.equals(getTypeName(), other.getTypeName())) {
        return false;
      }
      if (!Arrays.equals(getMetadata(), other.getMetadata())) {
        return false;
      }
      if (getTypeName() == TypeName.ARRAY
          && !getCollectionElementType().typesEqual(other.getCollectionElementType())) {
        return false;
      }
      if (getTypeName() == TypeName.MAP
          && (!getMapValueType().typesEqual(other.getMapValueType())
              || !getMapKeyType().typesEqual(other.getMapKeyType()))) {
        return false;
      }
      if (getTypeName() == TypeName.ROW && !getRowSchema().typesEqual(other.getRowSchema())) {
        return false;
      }
      return true;
    }

    private boolean equivalent(FieldType other, EquivalenceNullablePolicy nullablePolicy) {
      if (nullablePolicy == EquivalenceNullablePolicy.SAME
          && !other.getNullable().equals(getNullable())) {
        return false;
      } else if (nullablePolicy == EquivalenceNullablePolicy.WEAKEN) {
        if (getNullable() && !other.getNullable()) {
          return false;
        }
      }

      if (!getTypeName().equals(other.getTypeName())) {
        return false;
      }
      if (!getMetadata().equals(other.getMetadata())) {
        return false;
      }

      switch (getTypeName()) {
        case ROW:
          if (!getRowSchema().equivalent(other.getRowSchema(), nullablePolicy)) {
            return false;
          }
          break;
        case ARRAY:
          if (!getCollectionElementType()
              .equivalent(other.getCollectionElementType(), nullablePolicy)) {
            return false;
          }
          break;
        case MAP:
          if (!getMapKeyType().equivalent(other.getMapKeyType(), nullablePolicy)
              || !getMapValueType().equivalent(other.getMapValueType(), nullablePolicy)) {
            return false;
          }
          break;
        default:
          return true;
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(
          new Object[] {
            getTypeName(),
            getNullable(),
            getCollectionElementType(),
            getMapKeyType(),
            getMapValueType(),
            getRowSchema(),
            getMetadata()
          });
    }
  }

  /** Field of a row. Contains the {@link FieldType} along with associated metadata. */
  @AutoValue
  public abstract static class Field implements Serializable {
    /** Returns the field name. */
    public abstract String getName();

    /** Returns the field's description. */
    public abstract String getDescription();

    /** Returns the fields {@link FieldType}. */
    public abstract FieldType getType();

    public abstract Builder toBuilder();

    /** Builder for {@link Field}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setName(String name);

      public abstract Builder setDescription(String description);

      public abstract Builder setType(FieldType fieldType);

      public abstract Field build();
    }

    /** Return's a field with the give name and type. */
    public static Field of(String name, FieldType fieldType) {
      return new AutoValue_Schema_Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType)
          .build();
    }

    /** Return's a nullable field with the give name and type. */
    public static Field nullable(String name, FieldType fieldType) {
      return new AutoValue_Schema_Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType.withNullable(true))
          .build();
    }

    /** Returns a copy of the Field with the name set. */
    public Field withName(String name) {
      return toBuilder().setName(name).build();
    }

    /** Returns a copy of the Field with the description set. */
    public Field withDescription(String description) {
      return toBuilder().setDescription(description).build();
    }

    /** Returns a copy of the Field with the {@link FieldType} set. */
    public Field withType(FieldType fieldType) {
      return toBuilder().setType(fieldType).build();
    }

    /** Returns a copy of the Field with isNullable set. */
    public Field withNullable(boolean isNullable) {
      return toBuilder().setType(getType().withNullable(true)).build();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Field)) {
        return false;
      }
      Field other = (Field) o;
      return Objects.equals(getName(), other.getName())
          && Objects.equals(getDescription(), other.getDescription())
          && Objects.equals(getType(), other.getType());
    }

    /** Returns true if two fields are equal, ignoring name and description. */
    public boolean typesEqual(Field other) {
      return getType().typesEqual(other.getType());
    }

    private boolean equivalent(Field otherField, EquivalenceNullablePolicy nullablePolicy) {
      return getName().equals(otherField.getName())
          && getType().equivalent(otherField.getType(), nullablePolicy);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getDescription(), getType());
    }
  }

  /** Collects a stream of {@link Field}s into a {@link Schema}. */
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

  /** Return the list of all field names. */
  public List<String> getFieldNames() {
    return getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
  }

  /** Return a field by index. */
  public Field getField(int index) {
    return getFields().get(index);
  }

  public Field getField(String name) {
    return getFields().get(indexOf(name));
  }

  /** Find the index of a given field. */
  public int indexOf(String fieldName) {
    Integer index = fieldIndices.get(fieldName);
    if (index == null) {
      throw new IllegalArgumentException(
          String.format("Cannot find field %s in schema %s", fieldName, this));
    }
    return index;
  }

  /** Returns true if {@code fieldName} exists in the schema, false otherwise. */
  public boolean hasField(String fieldName) {
    return fieldIndices.containsKey(fieldName);
  }

  /** Return the name of field by index. */
  public String nameOf(int fieldIndex) {
    String name = fieldIndices.inverse().get(fieldIndex);
    if (name == null) {
      throw new IllegalArgumentException(String.format("Cannot find field %d", fieldIndex));
    }
    return name;
  }

  /** Return the count of fields. */
  public int getFieldCount() {
    return getFields().size();
  }
}

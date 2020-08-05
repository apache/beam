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

import static org.apache.beam.sdk.values.SchemaVerification.verifyFieldValue;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link Schema} describes the fields in {@link Row}. */
@Experimental(Kind.SCHEMAS)
public class Schema implements Serializable {
  // This is the metadata field used to store the logical type identifier.
  private static final String LOGICAL_TYPE_IDENTIFIER = "SchemaLogicalTypeId";

  private static final String LOGICAL_TYPE_ARGUMENT = "SchemaLogicalTypeArg";

  // Helper class that adds proper equality checks to byte arrays.
  static class ByteArrayWrapper implements Serializable {
    final byte[] array;

    private ByteArrayWrapper(byte[] array) {
      this.array = array;
    }

    static ByteArrayWrapper wrap(byte[] array) {
      return new ByteArrayWrapper(array);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof ByteArrayWrapper)) {
        return false;
      }
      return Arrays.equals(array, ((ByteArrayWrapper) other).array);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(array);
    }

    @Override
    public String toString() {
      return Arrays.toString(array);
    }
  }
  // A mapping between field names an indices.
  private final BiMap<String, Integer> fieldIndices = HashBiMap.create();
  private Map<String, Integer> encodingPositions = Maps.newHashMap();

  private final List<Field> fields;
  // Cache the hashCode, so it doesn't have to be recomputed. Schema objects are immutable, so this
  // is correct.
  private final int hashCode;
  // Every SchemaCoder has a UUID. The schemas created with the same UUID are guaranteed to be
  // equal, so we can short circuit comparison.
  private @Nullable UUID uuid = null;

  private final Options options;

  /** Builder class for building {@link Schema} objects. */
  public static class Builder {
    List<Field> fields;
    Options options = Options.none();

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

    public <InputT, BaseT> Builder addLogicalTypeField(
        String name, LogicalType<InputT, BaseT> logicalType) {
      fields.add(Field.of(name, FieldType.logicalType(logicalType)));
      return this;
    }

    public Builder addArrayField(String name, FieldType collectionElementType) {
      fields.add(Field.of(name, FieldType.array(collectionElementType)));
      return this;
    }

    public Builder addIterableField(String name, FieldType collectionElementType) {
      fields.add(Field.of(name, FieldType.iterable(collectionElementType)));
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

    /** Returns a copy of the Field with isNullable set. */
    public Builder setOptions(Options options) {
      this.options = options;
      return this;
    }

    public Builder setOptions(Options.Builder optionsBuilder) {
      this.options = optionsBuilder.build();
      return this;
    }

    public int getLastFieldId() {
      return fields.size() - 1;
    }

    public Schema build() {
      return new Schema(fields, options);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public Schema(List<Field> fields) {
    this(fields, Options.none());
  }

  public Schema(List<Field> fields, Options options) {
    this.fields = fields;
    int index = 0;
    for (Field field : fields) {
      Preconditions.checkArgument(
          fieldIndices.get(field.getName()) == null,
          "Duplicate field " + field.getName() + " added to schema");
      encodingPositions.put(field.getName(), index);
      fieldIndices.put(field.getName(), index++);
    }
    this.hashCode = Objects.hash(fieldIndices, fields);
    this.options = options;
  }

  public static Schema of(Field... fields) {
    return Schema.builder().addFields(fields).build();
  }

  /** Returns a copy of the Schema with the options set. */
  public Schema withOptions(Options options) {
    return new Schema(fields, getOptions().toBuilder().addOptions(options).build());
  }

  /** Returns a copy of the Schema with the options set. */
  public Schema withOptions(Options.Builder optionsBuilder) {
    return withOptions(optionsBuilder.build());
  }

  /** Set this schema's UUID. All schemas with the same UUID must be guaranteed to be identical. */
  public void setUUID(UUID uuid) {
    this.uuid = uuid;
  }

  /** Gets the encoding positions for this schema. */
  public Map<String, Integer> getEncodingPositions() {
    return encodingPositions;
  }

  /** Sets the encoding positions for this schema. */
  public void setEncodingPositions(Map<String, Integer> encodingPositions) {
    this.encodingPositions = encodingPositions;
  }

  /** Get this schema's UUID. */
  public @Nullable UUID getUUID() {
    return this.uuid;
  }

  /** Returns true if two Schemas have the same fields in the same order. */
  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Schema other = (Schema) o;
    // If both schemas have a UUID set, we can simply compare the UUIDs.
    if (uuid != null && other.uuid != null) {
      if (Objects.equals(uuid, other.uuid)) {
        return true;
      }
    }
    return Objects.equals(fieldIndices, other.fieldIndices)
        && Objects.equals(getFields(), other.getFields())
        && Objects.equals(getOptions(), other.getOptions());
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

  /** Control whether nullable is included in equivalence check. */
  public enum EquivalenceNullablePolicy {
    SAME,
    WEAKEN,
    IGNORE
  }

  /** Returns true if two Schemas have the same fields, but possibly in different orders. */
  public boolean equivalent(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.SAME);
  }

  /** Returns true if this Schema can be assigned to another Schema. * */
  public boolean assignableTo(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.WEAKEN);
  }

  /** Returns true if this Schema can be assigned to another Schema, ignoring nullable. * */
  public boolean assignableToIgnoreNullable(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.IGNORE);
  }

  private boolean equivalent(Schema other, EquivalenceNullablePolicy nullablePolicy) {
    if (other.getFieldCount() != getFieldCount()) {
      return false;
    }

    List<Field> otherFields =
        other.getFields().stream()
            .sorted(Comparator.comparing(Field::getName))
            .collect(Collectors.toList());
    List<Field> actualFields =
        getFields().stream()
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
    builder.append("Fields:");
    builder.append(System.lineSeparator());
    for (Field field : fields) {
      builder.append(field);
      builder.append(System.lineSeparator());
    }
    builder.append("Options:");
    builder.append(options);
    return builder.toString();
  }

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
    ITERABLE, // Iterable. Different than array in that it might not fit completely in memory.
    MAP,
    ROW, // The field is itself a nested row.
    LOGICAL_TYPE;

    public static final Set<TypeName> NUMERIC_TYPES =
        ImmutableSet.of(BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE);
    public static final Set<TypeName> STRING_TYPES = ImmutableSet.of(STRING);
    public static final Set<TypeName> DATE_TYPES = ImmutableSet.of(DATETIME);
    public static final Set<TypeName> COLLECTION_TYPES = ImmutableSet.of(ARRAY, ITERABLE);
    public static final Set<TypeName> MAP_TYPES = ImmutableSet.of(MAP);
    public static final Set<TypeName> COMPOSITE_TYPES = ImmutableSet.of(ROW);

    public boolean isPrimitiveType() {
      return !isCollectionType() && !isMapType() && !isCompositeType() && !isLogicalType();
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

    public boolean isLogicalType() {
      return this.equals(LOGICAL_TYPE);
    }

    public boolean isSubtypeOf(TypeName other) {
      return other.isSupertypeOf(this);
    }

    /** Whether this is a super type of the another type. */
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
   * A LogicalType allows users to define a custom schema type.
   *
   * <p>A LogicalType is a way to define a new type that can be stored in a schema field using an
   * underlying FieldType as storage. A LogicalType must specify a base FieldType used to store the
   * data by overriding the {@link #getBaseType()} method. Usually the FieldType returned will be
   * one of the standard ones implemented by Schema. It is legal to return another LogicalType, but
   * the storage types must eventually resolve to one of the standard Schema types; it is not
   * allowed to have LogicalTypes reference each other recursively via getBaseType. The {@link
   * #toBaseType} and {@link #toInputType} should convert back and forth between the Java type for
   * the LogicalType (InputT) and the Java type appropriate for the underlying base type (BaseT).
   *
   * <p>{@link #getIdentifier} must define a globally unique identifier for this LogicalType. A
   * LogicalType can optionally provide an identifying argument as well using {@link #getArgument}.
   * An example is a LogicalType that represents a fixed-size byte array. The identifier
   * "FixedBytes" uniquely identifies this LogicalType (or specifically, this class of
   * LogicalTypes), while the argument returned will be the length of the fixed-size byte array. The
   * combination of {@link #getIdentifier} and {@link #getArgument} must completely identify a
   * LogicalType.
   *
   * <p>A LogicalType can be added to a schema using {@link Schema.Builder#addLogicalTypeField}.
   *
   * @param <InputT> The Java type used to set the type when using {@link Row.Builder#addValue}.
   * @param <BaseT> The Java type for the underlying storage.
   */
  public interface LogicalType<InputT, BaseT> extends Serializable {
    /** The unique identifier for this type. */
    String getIdentifier();

    /** A schema type representing how to interpret the argument. */
    FieldType getArgumentType();

    /** An optional argument to configure the type. */
    @SuppressWarnings("TypeParameterUnusedInFormals")
    default <T> T getArgument() {
      return null;
    }

    /** The base {@link FieldType} used to store values of this type. */
    FieldType getBaseType();

    BaseT toBaseType(InputT input);

    /** Convert the Java type used by the base {@link FieldType} to the input type. */
    InputT toInputType(BaseT base);
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

    // For logical types, return the implementing class.

    public abstract @Nullable LogicalType getLogicalType();

    // For container types (e.g. ARRAY or ITERABLE), returns the type of the contained element.

    public abstract @Nullable FieldType getCollectionElementType();

    // For MAP type, returns the type of the key element, it must be a primitive type;

    public abstract @Nullable FieldType getMapKeyType();

    // For MAP type, returns the type of the value element, it can be a nested type;

    public abstract @Nullable FieldType getMapValueType();

    // For ROW types, returns the schema for the row.

    public abstract @Nullable Schema getRowSchema();

    /**
     * Returns optional extra metadata.
     *
     * @deprecated use schema options instead.
     */
    @SuppressWarnings("mutable")
    @Deprecated
    abstract Map<String, ByteArrayWrapper> getMetadata();

    public abstract FieldType.Builder toBuilder();

    public boolean isLogicalType(String logicalTypeIdentifier) {
      return getTypeName().isLogicalType()
          && getLogicalType().getIdentifier().equals(logicalTypeIdentifier);
    }

    /** Helper function for retrieving the concrete logical type subclass. */
    public <LogicalTypeT extends LogicalType> LogicalTypeT getLogicalType(
        Class<LogicalTypeT> logicalTypeClass) {
      return logicalTypeClass.cast(getLogicalType());
    }

    public static FieldType.Builder forTypeName(TypeName typeName) {
      return new AutoValue_Schema_FieldType.Builder()
          .setTypeName(typeName)
          .setNullable(false)
          .setMetadata(Collections.emptyMap());
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTypeName(TypeName typeName);

      abstract Builder setLogicalType(LogicalType logicalType);

      abstract Builder setCollectionElementType(@Nullable FieldType collectionElementType);

      abstract Builder setNullable(Boolean nullable);

      abstract Builder setMapKeyType(@Nullable FieldType mapKeyType);

      abstract Builder setMapValueType(@Nullable FieldType mapValueType);

      abstract Builder setRowSchema(@Nullable Schema rowSchema);

      /** @deprecated use schema options instead. */
      @Deprecated
      abstract Builder setMetadata(Map<String, ByteArrayWrapper> metadata);

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
    public static FieldType array(FieldType elementType) {
      return FieldType.forTypeName(TypeName.ARRAY).setCollectionElementType(elementType).build();
    }

    /** @deprecated Set the nullability on the elementType instead */
    @Deprecated
    public static FieldType array(FieldType elementType, boolean nullable) {
      return FieldType.forTypeName(TypeName.ARRAY)
          .setCollectionElementType(elementType.withNullable(nullable))
          .build();
    }

    public static FieldType iterable(FieldType elementType) {
      return FieldType.forTypeName(TypeName.ITERABLE).setCollectionElementType(elementType).build();
    }

    /** Create a map type for the given key and value types. */
    public static FieldType map(FieldType keyType, FieldType valueType) {
      return FieldType.forTypeName(TypeName.MAP)
          .setMapKeyType(keyType)
          .setMapValueType(valueType)
          .build();
    }

    /** @deprecated Set the nullability on the valueType instead */
    @Deprecated
    public static FieldType map(FieldType keyType, FieldType valueType, boolean valueTypeNullable) {
      return FieldType.forTypeName(TypeName.MAP)
          .setMapKeyType(keyType)
          .setMapValueType(valueType.withNullable(valueTypeNullable))
          .build();
    }

    /** Create a map type for the given key and value types. */
    public static FieldType row(Schema schema) {
      return FieldType.forTypeName(TypeName.ROW).setRowSchema(schema).build();
    }

    /** Creates a logical type based on a primitive field type. */
    public static <InputT, BaseT> FieldType logicalType(LogicalType<InputT, BaseT> logicalType) {
      return FieldType.forTypeName(TypeName.LOGICAL_TYPE).setLogicalType(logicalType).build();
    }

    /**
     * Set the metadata map for the type, overriding any existing metadata.
     *
     * @deprecated use schema options instead.
     */
    @Deprecated
    public FieldType withMetadata(Map<String, byte[]> metadata) {
      Map<String, ByteArrayWrapper> wrapped =
          metadata.entrySet().stream()
              .collect(
                  Collectors.toMap(Map.Entry::getKey, e -> ByteArrayWrapper.wrap(e.getValue())));
      return toBuilder().setMetadata(wrapped).build();
    }

    /**
     * Returns a copy of the descriptor with metadata set for the given key.
     *
     * @deprecated use schema options instead.
     */
    @Deprecated
    public FieldType withMetadata(String key, byte[] metadata) {
      Map<String, ByteArrayWrapper> newMetadata =
          ImmutableMap.<String, ByteArrayWrapper>builder()
              .putAll(getMetadata())
              .put(key, ByteArrayWrapper.wrap(metadata))
              .build();
      return toBuilder().setMetadata(newMetadata).build();
    }

    /**
     * Returns a copy of the descriptor with metadata set for the given key.
     *
     * @deprecated use schema options instead.
     */
    @Deprecated
    public FieldType withMetadata(String key, String metadata) {
      return withMetadata(key, metadata.getBytes(StandardCharsets.UTF_8));
    }

    /** @deprecated use schema options instead. */
    @Deprecated
    public Map<String, byte[]> getAllMetadata() {
      return getMetadata().entrySet().stream()
          .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().array));
    }

    /** @deprecated use schema options instead. */
    @Deprecated
    public byte @Nullable [] getMetadata(String key) {
      ByteArrayWrapper metadata = getMetadata().get(key);
      return (metadata != null) ? metadata.array : null;
    }

    /** @deprecated use schema options instead. */
    @Deprecated
    public String getMetadataString(String key) {
      ByteArrayWrapper metadata = getMetadata().get(key);
      if (metadata != null) {
        return new String(metadata.array, StandardCharsets.UTF_8);
      } else {
        return "";
      }
    }

    public FieldType withNullable(boolean nullable) {
      return toBuilder().setNullable(nullable).build();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof FieldType)) {
        return false;
      }

      FieldType other = (FieldType) o;
      if (getTypeName().isLogicalType()) {
        if (!other.getTypeName().isLogicalType()) {
          return false;
        }
        if (!Objects.equals(
            getLogicalType().getIdentifier(), other.getLogicalType().getIdentifier())) {
          return false;
        }
        if (!getLogicalType().getArgumentType().equals(other.getLogicalType().getArgumentType())) {
          return false;
        }
        if (!Row.Equals.deepEquals(
            getLogicalType().getArgument(),
            other.getLogicalType().getArgument(),
            getLogicalType().getArgumentType())) {
          return false;
        }
      }
      return Objects.equals(getTypeName(), other.getTypeName())
          && Objects.equals(getNullable(), other.getNullable())
          && Objects.equals(getCollectionElementType(), other.getCollectionElementType())
          && Objects.equals(getMapKeyType(), other.getMapKeyType())
          && Objects.equals(getMapValueType(), other.getMapValueType())
          && Objects.equals(getRowSchema(), other.getRowSchema())
          && Objects.equals(getMetadata(), other.getMetadata());
    }

    /** Returns true if two FieldTypes are equal. */
    public boolean typesEqual(FieldType other) {
      if (!Objects.equals(getTypeName(), other.getTypeName())) {
        return false;
      }
      if (getTypeName().isLogicalType()) {
        if (!other.getTypeName().isLogicalType()) {
          return false;
        }
        if (!Objects.equals(
            getLogicalType().getIdentifier(), other.getLogicalType().getIdentifier())) {
          return false;
        }
        if (!getLogicalType().getArgumentType().equals(other.getLogicalType().getArgumentType())) {
          return false;
        }
        if (!Row.Equals.deepEquals(
            getLogicalType().getArgument(),
            other.getLogicalType().getArgument(),
            getLogicalType().getArgumentType())) {
          return false;
        }
      }
      if (!Objects.equals(getNullable(), other.getNullable())) {
        return false;
      }
      if (!Objects.equals(getMetadata(), other.getMetadata())) {
        return false;
      }
      if (getTypeName().isCollectionType()
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

    /** Check whether two types are equivalent. */
    public boolean equivalent(FieldType other, EquivalenceNullablePolicy nullablePolicy) {
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

      switch (getTypeName()) {
        case ROW:
          if (!getRowSchema().equivalent(other.getRowSchema(), nullablePolicy)) {
            return false;
          }
          break;
        case ARRAY:
        case ITERABLE:
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

    /** Returns the fields {@link Options}. */
    public abstract Options getOptions();

    public abstract Builder toBuilder();

    /** Builder for {@link Field}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setName(String name);

      public abstract Builder setDescription(String description);

      public abstract Builder setType(FieldType fieldType);

      public abstract Builder setOptions(Options options);

      public Builder setOptions(Options.Builder optionsBuilder) {
        setOptions(optionsBuilder.build());
        return this;
      }

      public abstract Field build();
    }

    /** Return's a field with the give name and type. */
    public static Field of(String name, FieldType fieldType) {
      return new AutoValue_Schema_Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType)
          .setOptions(Options.none())
          .build();
    }

    /** Return's a nullable field with the give name and type. */
    public static Field nullable(String name, FieldType fieldType) {
      return new AutoValue_Schema_Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType.withNullable(true))
          .setOptions(Options.none())
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
      return toBuilder().setType(getType().withNullable(isNullable)).build();
    }

    /** Returns a copy of the Field with the options set. */
    public Field withOptions(Options options) {
      return toBuilder().setOptions(getOptions().toBuilder().addOptions(options).build()).build();
    }

    /** Returns a copy of the Field with the options set. */
    public Field withOptions(Options.Builder optionsBuilder) {
      return withOptions(optionsBuilder.build());
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof Field)) {
        return false;
      }
      Field other = (Field) o;
      return Objects.equals(getName(), other.getName())
          && Objects.equals(getDescription(), other.getDescription())
          && Objects.equals(getType(), other.getType())
          && Objects.equals(getOptions(), other.getOptions());
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

  public static class Options implements Serializable {
    private final Map<String, Option> options;

    @Override
    public String toString() {
      TreeMap sorted = new TreeMap(options);
      return "{" + sorted + '}';
    }

    Map<String, Option> getAllOptions() {
      return options;
    }

    public Set<String> getOptionNames() {
      return options.keySet();
    }

    public boolean hasOptions() {
      return options.size() > 0;
    }

    public boolean hasOption(String name) {
      return options.containsKey(name);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Options options1 = (Options) o;
      if (!options.keySet().equals(options1.options.keySet())) {
        return false;
      }
      for (Map.Entry<String, Option> optionEntry : options.entrySet()) {
        Option thisOption = optionEntry.getValue();
        Option otherOption = options1.options.get(optionEntry.getKey());
        if (!thisOption.equals(otherOption)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(options);
    }

    static class Option implements Serializable {
      Option(FieldType type, Object value) {
        this.type = type;
        this.value = value;
      }

      private FieldType type;
      private Object value;

      @SuppressWarnings("TypeParameterUnusedInFormals")
      <T> T getValue() {
        return (T) value;
      }

      FieldType getType() {
        return type;
      }

      @Override
      public String toString() {
        return "Option{type=" + type + ", value=" + value + '}';
      }

      @Override
      public boolean equals(@Nullable Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        Option option = (Option) o;
        return Objects.equals(type, option.type)
            && Row.Equals.deepEquals(value, option.value, type);
      }

      @Override
      public int hashCode() {
        return Row.Equals.deepHashCode(value, type);
      }
    }

    public static class Builder {
      private Map<String, Option> options;

      Builder(Map<String, Option> init) {
        this.options = new HashMap<>(init);
      }

      Builder() {
        this(new HashMap<>());
      }

      public Builder setOption(String optionName, Row value) {
        setOption(optionName, FieldType.row(value.getSchema()), value);
        return this;
      }

      public Builder setOption(String optionName, FieldType fieldType, Object value) {
        if (value == null) {
          if (fieldType.getNullable()) {
            options.put(optionName, new Option(fieldType, null));
          } else {
            throw new IllegalArgumentException(
                String.format("Option %s is not nullable", optionName));
          }
        } else {
          options.put(
              optionName, new Option(fieldType, verifyFieldValue(value, fieldType, optionName)));
        }
        return this;
      }

      public Options build() {
        return new Options(this.options);
      }

      public Builder addOptions(Options options) {
        this.options.putAll(options.options);
        return this;
      }
    }

    Options(Map<String, Option> options) {
      this.options = options;
    }

    Options() {
      this.options = new HashMap<>();
    }

    Options.Builder toBuilder() {
      return new Builder(new HashMap<>(this.options));
    }

    public static Options.Builder builder() {
      return new Builder();
    }

    public static Options none() {
      return new Options();
    }

    /** Get the value of an option. If the option is not found null is returned. */
    @SuppressWarnings("TypeParameterUnusedInFormals")
    public <T> T getValue(String optionName) {
      Option option = options.get(optionName);
      if (option != null) {
        return option.getValue();
      }
      throw new IllegalArgumentException(
          String.format("No option found with name %s.", optionName));
    }

    /** Get the value of an option. If the option is not found null is returned. */
    public <T> T getValue(String optionName, Class<T> valueClass) {
      return getValue(optionName);
    }

    /** Get the value of an option. If the option is not found the default value is returned. */
    public <T> T getValueOrDefault(String optionName, T defaultValue) {
      Option option = options.get(optionName);
      if (option != null) {
        return option.getValue();
      }
      return defaultValue;
    }

    /** Get the type of an option. */
    public FieldType getType(String optionName) {
      Option option = options.get(optionName);
      if (option != null) {
        return option.getType();
      }
      throw new IllegalArgumentException(
          String.format("No option found with name %s.", optionName));
    }

    public static Options.Builder setOption(String optionName, FieldType fieldType, Object value) {
      return Options.builder().setOption(optionName, fieldType, value);
    }

    public static Options.Builder setOption(String optionName, Row value) {
      return Options.builder().setOption(optionName, value);
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
    Preconditions.checkArgument(
        index != null, "Cannot find field %s in schema %s", fieldName, this);
    return index;
  }

  /** Returns true if {@code fieldName} exists in the schema, false otherwise. */
  public boolean hasField(String fieldName) {
    return fieldIndices.containsKey(fieldName);
  }

  /** Return the name of field by index. */
  public String nameOf(int fieldIndex) {
    String name = fieldIndices.inverse().get(fieldIndex);
    Preconditions.checkArgument(name != null, "Cannot find field %s", fieldIndex);
    return name;
  }

  /** Return the count of fields. */
  public int getFieldCount() {
    return getFields().size();
  }

  public Options getOptions() {
    return this.options;
  }
}

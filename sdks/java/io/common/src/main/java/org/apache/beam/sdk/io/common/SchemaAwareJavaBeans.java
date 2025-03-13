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
package org.apache.beam.sdk.io.common;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

/** Various Java Beans and associated schemas used in tests. */
public class SchemaAwareJavaBeans {

  private static final DefaultSchemaProvider DEFAULT_SCHEMA_PROVIDER = new DefaultSchemaProvider();

  /** Convenience method for {@link AllPrimitiveDataTypes} instantiation. */
  public static AllPrimitiveDataTypes allPrimitiveDataTypes(
      Boolean aBoolean,
      BigDecimal aDecimal,
      Double aDouble,
      Float aFloat,
      Integer anInteger,
      Long aLong,
      String aString) {
    return new AutoValue_SchemaAwareJavaBeans_AllPrimitiveDataTypes.Builder()
        .setABoolean(aBoolean)
        .setADecimal(aDecimal)
        .setADouble(aDouble)
        .setAFloat(aFloat)
        .setAnInteger(anInteger)
        .setALong(aLong)
        .setAString(aString)
        .build();
  }

  /** Convenience method for {@link NullableAllPrimitiveDataTypes} instantiation. */
  public static NullableAllPrimitiveDataTypes nullableAllPrimitiveDataTypes(
      @Nullable Boolean aBoolean,
      @Nullable Double aDouble,
      @Nullable Float aFloat,
      @Nullable Integer anInteger,
      @Nullable Long aLong,
      @Nullable String aString) {
    return new AutoValue_SchemaAwareJavaBeans_NullableAllPrimitiveDataTypes.Builder()
        .setABoolean(aBoolean)
        .setADouble(aDouble)
        .setAFloat(aFloat)
        .setAnInteger(anInteger)
        .setALong(aLong)
        .setAString(aString)
        .build();
  }

  /** Convenience method for {@link TimeContaining} instantiation. */
  public static TimeContaining timeContaining(Instant instant, List<Instant> instantList) {
    return new AutoValue_SchemaAwareJavaBeans_TimeContaining.Builder()
        .setInstant(instant)
        .setInstantList(instantList)
        .build();
  }

  /** Convenience method for {@link ByteType} instantiation. */
  public static ByteType byteType(Byte aByte, List<Byte> byteList) {
    return new AutoValue_SchemaAwareJavaBeans_ByteType.Builder()
        .setByte(aByte)
        .setByteList(byteList)
        .build();
  }

  /** Convenience method for {@link ByteSequenceType} instantiation. */
  public static ByteSequenceType byteSequenceType(
      byte[] byteSequence, List<byte[]> byteSequenceList) {
    return new AutoValue_SchemaAwareJavaBeans_ByteSequenceType.Builder()
        .setByteSequence(byteSequence)
        .setByteSequenceList(byteSequenceList)
        .build();
  }

  /** Convenience method for {@link ArrayPrimitiveDataTypes} instantiation. */
  public static ArrayPrimitiveDataTypes arrayPrimitiveDataTypes(
      List<Boolean> booleans,
      List<Double> doubles,
      List<Float> floats,
      List<Integer> integers,
      List<Long> longs,
      List<String> strings) {
    return new AutoValue_SchemaAwareJavaBeans_ArrayPrimitiveDataTypes.Builder()
        .setBooleanList(booleans)
        .setDoubleList(doubles)
        .setFloatList(floats)
        .setIntegerList(integers)
        .setLongList(longs)
        .setStringList(strings)
        .build();
  }

  /** Convenience method for {@link SinglyNestedDataTypes} instantiation. */
  public static SinglyNestedDataTypes singlyNestedDataTypes(
      AllPrimitiveDataTypes allPrimitiveDataTypes, AllPrimitiveDataTypes... repeated) {
    return new AutoValue_SchemaAwareJavaBeans_SinglyNestedDataTypes.Builder()
        .setAllPrimitiveDataTypes(allPrimitiveDataTypes)
        .setAllPrimitiveDataTypesList(Arrays.stream(repeated).collect(Collectors.toList()))
        .build();
  }

  /** Convenience method for {@link DoublyNestedDataTypes} instantiation. */
  public static DoublyNestedDataTypes doublyNestedDataTypes(
      SinglyNestedDataTypes singlyNestedDataTypes, SinglyNestedDataTypes... repeated) {
    return new AutoValue_SchemaAwareJavaBeans_DoublyNestedDataTypes.Builder()
        .setSinglyNestedDataTypes(singlyNestedDataTypes)
        .setSinglyNestedDataTypesList(Arrays.stream(repeated).collect(Collectors.toList()))
        .build();
  }

  public static final TypeDescriptor<AllPrimitiveDataTypes>
      ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR = TypeDescriptor.of(AllPrimitiveDataTypes.class);

  /** The schema for {@link AllPrimitiveDataTypes}. */
  public static final Schema ALL_PRIMITIVE_DATA_TYPES_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link AllPrimitiveDataTypes} to a
   * {@link Row}.
   */
  public static SerializableFunction<AllPrimitiveDataTypes, Row> allPrimitiveDataTypesToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link Row} to a {@link
   * AllPrimitiveDataTypes}.
   */
  public static SerializableFunction<Row, AllPrimitiveDataTypes> allPrimitiveDataTypesFromRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  public static final TypeDescriptor<NullableAllPrimitiveDataTypes>
      NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR =
          TypeDescriptor.of(NullableAllPrimitiveDataTypes.class);

  /** The schema for {@link NullableAllPrimitiveDataTypes}. */
  public static final Schema NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link NullableAllPrimitiveDataTypes}
   * to a {@link Row}.
   */
  public static SerializableFunction<NullableAllPrimitiveDataTypes, Row>
      nullableAllPrimitiveDataTypesToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link Row} to a {@link
   * NullableAllPrimitiveDataTypes}.
   */
  public static SerializableFunction<Row, NullableAllPrimitiveDataTypes>
      nullableAllPrimitiveDataTypesFromRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(
        NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  public static final TypeDescriptor<TimeContaining> TIME_CONTAINING_TYPE_DESCRIPTOR =
      TypeDescriptor.of(TimeContaining.class);

  /** The schema for {@link TimeContaining}. */
  public static final Schema TIME_CONTAINING_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(TIME_CONTAINING_TYPE_DESCRIPTOR);

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link TimeContaining} to a {@link
   * Row}.
   */
  public static SerializableFunction<TimeContaining, Row> timeContainingToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(TIME_CONTAINING_TYPE_DESCRIPTOR);
  }

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link Row} to a {@link
   * TimeContaining}.
   */
  public static SerializableFunction<Row, TimeContaining> timeContainingFromRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(TIME_CONTAINING_TYPE_DESCRIPTOR);
  }

  private static final TypeDescriptor<ByteType> BYTE_TYPE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(ByteType.class);

  /** The schema for {@link ByteType}. */
  public static final Schema BYTE_TYPE_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(BYTE_TYPE_TYPE_DESCRIPTOR);

  /** Returns a {@link SerializableFunction} to convert from a {@link ByteType} to a {@link Row}. */
  public static SerializableFunction<ByteType, Row> byteTypeToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(BYTE_TYPE_TYPE_DESCRIPTOR);
  }

  /** Returns a {@link SerializableFunction} to convert from a {@link Row} to a {@link ByteType}. */
  public static SerializableFunction<Row, ByteType> byteTypeFromRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(BYTE_TYPE_TYPE_DESCRIPTOR);
  }

  private static final TypeDescriptor<ByteSequenceType> BYTE_SEQUENCE_TYPE_TYPE_DESCRIPTOR =
      TypeDescriptor.of(ByteSequenceType.class);

  /** The schema for {@link ByteSequenceType}. */
  public static final Schema BYTE_SEQUENCE_TYPE_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(BYTE_SEQUENCE_TYPE_TYPE_DESCRIPTOR);

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link ByteSequenceType} to a {@link
   * Row}.
   */
  public static SerializableFunction<ByteSequenceType, Row> byteSequenceTypeToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(BYTE_SEQUENCE_TYPE_TYPE_DESCRIPTOR);
  }

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link Row} to a {@link
   * ByteSequenceType}.
   */
  public static SerializableFunction<Row, ByteSequenceType> byteSequenceTypeFromRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(BYTE_SEQUENCE_TYPE_TYPE_DESCRIPTOR);
  }

  public static final TypeDescriptor<ArrayPrimitiveDataTypes>
      ARRAY_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR = TypeDescriptor.of(ArrayPrimitiveDataTypes.class);

  /** The schema for {@link ArrayPrimitiveDataTypes}. */
  public static final Schema ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(ARRAY_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link ArrayPrimitiveDataTypes} to a
   * {@link Row}.
   */
  public static SerializableFunction<ArrayPrimitiveDataTypes, Row>
      arrayPrimitiveDataTypesToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(ARRAY_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link Row} to a {@link
   * ArrayPrimitiveDataTypes}.
   */
  public static SerializableFunction<Row, ArrayPrimitiveDataTypes>
      arrayPrimitiveDataTypesFromRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(ARRAY_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  private static final TypeDescriptor<SinglyNestedDataTypes>
      SINGLY_NESTED_DATA_TYPES_TYPE_DESCRIPTOR = TypeDescriptor.of(SinglyNestedDataTypes.class);

  /** The schema for {@link SinglyNestedDataTypes}. */
  public static final Schema SINGLY_NESTED_DATA_TYPES_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(SINGLY_NESTED_DATA_TYPES_TYPE_DESCRIPTOR);

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link SinglyNestedDataTypes} to a
   * {@link Row}.
   */
  public static SerializableFunction<SinglyNestedDataTypes, Row> singlyNestedDataTypesToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(SINGLY_NESTED_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link Row} to a {@link
   * SinglyNestedDataTypes}.
   */
  public static SerializableFunction<Row, SinglyNestedDataTypes> singlyNestedDataTypesFromRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(SINGLY_NESTED_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  private static final TypeDescriptor<DoublyNestedDataTypes>
      DOUBLY_NESTED_DATA_TYPES_TYPE_DESCRIPTOR = TypeDescriptor.of(DoublyNestedDataTypes.class);

  /** The schema for {@link DoublyNestedDataTypes}. */
  public static final Schema DOUBLY_NESTED_DATA_TYPES_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(DOUBLY_NESTED_DATA_TYPES_TYPE_DESCRIPTOR);

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link DoublyNestedDataTypes} to a
   * {@link Row}.
   */
  public static SerializableFunction<DoublyNestedDataTypes, Row> doublyNestedDataTypesToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(DOUBLY_NESTED_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link Row} to a {@link
   * DoublyNestedDataTypes}.
   */
  public static SerializableFunction<Row, DoublyNestedDataTypes> doublyNestedDataTypesFromRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(DOUBLY_NESTED_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  /**
   * Contains all primitive Java types i.e. String, Integer, etc and {@link BigDecimal}. The purpose
   * of this class is to test schema-aware PTransforms with flat {@link Schema} {@link Row}s.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class AllPrimitiveDataTypes implements Serializable {

    public abstract Boolean getABoolean();

    public abstract BigDecimal getADecimal();

    public abstract Double getADouble();

    public abstract Float getAFloat();

    public abstract Integer getAnInteger();

    public abstract Long getALong();

    public abstract String getAString();

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setABoolean(Boolean value);

      public abstract Builder setADecimal(BigDecimal value);

      public abstract Builder setADouble(Double value);

      public abstract Builder setAFloat(Float value);

      public abstract Builder setAnInteger(Integer value);

      public abstract Builder setALong(Long value);

      public abstract Builder setAString(String value);

      public abstract AllPrimitiveDataTypes build();
    }
  }

  /**
   * Contains all nullable primitive Java types i.e. String, Integer, etc and {@link BigDecimal}.
   * The purpose of this class is to test schema-aware PTransforms with flat {@link Schema} {@link
   * Row}s.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class NullableAllPrimitiveDataTypes implements Serializable {

    @Nullable
    public abstract Boolean getABoolean();

    @Nullable
    public abstract Double getADouble();

    @Nullable
    public abstract Float getAFloat();

    @Nullable
    public abstract Integer getAnInteger();

    @Nullable
    public abstract Long getALong();

    @Nullable
    public abstract String getAString();

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setABoolean(@Nullable Boolean value);

      public abstract Builder setADouble(@Nullable Double value);

      public abstract Builder setAFloat(@Nullable Float value);

      public abstract Builder setAnInteger(@Nullable Integer value);

      public abstract Builder setALong(@Nullable Long value);

      public abstract Builder setAString(@Nullable String value);

      public abstract NullableAllPrimitiveDataTypes build();
    }
  }

  /**
   * Contains time-related types. The purpose of this class is to test schema-aware PTransforms with
   * time-related {@link Schema.FieldType} containing {@link Row}s.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class TimeContaining {

    public abstract Instant getInstant();

    public abstract List<Instant> getInstantList();

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setInstant(Instant value);

      public abstract Builder setInstantList(List<Instant> value);

      public abstract TimeContaining build();
    }
  }

  /**
   * Contains {@link Byte} type. The purpose of this class is to test schema-aware PTransforms with
   * {@link Row}s containing this type. It is isolated because not all file formats support a single
   * byte data type (e.g. Avro, Parquet).
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ByteType {

    public abstract Byte getByte();

    public abstract List<Byte> getByteList();

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setByte(Byte value);

      public abstract Builder setByteList(List<Byte> value);

      public abstract ByteType build();
    }
  }

  /**
   * Contains {@link byte} type. The purpose of this class is to test schema-aware PTransforms with
   * {@link Row}s containing a sequence of bytes. It is isolated because not all file formats
   * support byte sequences (e.g. JSON, CSV, XML).
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ByteSequenceType {
    @SuppressWarnings("mutable")
    public abstract byte[] getByteSequence();

    @SuppressWarnings("mutable")
    public abstract List<byte[]> getByteSequenceList();

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setByteSequence(byte[] value);

      public abstract Builder setByteSequenceList(List<byte[]> value);

      public abstract ByteSequenceType build();
    }
  }

  /**
   * Contains arrays of all primitive Java types i.e. String, Integer, etc and {@link BigDecimal}.
   * The purpose of this class is to test schema-aware PTransforms with {@link Row}s containing
   * repeated primitive Java types.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ArrayPrimitiveDataTypes {

    public abstract List<Boolean> getBooleanList();

    public abstract List<Double> getDoubleList();

    public abstract List<Float> getFloatList();

    public abstract List<Integer> getIntegerList();

    public abstract List<Long> getLongList();

    public abstract List<String> getStringList();

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setBooleanList(List<Boolean> value);

      public abstract Builder setDoubleList(List<Double> value);

      public abstract Builder setFloatList(List<Float> value);

      public abstract Builder setIntegerList(List<Integer> value);

      public abstract Builder setLongList(List<Long> value);

      public abstract Builder setStringList(List<String> value);

      public abstract ArrayPrimitiveDataTypes build();
    }
  }

  /**
   * Contains a singly nested and repeated {@link AllPrimitiveDataTypes}. The purpose of this class
   * is to test schema-aware PTransforms with {@link Row}s containing nested and repeated complex
   * Java types.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SinglyNestedDataTypes {

    public abstract AllPrimitiveDataTypes getAllPrimitiveDataTypes();

    public abstract List<AllPrimitiveDataTypes> getAllPrimitiveDataTypesList();

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setAllPrimitiveDataTypes(AllPrimitiveDataTypes value);

      public abstract Builder setAllPrimitiveDataTypesList(List<AllPrimitiveDataTypes> value);

      public abstract SinglyNestedDataTypes build();
    }
  }

  /**
   * Contains a nested and repeated {@link SinglyNestedDataTypes}. The purpose of this class is to
   * test schema-aware PTransforms with {@link Row}s containing deeper nested and repeated complex
   * Java types.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class DoublyNestedDataTypes {

    public abstract SinglyNestedDataTypes getSinglyNestedDataTypes();

    public abstract List<SinglyNestedDataTypes> getSinglyNestedDataTypesList();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setSinglyNestedDataTypes(SinglyNestedDataTypes value);

      public abstract Builder setSinglyNestedDataTypesList(List<SinglyNestedDataTypes> value);

      public abstract DoublyNestedDataTypes build();
    }
  }
}
